//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

package mongoq

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	"github.com/facebookgo/rocks-strata/strata"
	"github.com/jessevdk/go-flags"
	"github.com/kr/pty"
	"golang.org/x/crypto/ssh/terminal"
)

type mongoState struct {
	dbpath   string
	mongod   *exec.Cmd
	mongo    *exec.Cmd
	mongoPty *os.File
}

func (ms *mongoState) close() error {
	var closeErr error
	if ms.mongod != nil {
		if err := ms.mongod.Process.Kill(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	if ms.mongo != nil {
		if err := ms.mongo.Process.Kill(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	if ms.mongoPty != nil {
		if err := ms.mongoPty.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	if ms.dbpath != "" {
		if err := os.RemoveAll(ms.dbpath); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

// newStartedMongoState takes a term argument to have a context for printing
func (msh *mongoStateHolder) newStartedMongoState(replicaID, backupID, mongodPath, mongoPath, mountPath string,
	term *terminal.Terminal, driver *strata.Driver) (*mongoState, error) {

	mongoState := mongoState{}
	var err error
	mongoState.dbpath, err = ioutil.TempDir("", "mongoq_")
	if err != nil {
		return &mongoState, err
	}

	if err := driver.RestoreReadOnly(replicaID, backupID, mountPath, mongoState.dbpath); err != nil {
		return &mongoState, err
	}

	// Try to start mongod
	// Look for output text to determine success
	// If output text indicates that port is already in use, try another port
	for mongoState.mongod == nil {
		mongoState.mongod = exec.Command(mongodPath, "--port="+strconv.Itoa(msh.nextPort),
			"--dbpath="+mongoState.dbpath, "--storageEngine=rocksdb", "--rocksdbConfigString=max_open_files=10")

		mongodOut, err := mongoState.mongod.StdoutPipe()
		if err != nil {
			return &mongoState, err
		}
		defer mongodOut.Close()
		if err := mongoState.mongod.Start(); err != nil {
			return &mongoState, err
		}
		// Wait until mongod is ready to accept a connection
		for {
			buf := make([]byte, 10000)
			n, _ := mongodOut.Read(buf)
			term.Write(buf[:n]) // If there is a problem starting mongod, the user should see it and kill process
			rec := string(buf[:n])
			if strings.Contains(rec, "waiting for connections on port") {
				mongodOut.Close()
				break
			} else if strings.Contains(rec, "Address already in use for socket") {
				mongodOut.Close()
				if err := mongoState.mongod.Process.Kill(); err != nil {
					return &mongoState, err
				}
				mongoState.mongod = nil
				term.Write([]byte("MONGOQ Trying to start mongod again on another port\n"))
				msh.nextPort++
				break
			}
		}
	}

	mongoState.mongo = exec.Command(mongoPath, "--port="+strconv.Itoa(msh.nextPort))
	msh.nextPort++
	mongoState.mongoPty, err = pty.Start(mongoState.mongo)
	return &mongoState, err
}

type backupUID struct {
	ReplicaID string
	BackupID  string
}

type mongoStateHolder struct {
	// Interactions through mongo shell are done through current, or through noDB
	// if current has not been successfully loaded. noDB uses a mongo shell that
	// was started with "--nodb".
	current  *mongoState
	noDB     *mongoState
	stateMap map[backupUID]*mongoState
	nextPort int
}

func newMongoStateHolder(mongoPath string) (*mongoStateHolder, error) {
	ms := mongoState{mongo: exec.Command(mongoPath, "--nodb")}
	var err error
	ms.mongoPty, err = pty.Start(ms.mongo)
	if err != nil {
		return nil, err
	}
	return &mongoStateHolder{noDB: &ms, stateMap: make(map[backupUID]*mongoState), nextPort: 27017}, nil
}

func (msh *mongoStateHolder) getPty() *os.File {
	if msh.current != nil {
		return msh.current.mongoPty
	}
	return msh.noDB.mongoPty
}

// load sets current to a mongoState with mongod and mongo corresponding to replicaID/backupID.
// load tries to re-use mongod and mongo processes that were already created.
// If it can't, it creates new mongod and mongo processes.
// monogod and mongo processes that are not in use are suspended so that OS can swap them out.
// load takes a Terminal argument to have a context for printing.
// load returns true if it started new mongod and mongo processes successfully.
func (msh *mongoStateHolder) load(replicaID, backupID, mongodPath, mongoPath, mountPath string, term *terminal.Terminal, driver *strata.Driver) (bool, error) {
	b := backupUID{ReplicaID: replicaID, BackupID: backupID}
	mongoState, found := msh.stateMap[b]
	if found && mongoState == msh.current {
		return false, nil
	}
	if msh.current != nil {
		// Suspend current
		if err := msh.current.mongo.Process.Signal(syscall.SIGSTOP); err != nil {
			return false, err
		}
		if err := msh.current.mongod.Process.Signal(syscall.SIGSTOP); err != nil {
			return false, err
		}
	}
	if found {
		msh.current = mongoState
		if err := msh.current.mongod.Process.Signal(syscall.SIGCONT); err != nil {
			return false, err
		}
		if err := msh.current.mongo.Process.Signal(syscall.SIGCONT); err != nil {
			return false, err
		}
		return false, nil
	}
	var err error
	msh.current, err = msh.newStartedMongoState(replicaID, backupID, mongodPath, mongoPath, mountPath, term, driver)
	if err != nil {
		msh.current = nil
		return false, err
	}
	msh.stateMap[b] = msh.current
	return true, nil
}

func (msh *mongoStateHolder) close() error {
	closeErr := msh.noDB.close()
	for _, ms := range msh.stateMap {
		if err := ms.close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

type readerWriter struct {
	r io.Reader
	w io.Writer
}

func (rw *readerWriter) Read(buf []byte) (n int, err error) {
	return rw.r.Read(buf)
}

func (rw *readerWriter) Write(buf []byte) (n int, err error) {
	return rw.w.Write(buf)
}

// QShell keeps track of mongod process, mongo process, and pseudo-terminal to
// and uses a psuedo-terminal to interact with mongo process
type QShell struct {
	driver     *strata.Driver
	mountPath  string
	showSize   bool
	mongodPath string
	mongoPath  string

	replicaID string
	backupID  string

	term *terminal.Terminal

	mongoStateHolder *mongoStateHolder

	// Only used for cleanup
	oldState *terminal.State
	stdinFd  int
}

// NewStartedQShell returns an initialized QShell, ready to Interact() with.
// Prints mongo startup message.
func NewStartedQShell(driver *strata.Driver, mountPath string, showSize bool, mongodPath, mongoPath string) (*QShell, error) {
	finished := false
	sh := QShell{driver: driver, mountPath: mountPath, showSize: showSize, mongodPath: mongodPath, mongoPath: mongoPath}

	// Put terminal in raw mode
	var err error
	sh.stdinFd = int(os.Stdin.Fd())
	sh.oldState, err = terminal.MakeRaw(sh.stdinFd)
	if err != nil {
		return &sh, err
	}

	// Convention seems to be that caller invokes Close() only if there is no error.
	// But we need to clean up if there is an error.
	defer func() {
		if !finished {
			sh.Close()
		}
	}()

	sh.term = terminal.NewTerminal(&readerWriter{r: os.Stdin, w: os.Stdout}, "\r> ")

	sh.mongoStateHolder, err = newMongoStateHolder(sh.mongoPath)
	if err != nil {
		return &sh, err
	}

	if err := sh.print("Wrapper around MongoDB shell with additional commands for querying backups.\n"); err != nil {
		return &sh, err
	}
	if err := sh.printMongoOutput(true); err != nil {
		return &sh, err
	}

	finished = true
	return &sh, nil
}

func (sh *QShell) loadBackup() error {
	newMongo, err := sh.mongoStateHolder.load(sh.replicaID, sh.backupID, sh.mongodPath, sh.mongoPath, sh.mountPath, sh.term, sh.driver)
	if err != nil {
		return err
	}
	if newMongo {
		return sh.printMongoOutput(true)
	}
	return nil
}

// Close cleans up mongod and mongo processes and closes fd for the terminal
func (sh *QShell) Close() error {
	closeErr := sh.mongoStateHolder.close()
	if err := terminal.Restore(sh.stdinFd, sh.oldState); err != nil && closeErr == nil {
		closeErr = err
	}
	return closeErr
}

func (sh *QShell) print(msg string) error {
	_, err := sh.term.Write([]byte(msg))
	return err
}

func (sh *QShell) printIfErr(err error) error {
	if err != nil {
		return sh.print(err.Error() + "\n")
	}
	return nil
}

func (sh *QShell) sendMongoCmd(cmd string) error {
	if _, err := sh.mongoStateHolder.getPty().Write([]byte(cmd + "\r\n")); err != nil {
		return err
	}
	return sh.printMongoOutput(false)
}

// Use firstTime=true if this is the first time calling printMongoOutput() after startMongo()
func (sh *QShell) printMongoOutput(firstTime bool) error {
	maxBytesReadAtOnce := 10000
	buf := make([]byte, maxBytesReadAtOnce)
	var recBytes []byte
	for {
		n, err := sh.mongoStateHolder.getPty().Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == maxBytesReadAtOnce {
			if err := sh.print("\nWARNING: Probably missing some of mongo's output"); err != nil {
				return err
			}
		}
		recBytes = append(recBytes, buf[:n]...)
		rec := string(recBytes) // Very fast in Golang
		// Terrible hacks to determine when mongo is finished printing output
		if strings.Count(rec, "\r\n> ") == 2 || firstTime && strings.Count(rec, "\r\n> ") == 1 {
			// Remove the "\r\n> " that mongo outputs
			msg := strings.TrimSuffix(strings.TrimSpace(strings.Replace(rec, "\r\n> ", "\n", -1)), "\n")
			if !firstTime {
				// Remove the first line of mongo output, which is an echo of the user's input
				msg = strings.SplitAfterN(msg, "\n", 2)[1]
			}
			if firstTime {
				msg += "\n"
			}
			if err := sh.print(msg); err != nil {
				return err
			}
			break
		}
	}
	return nil
}

func mongoHelpLineFmt(cmd, description string) string {
	return "\t" + cmd + strings.Repeat(" ", 29-len(cmd)) + description + "\n"
}

func (sh *QShell) printHelp() error {
	if err := sh.sendMongoCmd("help"); err != nil {
		return err
	}
	return sh.print("\nCommands added by wrapper:\n" +
		mongoHelpLineFmt("status", "show current replica ID and backup ID") +
		mongoHelpLineFmt("list_replicas [lr]", "list replicas with available backups") +
		mongoHelpLineFmt("list_backups [lb]", "list backups for current replica ID") +
		mongoHelpLineFmt("change_replica [cr]", "change to use the specified replica ID") +
		mongoHelpLineFmt("change_backup [cb]", "change to use the specified backup ID"))
}

func (sh *QShell) printStatus() error {
	rid := sh.replicaID
	if rid == "" {
		rid = "?"
	}
	bid := sh.backupID
	if bid == "" {
		bid = "?"
	}
	return sh.print("Current replica ID: " + rid + "\nCurrent backup ID: " + bid + "\n")
}

func (sh *QShell) changeBackup(cmd string) error {
	// Extract backup ID from command
	split := strings.Split(cmd, " ")
	if len(split) != 2 {
		return errors.New("Invalid command: " + cmd)
	}
	sh.backupID = split[1]

	if sh.replicaID == "" {
		return errors.New("Specify replica ID with `cr REPLICA_ID`")
	}

	return sh.loadBackup()
}

func (sh *QShell) changeReplicaID(cmd string) error {
	// Extract replica ID from command
	split := strings.Split(cmd, " ")
	if len(split) != 2 {
		return sh.print("Invalid command: " + cmd + "\n")
	}

	newReplicaID := split[1]
	if sh.replicaID != newReplicaID {
		sh.backupID = ""
		if err := sh.print("Replica ID changing to " + newReplicaID + "\nDatabase state is undefined until change_backup [cb] is run\n"); err != nil {
			return err
		}
	}
	sh.replicaID = newReplicaID
	return nil
}

// Interact runs the interactive mongo shell, but intercepts special commands
func (sh *QShell) Interact() error {
	for {
		cmd, err := sh.term.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if cmd == "exit" {
			break
		}
		if cmd == "help" {
			if err := sh.printHelp(); err != nil {
				return err
			}
		} else if cmd == "lb" || cmd == "list_backups" {
			if err := sh.printIfErr(sh.driver.ListBackups(sh.replicaID, sh.showSize)); err != nil {
				return err
			}
		} else if cmd == "lr" || cmd == "list_replicas" {
			if err := sh.printIfErr(sh.driver.ListReplicaIDs()); err != nil {
				return err
			}
		} else if cmd == "status" {
			if err := sh.printStatus(); err != nil {
				return err
			}
		} else if strings.HasPrefix(cmd, "cb ") || strings.HasPrefix(cmd, "change_backup ") {
			if err := sh.printIfErr(sh.changeBackup(cmd)); err != nil {
				return err
			}
		} else if strings.HasPrefix(cmd, "cr ") || strings.HasPrefix(cmd, "change_replica ") {
			if err := sh.changeReplicaID(cmd); err != nil {
				return err
			}
		} else {
			if err := sh.printIfErr(sh.sendMongoCmd(cmd)); err != nil {
				return err
			}
		}
	}

	return nil
}

func startMongoQ(driver *strata.Driver, mountPath string, showSize bool, mongodPath, mongoPath string) error {
	sh, err := NewStartedQShell(driver, mountPath, showSize, mongodPath, mongoPath)
	if err != nil {
		return err
	}
	defer sh.Close()
	return sh.Interact()
}

// Options relevant to extended mongo shell
type Options struct {
	MountPath  string `short:"m" long:"mount-path" description:"Wrapped mongo shell will look for database files at mount-path. You could set up this path with yas3fs." required:"true"`
	ShowSize   bool   `short:"s" long:"show-size" description:"Print backup size with backup ids"`
	MongodPath string `long:"mongod-path" default:"mongod" description:"path to mongod executable"`
	MongoPath  string `long:"mongo-path" default:"mongo" description:"path to mongo executable"`
}

// RunCLI parses command-line arguments and launches the extended mongo shell
func RunCLI(factory strata.DriverFactory) {
	mqOptions := &Options{}
	parser := flags.NewParser(factory.GetOptions(), flags.Default)
	parser.AddGroup("Extended Mongo Shell", "start a wrapped mongo shell that includes additional commands for switching between backups", mqOptions)

	if _, err := parser.Parse(); err != nil {
		os.Exit(1)
	}

	driver, err := factory.Driver()
	if err != nil {
		panic(err)
	}
	if err := startMongoQ(driver, mqOptions.MountPath, mqOptions.ShowSize, mqOptions.MongodPath, mqOptions.MongoPath); err != nil {
		panic(err)
	}
}
