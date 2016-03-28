//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

package lreplica

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/facebookgo/rocks-strata/strata"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type sessionGetter interface {
	get(port, username, password string) (*mgo.Session, error)
}

type localSessionGetter struct{}

// port could be the empty string
func (l *localSessionGetter) get(port, username, password string) (*mgo.Session, error) {
	addr := "localhost"
	if port != "" {
		addr += ":" + port
	}
	return mgo.DialWithInfo(&mgo.DialInfo{
		Direct:   true,
		Addrs:    []string{addr},
		Timeout:  5 * time.Minute,
		Username: username,
		Password: password})
}

// LocalReplica is a replica where all methods that take a ReplicaID must be
// run on the host corresponding to ReplicaID
type LocalReplica struct {
	port                string
	username            string
	password            string
	sessionGetter       sessionGetter
	maxBackgroundCopies int
}

// NewLocalReplica constructs a LocalReplica
func NewLocalReplica(maxBackgroundCopies int, port, username, password string) (*LocalReplica, error) {
	return &LocalReplica{
		sessionGetter:       &localSessionGetter{},
		maxBackgroundCopies: maxBackgroundCopies,
		port:                port,
		username:            username,
		password:            password,
	}, nil

}

// MaxBackgroundCopies returns the maximum number of copies that CreateSnapshot() and
// RestoreSnapshot() should perform in the background.
func (r *LocalReplica) MaxBackgroundCopies() int {
	if r.maxBackgroundCopies == 0 {
		return 1
	}
	return r.maxBackgroundCopies
}

// PutReader writes a file to |path|
func (r *LocalReplica) PutReader(path string, reader io.Reader) error {
	writer, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating writer for putting reader at %s: %s", path, err)
	}
	defer writer.Close()
	_, err = io.Copy(writer, reader)
	if err != nil {
		return fmt.Errorf("copying to %s: %s", path, err)
	}
	return nil
}

// PutSoftlink creates a softlink
func (r *LocalReplica) PutSoftlink(pointingTo, path string) error {
	return os.Symlink(pointingTo, path)
}

// List returns up to |n| files on |path|
// This list does not include any directories
func (r *LocalReplica) List(path string, n int) ([]string, error) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}
	filenames := make([]string, 0, len(files))
	for _, file := range files {
		if len(filenames) == n {
			break
		}
		if !file.IsDir() {
			filenames = append(filenames, file.Name())
		}
	}
	return filenames, nil
}

// Delete removes the file at |path|
func (r *LocalReplica) Delete(path string) error {
	return os.RemoveAll(path)
}

// PrepareToRestoreSnapshot checks that the necessary directories exist and are not being used
func (r *LocalReplica) PrepareToRestoreSnapshot(replicaID string, targetPath string, s *strata.Snapshot) (string, error) {
	if targetPath == "" {
		targetPath = s.Metadata.Path
	}
	dbpath := targetPath + "/db/"
	syscall.Umask(0)
	if err := os.MkdirAll(dbpath, 0777); err != nil {
		return dbpath, err
	}

	// Check that database is not in use
	strata.Log("Checking that database is not in use")
	lockfile, err := GetDBLock(targetPath)
	if lockfile != nil {
		defer lockfile.Close()
	}
	if err != nil && !os.IsNotExist(err) {
		return dbpath, err
	}

	return dbpath, nil
}

// Same checks that the file at |path| has the same partial checksum as |file|
func (r *LocalReplica) Same(path string, file strata.File) (bool, error) {
	csum, err := partialChecksum(path)
	if err != nil {
		return false, err
	}
	return csum == file.Checksum, nil
}

func nestedBsonMapGet(m bson.M, arg string, moreArgs ...string) (interface{}, error) {
	strata.Log("Call to nestedBsonMapGet()")
	value, found := m[arg]
	if !found {
		return nil, errors.New("Key not found")
	}
	if len(moreArgs) == 0 {
		return value, nil
	}
	nextNest, ok := value.(bson.M)
	if !ok {
		return nil, errors.New("Nested value is not a bson map")
	}
	return nestedBsonMapGet(nextNest, moreArgs[0], moreArgs[1:]...)
}

// CreateSnapshot is called to trigger snapshot creation on the target source.
// It should run on the host that corresponds to replicaID unless you know what you're doing.
// CreateSnapshot replaces DBPATH/backup/latest with the snapshot that it creates. No other snapshots are kept locally.
// TODO(agf): Have a way to pass in tags
func (r *LocalReplica) CreateSnapshot(replicaID, snapshotID string) (*strata.Snapshot, error) {
	strata.Log("Getting session for CreateSnapshot()")
	session, err := r.sessionGetter.get(r.port, r.username, r.password)
	if err != nil {
		return nil, err
	}
	defer session.Close()
	session.SetMode(mgo.Eventual, false)

	result := bson.M{}

	strata.Log("Getting dbpath")
	// determine dbpath
	cmd := bson.D{{"getCmdLineOpts", 1}}
	if err := session.DB("admin").Run(cmd, &result); err != nil {
		return nil, err
	}
	dbpathInterface, err := nestedBsonMapGet(result, "parsed", "storage", "dbPath")
	if err != nil {
		return nil, err
	}
	dbpath, ok := dbpathInterface.(string)
	if !ok {
		return nil, errors.New("Nested bson map do not end in a string")
	}
	if !strings.HasPrefix(dbpath, "/") {
		return nil, fmt.Errorf(
			"Found relative database path %s. Database path must be absolute.", dbpath)
	}
	strata.Log("Got dbpath: " + dbpath)

	strata.Log("Preparing backup directory")
	// The backup's parent directory should exist
	syscall.Umask(0)
	if err := os.MkdirAll(getParentOfBackupPath(dbpath, replicaID), 0777); err != nil {
		return nil, err
	}
	// The actual backup directory shouldn't exist yet
	backupPath := getBackupPath(dbpath, replicaID)
	if err := os.RemoveAll(backupPath); err != nil {
		return nil, err
	}

	strata.Log("Getting new snapshot metadata")
	metadata := strata.SnapshotMetadata{
		ID:        snapshotID,
		ReplicaID: replicaID,
		Time:      time.Now(),
		Path:      dbpath}

	cmd = bson.D{{"setParameter", 1}, {"rocksdbBackup", backupPath}}
	strata.Log("Performing command for local backup")
	if err := session.DB("admin").Run(cmd, &result); err != nil {
		strata.Log("Error performing local backup....")
		return nil, err
	}

	strata.Log("Building metadata.Files")
	// Build metadata.Files
	files, err := ioutil.ReadDir(backupPath)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		fullpath := backupPath + "/" + file.Name()
		csum, err := partialChecksum(fullpath)
		if err != nil {
			return nil, err
		}
		metadata.Files = append(
			metadata.Files,
			strata.File{
				Name:     file.Name(),
				Size:     file.Size(),
				Checksum: csum})
	}
	strata.Log("Finished building metadata.Files.")

	return r.GetSnapshot(metadata)
}

// DeleteSnapshot just returns nil. See CreateSnapshot() for more about how
// snapshots are kept locally.
func (r *LocalReplica) DeleteSnapshot(lz strata.LazySnapshotMetadata) error {
	return nil
}

// GetSnapshot returns a Snapshot given the specified SnapshotMetadata
func (r *LocalReplica) GetSnapshot(metadata strata.SnapshotMetadata) (*strata.Snapshot, error) {
	snapshot := strata.NewSnapshot(&metadata)

	for _, file := range metadata.Files {
		fullpath := getBackupPath(metadata.Path, metadata.ReplicaID) + "/" + file.Name
		snapshot.AddReaderFunc(file.Name, func() (io.ReadCloser, error) {
			return os.Open(fullpath)
		})
	}
	return snapshot, nil
}

func getParentOfBackupPath(dbpath, replicaID string) string {
	return dbpath + "/backup/" + replicaID
}

func getBackupPath(dbpath, replicaID string) string {
	return getParentOfBackupPath(dbpath, replicaID) + "/latest"
}

// GetDBLock locks the database on the given dbpath, and returns an error if the database is already in use
// GetDBLock also returns a pointer to the opened lock file. It is the caller's responsibility to close this file.
func GetDBLock(dbpath string) (*os.File, error) {
	lockfilePath := dbpath + "/mongod.lock"
	file, err := os.Open(lockfilePath)
	if err != nil {
		return nil, err
	}
	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	return file, err
}

func partialChecksum(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()
	fileinfo, err := file.Stat()
	if err != nil {
		return "", err
	}

	csum, err := strata.PartialChecksum(file, fileinfo.Size())
	return fmt.Sprintf("%x", csum), err
}
