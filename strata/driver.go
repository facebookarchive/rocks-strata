//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

package strata

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/jessevdk/go-flags"
)

// Driver holds a SnapshotManager and provides functions that correspond
// closely to command-line arguments
type Driver struct {
	Manager *SnapshotManager
}

// DriverFactory holds options relevant to SnapshotManagers/Drivers, and
// provides a function to construct Drivers from these options.
type DriverFactory interface {
	// GetOptions should return a pointer suitable for use as the first
	// argument of flags.NewParser()
	GetOptions() interface{}
	// Driver returns an initialized Driver
	Driver() (*Driver, error)
}

// Backup triggers a backup
func (driver *Driver) Backup(replicaID string) error {
	Log("Creating snapshot")
	stats, err := driver.Manager.CreateSnapshot(replicaID)
	if err != nil {
		return err
	}
	Log("Saving to remote storage")
	if err := driver.Manager.SaveMetadataForReplica(replicaID); err != nil {
		return err
	}
	Log(fmt.Sprintf("Backup finished (%f MB/s)", stats.Throughput()))
	return nil
}

// DeleteBackup calls SnapshtoManager.DeleteForReplicaByID
func (driver *Driver) DeleteBackup(replicaID, backupID string) error {
	if err := driver.Manager.DeleteForReplicaByID(replicaID, backupID); err != nil {
		return err
	}
	if err := driver.Manager.SaveMetadataForReplica(replicaID); err != nil {
		return err
	}
	Log("Delete finished.")
	return nil
}

// qfmat stands for "question mark format". It returns x as a string, or "?" if x is less than 0.
func qfmt(x interface{}) string {
	switch x := x.(type) {
	default:
		return "?"
	case int:
		if x < 0 {
			return "?"
		}
		return fmt.Sprintf("%d", x)
	case int64:
		if x < 0 {
			return "?"
		}
		return fmt.Sprintf("%d", x)
	case float64:
		if x < 0 {
			return "?"
		}
		return fmt.Sprintf("%f", x)
	case time.Duration:
		if x.Hours() < 0 {
			return "?"
		}
		return x.String()
	}
}

// PrintLazyMetadatas prints backup information given a list of LazySnapshotMetadata
func (driver *Driver) PrintLazyMetadatas(lazyMetadatas []LazySnapshotMetadata, showBackupSize bool) error {
	writer := new(tabwriter.Writer)
	// Columns are separated by three spaces.
	// Print to stdout so that driver is more composable
	writer.Init(os.Stdout, 0, 0, 3, ' ', 0)
	columns := []string{"ID", "data", "num files", "size (GB)", "incremental files", "incremental size", "duration"}
	fmt.Fprintln(writer, strings.Join(columns, "\t"))

	for _, lazy := range lazyMetadatas {
		_, id, timeStr, err := GetInfoFromMetadataPath(lazy.MetadataPath)
		if err != nil {
			return err
		}
		timeInt, err := strconv.ParseInt(timeStr, 10, 64)
		if err != nil {
			Log(fmt.Sprintf("Non-fatal error: %s", err))
			continue
		}
		timeHuman := time.Unix(0, timeInt)
		stats, _ := driver.Manager.GetBackupStats(&lazy, showBackupSize)
		line := fmt.Sprintf(strings.Repeat("%s\t", len(columns)), id,
			timeHuman.Format("2006-01-02 15:04:05 MST"),
			qfmt(stats.NumFiles), qfmt(ToGB(stats.SizeFiles)), qfmt(stats.NumIncrementalFiles),
			qfmt(ToGB(stats.SizeIncrementalFiles)), qfmt(stats.Duration))
		// Reset to enable GC. GetBackupStats might have pulled down the
		// metadata files, and those files can be ~2MB.
		lazy.Reset()
		fmt.Fprintln(writer, line)
	}
	writer.Flush()
	return nil
}

// PrintMetadataPaths prints backup information given a list of metadata paths
func (driver *Driver) PrintMetadataPaths(mdpaths []string) error {
	writer := new(tabwriter.Writer)
	// Columns are separated by three spaces.
	// Print to stdout so that driver is more composable
	writer.Init(os.Stdout, 0, 0, 3, ' ', 0)

	fmt.Fprintln(writer, "ID\tdate")
	for _, path := range mdpaths {
		_, id, timeStr, err := GetInfoFromMetadataPath(path)
		if err != nil {
			return err
		}
		timeInt, err := strconv.ParseInt(timeStr, 10, 64)
		if err != nil {
			Log(fmt.Sprintf("Non-fatal error: %s", err))
			continue
		}
		timeHuman := time.Unix(0, timeInt)
		fmt.Fprintf(writer, "%s\t%s\n", id, timeHuman.Format("2006-01-02 15:04:05 MST"))
	}
	writer.Flush()
	return nil
}

// DeleteOlderThan calls SnapshotManager.DeleteEarlierThan
func (driver *Driver) DeleteOlderThan(replicaID, age string) error {
	maxAge, err := time.ParseDuration(age)
	if err != nil {
		return err
	}
	cutoffTime := time.Now().Add(-1 * maxAge)

	pathsForDeletion, err := driver.Manager.DeleteEarlierThan(replicaID, cutoffTime)
	if err != nil {
		return err
	}
	err = driver.Manager.SaveMetadataForReplica(replicaID)
	if err != nil {
		return err
	}
	Log(fmt.Sprintf("Finished deleting backups earlier than %s", cutoffTime))
	Log("These backup metadatas were deleted:")
	return driver.PrintMetadataPaths(pathsForDeletion)
}

// CollectGarbage calls SnapshotManager.CollectGarbage
func (driver *Driver) CollectGarbage(replicaID string) error {
	if _, err := driver.Manager.CollectGarbage(replicaID); err != nil {
		return err
	}
	Log("Garbage collection finished.")
	return nil
}

// Restore calls SnapshotManager.RestoreSnapshot
func (driver *Driver) Restore(replicaID, backupID, targetPath string) error {
	snapshotMetadata, err := driver.Manager.GetSnapshotMetadata(replicaID, backupID)
	if err != nil {
		return err
	}

	stats, err := driver.Manager.RestoreSnapshot(replicaID, targetPath, *snapshotMetadata)
	if err != nil {
		return err
	}
	Log(fmt.Sprintf("Restore finished (%f MB/s)", stats.Throughput()))
	return nil
}

// RestoreReadOnly calls SnapshotManager.RestoreReadOnly
func (driver *Driver) RestoreReadOnly(replicaID, backupID, mountPath, targetPath string) error {
	snapshotMetadata, err := driver.Manager.GetSnapshotMetadata(replicaID, backupID)
	if err != nil {
		return err
	}

	stats, err := driver.Manager.RestoreReadOnly(replicaID, mountPath, targetPath, *snapshotMetadata)
	if err != nil {
		return err
	}
	Log(fmt.Sprintf("Read-only restore finished in %s", stats.Duration))
	return nil
}

// ListBackups prints a list of backups for a given replicaID
// If showBackupSize is true, then attempt to calculate backup size even if a stats file is missing.
func (driver *Driver) ListBackups(replicaID string, showBackupSize bool) error {
	lazyMetadatas, err := driver.Manager.GetLazyMetadata(replicaID)
	if err != nil {
		return err
	}
	sort.Sort(ByTime(lazyMetadatas))
	return driver.PrintLazyMetadatas(lazyMetadatas, showBackupSize)
}

// LastBackupTime prints the time of the last backup, in seconds since Unix
// epoch, to stdout. Nothing else is printed to stdout.
func (driver *Driver) LastBackupTime(replicaID string) error {
	lazyMetadatas, err := driver.Manager.GetLazyMetadata(replicaID)
	if err != nil {
		return ErrNoSnapshotMetadata(fmt.Sprintf("%s: %s", replicaID, err))
	}
	sort.Sort(ByTime(lazyMetadatas))
	if len(lazyMetadatas) == 0 {
		return ErrNoSnapshotMetadata(replicaID)
	}
	_, _, timeStr, err := GetInfoFromMetadataPath(lazyMetadatas[len(lazyMetadatas)-1].MetadataPath)
	if err != nil {
		return err
	}
	timeNano, err := strconv.ParseInt(timeStr, 10, 64)
	if err != nil {
		return err
	}
	fmt.Printf("%d", time.Unix(0, timeNano).Unix())
	return nil
}

// ListReplicaIDs prints the replica IDs of backups that are on remote storage
func (driver *Driver) ListReplicaIDs() error {
	fmt.Println("Replicas with backups in remote storage:")
	for _, id := range driver.Manager.GetReplicaIDs() {
		fmt.Println(id)
	}
	return nil
}

// BackupCommand defines "backup"
type BackupCommand struct {
	driverFactory DriverFactory
	ReplicaID     string `short:"r" long:"replica-id" description:"An arbitrary string representing this node in the metadata. Defaults to hostname."`
}

// Execute validates input to the backup command, then calls backup to implement "backup
func (c *BackupCommand) Execute(args []string) error {
	// if not set, look up the hostname, and use that
	if c.ReplicaID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			panic(err)
		}
		c.ReplicaID = hostname
	}

	driver, err := c.driverFactory.Driver()
	if err != nil {
		panic(err)
	}

	if err := driver.Backup(c.ReplicaID); err != nil {
		return err
	}
	return nil
}

// RestoreCommand defines "restore"
type RestoreCommand struct {
	driverFactory DriverFactory
	ReplicaID     string `short:"r" long:"replica-id" description:"An arbitrary string representing this node in the metadata" required:"true"`
	BackupID      string `short:"i" long:"backup-id" description:"The backup id to restore from" required:"true"`
	TargetPath    string `short:"t" long:"target-path" description:"If not specified, restore to the path in snapshot metadata."`
}

// Execute calls restore to implement "restore"
func (c *RestoreCommand) Execute(args []string) error {
	driver, err := c.driverFactory.Driver()
	if err != nil {
		panic(err)
	}
	if err := driver.Restore(c.ReplicaID, c.BackupID, c.TargetPath); err != nil {
		panic(err)
	}
	return nil
}

// DeleteCommand defines "delete"
type DeleteCommand struct {
	driverFactory DriverFactory
	ReplicaID     string `short:"r" long:"replica-id" description:"An arbitrary string representing this node in the metadata" required:"true"`
	BackupID      string `short:"d" long:"delete-backup-id" description:"Optional. If set, delete this backup id." optional:"true" default:""`
	Age           string `short:"a" long:"age" description:"Optional. If set, delete backups older than this date. Use a format accepted by time.ParseDuration, such as \"2h45m30s\"" optional:"true" default:""`
}

// Execute validates input to "delete" and calls the appropriate delete operation
func (c *DeleteCommand) Execute(args []string) error {
	driver, err := c.driverFactory.Driver()
	if err != nil {
		panic(err)
	}
	if c.BackupID == "" && c.Age == "" {
		return fmt.Errorf("Please set either --age or --backup-id to use this command")
	} else if c.BackupID != "" && c.Age != "" {
		return fmt.Errorf("Please set only --age or --backup-id, not both")
	} else if c.BackupID != "" {
		err = driver.DeleteBackup(c.ReplicaID, c.BackupID)
	} else {
		err = driver.DeleteOlderThan(c.ReplicaID, c.Age)
	}

	if err != nil {
		panic(err)
	}
	return nil
}

// GCCommand defines "gc"
type GCCommand struct {
	driverFactory DriverFactory
	ReplicaID     string `short:"r" long:"replica-id" description:"An arbitrary string representing this node in the metadata" required:"true"`
}

// Execute calls CollectGarbage to implement "gc"
func (c *GCCommand) Execute(args []string) error {
	driver, err := c.driverFactory.Driver()
	if err != nil {
		panic(err)
	}
	if err := driver.CollectGarbage(c.ReplicaID); err != nil {
		return err
	}
	return nil
}

// ShowCommand defines "show", which is split into subcommands
type ShowCommand struct{}

// ShowReplicaIDsCommand defines "show replica-ids"
type ShowReplicaIDsCommand struct {
	driverFactory DriverFactory
}

// Execute calls ListReplicaIDs to implement "show replica-ids"
func (c *ShowReplicaIDsCommand) Execute(args []string) error {
	driver, err := c.driverFactory.Driver()
	if err != nil {
		panic(err)
	}
	if err := driver.ListReplicaIDs(); err != nil {
		panic(err)
	}
	return nil
}

// ShowBackupsCommand defines "show backups"
type ShowBackupsCommand struct {
	driverFactory DriverFactory
	ReplicaID     string `short:"r" long:"replica-id" description:"The replica ID to show backups for" required:"true"`
	ShowSize      bool   `short:"s" long:"show-size" description:"Print backup size with backup ids"`
}

// Execute calls ListBackups to implement "show backups"
func (c *ShowBackupsCommand) Execute(args []string) error {
	driver, err := c.driverFactory.Driver()
	if err != nil {
		panic(err)
	}
	if err := driver.ListBackups(c.ReplicaID, c.ShowSize); err != nil {
		panic(err)
	}
	return nil
}

// ShowLastBackupTimeCommand defines "show last-backup-time"
type ShowLastBackupTimeCommand struct {
	driverFactory DriverFactory
	ReplicaID     string `short:"r" long:"replica-id" description:"The replica ID to show backups for" required:"true"`
}

// Execute calls LastBackupTime to implement "show last-backup-time"
func (c *ShowLastBackupTimeCommand) Execute(args []string) error {
	driver, err := c.driverFactory.Driver()
	if err != nil {
		panic(err)
	}
	if err := driver.LastBackupTime(c.ReplicaID); err != nil && !IsErrNoSnapshotMetadata(err) {
		panic(err)
	}
	return nil
}

// RunCLI parses command-line arguments and runs the corresponding command
func RunCLI(factory DriverFactory) {
	Log("Starting strata driver")

	parser := flags.NewParser(factory.GetOptions(), flags.Default)
	parser.AddCommand("backup", "trigger a backup", "trigger a backup on the replica", &BackupCommand{driverFactory: factory})
	parser.AddCommand("restore", "trigger a restore", "trigger a restore using the specified replica and backup id", &RestoreCommand{driverFactory: factory})
	parser.AddCommand("delete", "delete backup data", "remove backup metadata from storage", &DeleteCommand{driverFactory: factory})
	parser.AddCommand("gc", "garbage collect", "remove unneeded data files for the given replica id from storage", &GCCommand{driverFactory: factory})
	showCmd, _ := parser.AddCommand("show", "show backup data", "use to display the backup metadata", &ShowCommand{})
	showCmd.AddCommand("backups", "show backups", "show backups for the given replica id", &ShowBackupsCommand{driverFactory: factory})
	showCmd.AddCommand("last-backup-time", "seconds since epoch", "show the start time of the most recent successful backup for the given replica id, in seconds since epoch", &ShowLastBackupTimeCommand{driverFactory: factory})
	showCmd.AddCommand("replica-ids", "show replica ids", "show replica ids known to the metadata", &ShowReplicaIDsCommand{driverFactory: factory})

	if _, err := parser.Parse(); err != nil {
		os.Exit(1)
	}

	Log("Driver finished cleanly.")
}
