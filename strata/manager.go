//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

package strata

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxReplicas            = 1000
	maxSnapshotsPerReplica = 24 * 365
	maxBackupFilesPerDB    = 100000000
	fileMetadataPrefix     = "file_metadata/"
	metadataPrefix         = "metadata/"
	statsPrefix            = "stats/"
)

// ReaderFunc are intended to retrieve Readers that can consume snapshot files or metadata files
type ReaderFunc func() (io.ReadCloser, error)

// Snapshot represents a readable snapshot. It is the combination of snapshot data and
// a number of ReaderFuncs which are used to retrieve Readers that can consume the actual
// snapshot files
type Snapshot struct {
	Metadata    *SnapshotMetadata
	readerFuncs map[string]ReaderFunc
}

// NewSnapshot creates a Snapshot from the given metadata.
func NewSnapshot(metadata *SnapshotMetadata) *Snapshot {
	return &Snapshot{
		Metadata:    metadata,
		readerFuncs: map[string]ReaderFunc{},
	}
}

// AddReaderFunc is used to tell the Snapshot how to retrieve a Reader for a given
// file.
func (s *Snapshot) AddReaderFunc(name string, readerFunc func() (io.ReadCloser, error)) {
	s.readerFuncs[name] = readerFunc
}

// GetReader calls the ReaderFunc for the given file name, if it exists, and
// returns the resulting Reader and error information. GetReader uses strata's
// retry strategy.
func (s *Snapshot) GetReader(name string) (io.ReadCloser, error) {
	if readerFunc, ok := s.readerFuncs[name]; ok {
		var reader io.ReadCloser
		err := Try(func() error {
			var err error
			reader, err = readerFunc()
			return err
		}, "")
		return reader, err
	}

	return nil, ErrNotFound(name)
}

// Replica defines the interface for creating, deleting, and restoring snapshots
// on mongo replicas. Ugly details of triggering snapshots and retrieving them from disk
// should be handled here
type Replica interface {
	// CreateSnapshot is called to trigger Snapshot creation on the target source
	CreateSnapshot(replicaID, snapshotID string) (*Snapshot, error)

	// DeleteSnapshot is used to clean up the snapshot files on the target host
	DeleteSnapshot(lz LazySnapshotMetadata) error

	// PrepareToRestoreSnapshot() is called by a SnapshotManager when the
	// SnapshotManager is about to restore a snapshot on the replica.
	// If targetPath is the empty string, then the database is restored to the directory in Snapshot s.
	// The first return value is the path/prefix on replica for SST files. For example, /data/mongodb/db.
	// Note that this is slightly different from MongoDB's --dbpath argument, which would be /data/mongodb.
	PrepareToRestoreSnapshot(replicaID, targetPath string, s *Snapshot) (dbpath string, err error)

	// PutReader lays down a file.
	PutReader(path string, reader io.Reader) error

	// PutSoftlink creates a softlink
	PutSoftlink(pointingTo, path string) error

	// Delete the contents stored at the specified string
	Delete(string) error

	// List returns up to |n| files that have path/prefix |prefix| on replica
	// This list does not include any directories
	List(prefix string, n int) ([]string, error)

	// Same returns true if the replica object at |path| has the same fingerprint as |file|.
	// This fingerprint might be a partial checksum. It should be computed quickly.
	Same(path string, file File) (bool, error)

	// TODO(agf): GetSnapshot() could be removed if tests are changed

	// GetSnapshot returns a Snapshot given the specified SnapshotMetadata
	GetSnapshot(s SnapshotMetadata) (*Snapshot, error)

	// MaxBackgroundCopies returns the maximum number of copies that CreateSnapshot
	// and RestoreSnapshot may perform in parallel
	MaxBackgroundCopies() int
}

// Storage defines a generic interface for persisting snapshots and snapshot metadata
type Storage interface {
	// Get returns a Reader at the specified string
	Get(string) (reader io.ReadCloser, err error)

	// Put stores the bytes at the specified location.
	// Implementations should see comments on PutReader.
	Put(path string, data []byte) error

	// PutReader stores the contents of Reader at the specified location
	// Put and PutReader should not leave any incomplete files in the event of
	// a failure, unless those files end in ".tmp". Garbage collection will
	// eventually remove .tmp files.
	// Suggestion: If the storage system does not support atomic Puts, consider
	// implementing Put and PutReader by first writing to a .tmp file, and then
	// atomically renaming the file to remove the .tmp suffix.
	PutReader(path string, reader io.Reader) error

	// Delete the contents stored at the specified string.
	// Should return nil if there is no object at the specified string.
	Delete(string) error

	// List up to n items satisfying the specified prefix
	List(prefix string, n int) ([]string, error)

	// Lock is optional. If implemented, provide exclusive access to the specified item
	// Lock is not currently used by SnapshotManager anyway
	Lock(string) error

	// Unlock
	Unlock(string) error
}

// SnapshotManager orchestrates snapshots by integrating the Replica and Storage interfaces
type SnapshotManager struct {
	replica  Replica
	storage  Storage
	metadata MetadataStore
}

// NewSnapshotManager builds a new SnapshotManager with the specified Replica and Storage
// implementations
func NewSnapshotManager(r Replica, p Storage) (*SnapshotManager, error) {
	metadata := NewMetadataStore()
	s := &SnapshotManager{
		replica:  r,
		storage:  p,
		metadata: metadata,
	}

	err := s.RefreshMetadata()
	if err != nil {
		return nil, err
	}
	return s, nil
}

// SnapshotStats are for CreateSnapshot() and RestoreSnapshot() operations
type SnapshotStats struct {
	NumFiles             int           `json:"num_files"`
	NumIncrementalFiles  int           `json:"num_ifiles"`
	SizeFiles            int64         `json:"size_files"`
	SizeIncrementalFiles int64         `json:"size_ifiles"`
	Duration             time.Duration `json:"duration"`
}

// Throughput is in MB/s
func (s *SnapshotStats) Throughput() float64 {
	seconds := s.Duration.Seconds()
	if seconds == 0.0 {
		return -1.0
	}
	return float64(s.SizeIncrementalFiles) / 1048576.0 / seconds
}

// CreateSnapshot generates a snapshot on the host specified by replicaID and
// persists the snapshot. The snapshot metadata will be persisted when
// SaveMetadataForReplica() is called.
func (s *SnapshotManager) CreateSnapshot(replicaID string) (*SnapshotStats, error) {
	start := time.Now()
	stats := SnapshotStats{}
	defer func() { stats.Duration = time.Since(start) }()

	snapshotID, err := s.getNextSnapshotID(replicaID)
	if err != nil {
		return &stats, err
	}
	Log(fmt.Sprintf("Creating snapshot for %s with ID %s", replicaID, snapshotID))
	snapshot, err := s.replica.CreateSnapshot(replicaID, snapshotID)
	if err != nil {
		return &stats, err
	}

	stats.NumFiles = len(snapshot.Metadata.Files)
	for _, file := range snapshot.Metadata.Files {
		stats.SizeFiles += file.Size
	}

	filesToSave, err := s.getFilesDelta(*snapshot.Metadata)
	if err != nil {
		return &stats, err
	}

	stats.NumIncrementalFiles = len(filesToSave)
	for _, file := range filesToSave {
		stats.SizeIncrementalFiles += file.Size
	}

	// copy to persistent storage
	nfiles := uint64(len(filesToSave))
	filesToSaveChan := make(chan File)
	var copyWG sync.WaitGroup
	go func() {
		for _, file := range filesToSave {
			filesToSaveChan <- file
		}
		close(filesToSaveChan)
	}()
	var nfinished uint64
	var errValue atomic.Value
	for t := 0; t < s.replica.MaxBackgroundCopies(); t++ {
		copyWG.Add(1)
		go func() {
			defer copyWG.Done()
			for file := range filesToSaveChan {
				path := getFilePath(replicaID, file)
				err := Try(func() error {
					reader, err := snapshot.GetReader(file.Name)
					if err != nil {
						return fmt.Errorf("getting reader for %s: %s", file.Name, err)
					}
					defer reader.Close()
					// Wrap reader so that checksum is computed as reader is Put
					checksummingReader := NewChecksummingReader(reader, nil)
					defer checksummingReader.Close()
					if err = s.storage.PutReader(path, checksummingReader); err != nil {
						return fmt.Errorf("putting reader at %s: %s", path, err)
					}
					return s.storeChecksum(checksummingReader.Sum(), replicaID, file)
				}, "Starting to save "+file.Name)
				if err == nil {
					Log(fmt.Sprintf("Finished saving %s", file.Name))
					Log(fmt.Sprintf("%d out of %d files saved", atomic.AddUint64(&nfinished, 1), nfiles))
				} else {
					Log(fmt.Sprintf("%s", err))
					Log(fmt.Sprintf("Unable to save %s", file.Name))
					errValue.Store(err)
				}
			}
		}()
	}
	copyWG.Wait()

	if errIface := errValue.Load(); errIface != nil {
		err, ok := errIface.(error)
		if !ok {
			return &stats, errors.New("errValue does not store an error")
		}
		return &stats, err
	}

	lazySM := NewLazySMFromM(snapshot.Metadata)
	lazySM.SaveMark = true
	err = s.metadata.Add(lazySM)
	if err != nil {
		return &stats, err
	}

	// Persist the stats
	stats.Duration = time.Since(start)
	err = Try(func() error {
		statsBytes, err := json.Marshal(stats)
		if err != nil {
			return err
		}
		if err = s.storage.Put(getStatsPath(snapshot.Metadata), statsBytes); err != nil {
			return err
		}
		return nil
	}, "Saving backup stats")
	if err == nil {
		Log("Saved backup stats")
	} else {
		Log("Non-fatal error: Could not save backup stats")
	}

	return &stats, nil
}

func (s *SnapshotManager) storeChecksum(checksum []byte, replicaID string, file File) error {
	fileMetadata := FileMetadata{CompleteChecksum: checksum}
	fileMetadataBytes, err := json.Marshal(fileMetadata)
	fileMetadataPath := getFileMetadataPath(replicaID, file)
	if err = s.storage.Put(fileMetadataPath, fileMetadataBytes); err != nil {
		return fmt.Errorf("putting file metadata at %s: %s", fileMetadataPath, err)
	}
	return nil
}

func (s *SnapshotManager) getNextSnapshotID(replicaID string) (string, error) {
	lazyMetadatas, err := s.GetLazyMetadata(replicaID)
	if IsErrNotFound(err) {
		return "0", nil
	} else if err != nil {
		return "", err
	}

	var nextID int64 = 0
	for _, lazy := range lazyMetadatas {
		_, id, _, err := GetInfoFromMetadataPath(lazy.MetadataPath)
		if err != nil {
			return "", err
		}
		idInt, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			Log(fmt.Sprintf("Skipping backup ID %s (does not parse to int64)", id))
		} else if idInt >= nextID {
			nextID = idInt + 1
		}
	}
	return strconv.FormatInt(nextID, 10), nil
}

// GetSnapshot builds a Snapshot struct from the given metadata, which can be used to access
// the actual snapshot data
func (s *SnapshotManager) GetSnapshot(metadata SnapshotMetadata) (*Snapshot, error) {
	snapshot := NewSnapshot(&metadata)
	for _, file := range metadata.Files {
		path := getFilePath(metadata.ReplicaID, file)
		snapshot.AddReaderFunc(file.Name, func() (io.ReadCloser, error) {
			return s.storage.Get(path)
		})
	}
	return snapshot, nil
}

// DeleteSnapshot marks metadata to be deleted from persistent storage the next time
// that SaveMetadataForReplica() is called.
func (s *SnapshotManager) DeleteSnapshot(metadata SnapshotMetadata) error {
	return s.metadata.Delete(metadata)
}

// DeleteEarlierThan finds snapshot metadata from earlier than the specified
// time and marks them to be deleted from persistent storage the next time that
// SaveMetadataForReplica() is called.
// The first return value is a slice of the metadata paths that were marked for
// deletion. Currently, this return value is only used as a statistic.
func (s *SnapshotManager) DeleteEarlierThan(replicaID string, t time.Time) ([]string, error) {
	return s.metadata.DeleteEarlierThan(replicaID, t)
}

// DeleteForReplicaByID finds the metadata that corresponds to replicaID and
// id, and marks the metadata to be deleted from persistent storage the next
// time that SaveMetadataForReplica() is called.
func (s *SnapshotManager) DeleteForReplicaByID(replicaID string, id string) error {
	return s.metadata.DeleteForReplicaByID(replicaID, id)
}

// GetBackupStats returns a SnapshotStats struct for the backup described by |lazy|.
// It tries to do this by looking up a persisted stats file. If it can't find a stats
// file and |force| is false, then stats fields are set to -1. If it can't find a stats file
// and |force| is true, then SnapshotStats.NumFiles and SnapshotStats.SizeFiles are
// computed from lazy's metadata, and the other fields are set to -1.
// Even if GetSnapshotStats returns an error, all the fields of the returned
// stats should be populated.
func (s *SnapshotManager) GetBackupStats(lazy *LazySnapshotMetadata, force bool) (*SnapshotStats, error) {
	stats := SnapshotStats{NumFiles: -1, NumIncrementalFiles: -1, SizeFiles: -1,
		SizeIncrementalFiles: -1, Duration: time.Duration(-1 * time.Hour)}
	backupStatsPath := statsPrefix + strings.TrimPrefix(lazy.MetadataPath, metadataPrefix)
	backupStatsReader, err := s.storage.Get(backupStatsPath)
	if err != nil {
		if IsErrNotFound(err) {
			if !force {
				return &stats, nil
			}
			// No stats file was saved for this backup, so we compute manually
			metadata, err := lazy.Get()
			if err != nil {
				return &stats, err
			}
			stats.NumFiles = len(metadata.Files)
			for _, file := range metadata.Files {
				stats.SizeFiles += file.Size
			}
			return &stats, nil
		}
		return &stats, err
	}
	defer backupStatsReader.Close()
	backupStatsBytes, err := ioutil.ReadAll(backupStatsReader)
	if err != nil {
		return &stats, fmt.Errorf("Error reading stats data at %s: %s", backupStatsPath, err)
	}
	err = json.Unmarshal(backupStatsBytes, &stats)
	return &stats, err
}

// RestoreSnapshot retrieves a Snapshot specified by the given metadata and triggers a restore.
// The Replica implementation determines the physical machine on which to write the restored files.
// The Replica implementation may or may not consider |replicaID| when making this determination.
// The restored files go to the database path specified by |targetPath|.
func (s *SnapshotManager) RestoreSnapshot(replicaID string, targetPath string, metadata SnapshotMetadata) (*SnapshotStats, error) {
	start := time.Now()
	stats := SnapshotStats{}
	defer func() { stats.Duration = time.Since(start) }()

	snapshot, err := s.GetSnapshot(metadata)
	if err != nil {
		return &stats, err
	}

	dbpath, err := s.replica.PrepareToRestoreSnapshot(replicaID, targetPath, snapshot)
	if err != nil {
		return &stats, err
	}

	// Determine which files we need to restore and keep them in |files|.
	// Remove unwanted files from database.
	Log("Determining files to restore")
	stats.NumFiles = len(snapshot.Metadata.Files)
	files := make(map[string]File, stats.NumFiles)
	for _, file := range snapshot.Metadata.Files {
		files[file.Name] = file
		stats.SizeFiles += file.Size
	}
	// TODO(agf): If we really want to support restores to non-local replicas,
	// then List() would need to take replicaID as an argument
	dbcontents, err := s.replica.List(dbpath, -1)
	if err != nil {
		return &stats, err
	}
	for _, dbfilename := range dbcontents {
		dbfilepath := dbpath + "/" + dbfilename
		if !strings.HasSuffix(dbfilename, ".sst") {
			s.replica.Delete(dbfilepath)
			continue
		}
		file, neededForRestore := files[dbfilename]
		if !neededForRestore {
			s.replica.Delete(dbfilepath)
			continue
		}
		same, err := s.replica.Same(dbfilepath, file)
		if !same || err != nil {
			s.replica.Delete(dbfilepath)
			continue
		}
		delete(files, dbfilename)
	}
	Log(fmt.Sprintf("Keeping %d files in current database", len(snapshot.Metadata.Files)-len(files)))

	// Write the files from snapshot
	stats.NumIncrementalFiles = len(files)
	filesToCopy := make(chan File)
	var copyWG sync.WaitGroup
	go func() {
		for _, file := range files {
			filesToCopy <- file
			stats.SizeIncrementalFiles += file.Size
		}
		close(filesToCopy)
	}()
	var nfinished uint64
	var errValue atomic.Value
	for t := 0; t < s.replica.MaxBackgroundCopies(); t++ {
		copyWG.Add(1)
		go func() {
			defer copyWG.Done()
			for file := range filesToCopy {
				err := Try(func() error {
					reader, err := snapshot.GetReader(file.Name)
					if err != nil {
						return fmt.Errorf("getting reader to restore %s: %s", file.Name, err)
					}
					defer reader.Close()
					// Wrap reader so that checksum is computed as reader is Put
					checksummingReader := NewChecksummingReader(reader, nil)
					defer checksummingReader.Close()
					if err := s.replica.PutReader(dbpath+"/"+file.Name, checksummingReader); err != nil {
						return fmt.Errorf("Putting reader to %s: %s", dbpath+"/"+file.Name, err)
					}
					return s.verifyChecksum(checksummingReader.Sum(), metadata.ReplicaID, file)
				}, "Starting to restore "+file.Name)
				if err == nil {
					Log(fmt.Sprintf("Finished restoring %s", file.Name))
					Log(fmt.Sprintf("%d out of %d files restored",
						atomic.AddUint64(&nfinished, 1), stats.NumIncrementalFiles))
				} else {
					Log(fmt.Sprintf("%s", err))
					Log(fmt.Sprintf("Unable to restore %s", file.Name))
					errValue.Store(err)
				}
			}
		}()
	}
	copyWG.Wait()

	if errIface := errValue.Load(); errIface != nil {
		err, ok := errIface.(error)
		if !ok {
			return &stats, errors.New("errValue does not store an error")
		}
		return &stats, err
	}
	return &stats, nil
}

// checkReadOnlyMountPath returns an error if it is possible to create a directory in |path|
func checkReadOnlyMountPath(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("path %s is not a directory", path)
	}

	// yas3fs files appear to have permission 0755, even when yas3fs is opened read-only.
	// So, we test read-only-ness by attempting to create a directory
	probe := fmt.Sprintf("%s/probing_read_only_fs_%d", path, rand.Int63())
	defer os.RemoveAll(probe)
	if writeErr := os.MkdirAll(probe, 0777); writeErr == nil {
		return fmt.Errorf("Should not be able to create directory in read-only file system %s", path)
	}
	return nil
}

// RestoreReadOnly aims to perform a fast restore upon which the user
// can perform read-only queries.  mountPath should be a mountpoint on
// replicaID that provides read-only access to storage. For example, remote
// storage might be Amazon S3 and mountPath might be created with `yas3fs
// s3:://bucket-name/bucket-prefix mountPath --mkdir --no-metadata
// --read-only`. See https://github.com/danilop/yas3fs. Allowing write access
// to mountPath is unsafe.
func (s *SnapshotManager) RestoreReadOnly(replicaID, mountPath, targetPath string, metadata SnapshotMetadata) (*SnapshotStats, error) {
	start := time.Now()
	stats := SnapshotStats{NumIncrementalFiles: -1}
	defer func() { stats.Duration = time.Since(start) }()

	if err := checkReadOnlyMountPath(mountPath); err != nil {
		return &stats, err
	}

	snapshot := NewSnapshot(&metadata) // We don't need GetSnapshot()

	dbpath, err := s.replica.PrepareToRestoreSnapshot(replicaID, targetPath, snapshot)
	if err != nil {
		return &stats, err
	}

	// Clear the current db contents
	dbcontents, err := s.replica.List(dbpath, -1)
	if err != nil {
		return &stats, err
	}
	for _, dbfilename := range dbcontents {
		s.replica.Delete(dbpath + "/" + dbfilename)
	}

	stats.NumFiles = len(metadata.Files)
	// Fill db with softlinks that point to mountPath
	for _, file := range metadata.Files {
		err := s.replica.PutSoftlink(mountPath+"/"+getFilePath(metadata.ReplicaID, file), dbpath+"/"+file.Name)
		if err != nil {
			return &stats, err
		}
	}

	return &stats, nil
}

// Get checksum from storage and compare it to |checksum|
func (s *SnapshotManager) verifyChecksum(checksum []byte, replicaID string, file File) error {
	fileMetadataPath := getFileMetadataPath(replicaID, file)
	fileMetadataReader, err := s.storage.Get(fileMetadataPath)
	if err != nil {
		if IsErrNotFound(err) {
			Log(fmt.Sprintf("Could not verify checksum for %s (no file metadata found at %s)",
				file.Name, fileMetadataPath))
			return nil
		}
		return fmt.Errorf("Getting file metadata at %s: %s", fileMetadataPath, err)
	}
	defer fileMetadataReader.Close()
	fileMetadataBytes, err := ioutil.ReadAll(fileMetadataReader)
	if err != nil {
		return fmt.Errorf("Error reading from file metadata at %s: %s", fileMetadataPath, err)
	}
	fileMetadata := FileMetadata{}
	err = json.Unmarshal(fileMetadataBytes, &fileMetadata)
	if err != nil {
		return fmt.Errorf("Error unmarshalling data from %s: %s", fileMetadataPath, err)
	}
	storedChecksum := fileMetadata.CompleteChecksum
	if !bytes.Equal(checksum, storedChecksum) {
		return fmt.Errorf(
			"%s has checksum %s but expected %s. %s may have become corrupted on storage",
			file.Name, checksum, storedChecksum, file.Name)
	}
	return nil
}

// GetLazyMetadata returns all LazySnapshotMetadata for the given replicaID
func (s *SnapshotManager) GetLazyMetadata(replicaID string) ([]LazySnapshotMetadata, error) {
	return s.metadata.getForReplica(replicaID)
}

// GetSnapshotMetadata returns the SnapshotMetadata specified by replicaID and snapshotID
func (s *SnapshotManager) GetSnapshotMetadata(replicaID, snapshotID string) (*SnapshotMetadata, error) {
	lazies, err := s.GetLazyMetadata(replicaID)
	if err != nil {
		return nil, err
	}
	for _, lazy := range lazies {
		_, id, _, err := GetInfoFromMetadataPath(lazy.MetadataPath)
		if err != nil {
			return nil, err
		}
		if id == snapshotID {
			return lazy.Get()
		}
	}
	return nil, ErrNotFound(fmt.Sprintf("%s[%s]", replicaID, snapshotID))
}

// RefreshMetadata instructs the SnapshotManager to retrieve the snapshot metadata from persisted storage
func (s *SnapshotManager) RefreshMetadata() error {
	md := NewMetadataStore()

	mdFiles, err := s.storage.List(metadataPrefix, maxReplicas*maxSnapshotsPerReplica)
	if err != nil {
		return err
	}

	for _, file := range mdFiles {
		if !goodMetadataPathFormat(file) {
			Log(fmt.Sprintf("Not refreshing metadata from %s (bad path format)", file))
			continue
		}
		fcopy := file
		lazySM := NewLazySMFromPath(fcopy, func() (io.ReadCloser, error) { return s.storage.Get(fcopy) })
		if err = md.Add(lazySM); err != nil {
			return err
		}
	}
	s.metadata = md
	return nil
}

// SaveMetadataForReplica instructs the SnapshotManager to persist a single
// replica ID's metadata to storage.  After SaveMetadataForReplica is called,
// the SnapshotManager is in an undefined state until RefreshMetadata is
// called.
// If metadata is marked for deletion, then then SaveMetadataForReplica also
// calls replica.DeleteSnapshot()
func (s *SnapshotManager) SaveMetadataForReplica(replicaID string) error {
	lazySnapshotMetadatas, err := s.GetLazyMetadata(replicaID)
	if err != nil {
		return err
	}

	for _, lazyMetadata := range lazySnapshotMetadatas {
		if lazyMetadata.DeleteMark {
			// Delete snapshot from replica, if replica implements DeleteSnapshot
			err = Try(func() error {
				return s.replica.DeleteSnapshot(lazyMetadata)
			}, "")
			if err != nil {
				return err
			}
			// Remove metadata from persistent storage
			err = Try(func() error {
				return s.storage.Delete(lazyMetadata.MetadataPath)
			}, "")
			if err != nil {
				return err
			}
		} else if lazyMetadata.SaveMark {
			err := Try(func() error {
				metadata, err := lazyMetadata.Get()
				if err != nil {
					return err
				}
				mdBytes, err := json.Marshal(*metadata)
				if err != nil {
					return err
				}
				if err = s.storage.Put(getMetadataPath(metadata), mdBytes); err != nil {
					return err
				}
				return nil
			}, "")
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// GetReplicaIDs returns all known replica ids in the metadata store
func (s *SnapshotManager) GetReplicaIDs() []string {
	return s.metadata.GetReplicaIDs()
}

// GCStats holds statistics about a call to CollectGarbage(). SizeGarbage and
// SizeGarbageNotDeleted are technically lower bounds because it is possible
// that sizes cannot be extracted from garbage file names. However, these
// values should usually be exact.
// Sizes exclude file metadata and stats files, since those do not store their
type GCStats struct {
	// NumNeeded and NumGarbage include only DB files
	NumNeeded  int
	NumGarbage int
	// FM means "file metadata"
	NumFMNeeded     int
	NumFMGarbage    int
	NumStatsNeeded  int
	NumStatsGarbage int
	NumErrsDeleting int
	// Sizes exclude file metadata and stats files, since those do not include
	// their sizes in their names
	SizeNeeded            int64
	SizeGarbage           int64
	SizeGarbageNotDeleted int64
	Duration              time.Duration
}

// CollectGarbage examines the persisted files related to the specified replica
// and deletes files that are not needed by current backups.
// This is mostly intended to remove database files that were once needed, but are no longer needed by existing backups.
// CollectGarbage also has the following important property: It examines all
// the paths where SnapshotManager might write objects associated with
// replicaID, and it deletes all .tmp files that it finds.
//
func (s *SnapshotManager) CollectGarbage(replicaID string) (*GCStats, error) {
	start := time.Now()
	stats := GCStats{}
	defer func() { stats.Duration = time.Since(start) }()
	Log("Starting to collect garbage for " + replicaID)

	// Determine which files are needed by current backups
	// This will remove any .tmp files that are under replicaID+"/", since no
	// needed files end in .tmp.
	lazySnapshotMetadatas, err := s.GetLazyMetadata(replicaID)
	if err != nil && !IsErrNotFound(err) {
		return &stats, err
	}
	neededFiles := make(map[string]struct{})
	for _, lazyMetadata := range lazySnapshotMetadatas {
		metadata, err := lazyMetadata.Get()
		if err != nil {
			return &stats, err
		}
		for _, file := range metadata.Files {
			path := getFilePath(replicaID, file)
			if _, found := neededFiles[path]; !found {
				neededFiles[path] = struct{}{}
				stats.SizeNeeded += file.Size
			}
		}
		// Reset() to allow Go runtime to garbage-collect metadata
		lazyMetadata.Reset()
	}
	stats.NumNeeded = len(neededFiles)
	Log(fmt.Sprintf("%s has %d backups that require %d total files",
		replicaID, len(lazySnapshotMetadatas), stats.NumNeeded))

	files, err := s.storage.List(replicaID+"/", maxBackupFilesPerDB)
	if err != nil {
		return &stats, err
	}
	numFiles := len(files)
	Log(fmt.Sprintf("Found %d files related to backups of %s", numFiles, replicaID))

	// Prepare to delete unneeded files
	filesToDelete := make([]string, 0, numFiles-stats.NumNeeded)
	for _, file := range files {
		if _, found := neededFiles[file]; !found {
			filesToDelete = append(filesToDelete, file)
			if size, err := getSizeFromFilePath(file); err == nil {
				stats.SizeGarbage += size
			}
		}
	}
	stats.NumGarbage = len(filesToDelete)
	Log(fmt.Sprintf("Found %d files for deletion", stats.NumGarbage))
	if numFiles != stats.NumNeeded+stats.NumGarbage {
		return &stats, fmt.Errorf("For %s, NumFiles != NumNeeded + NumGarbage", replicaID)
	}

	// Delete unneeded files
	for _, file := range filesToDelete {
		Log("Deleting " + file)
		if err = s.storage.Delete(file); err != nil {
			Log(fmt.Sprintf("Non-fatal error deleting %s from persistent storage: %s", file, err))
			stats.NumErrsDeleting++
			if size, err := getSizeFromFilePath(file); err == nil {
				stats.SizeGarbageNotDeleted += size
			}
		}
	}

	// Now handle files under fileMetadataPrefix
	// All .tmp files under fileMetadataPrefix+replicaID will be removed
	filesFM, err := s.storage.List(fileMetadataPrefix+replicaID+"/", maxBackupFilesPerDB)
	if err != nil {
		return &stats, err
	}
	Log(fmt.Sprintf("Found %d file metadata files related to backups of %s", len(filesFM), replicaID))
	filesToDeleteFM := filesToDelete[:0]
	for _, fileFM := range filesFM {
		if _, found := neededFiles[strings.TrimPrefix(fileFM, fileMetadataPrefix)]; !found {
			filesToDeleteFM = append(filesToDeleteFM, fileFM)
		}
	}
	stats.NumFMGarbage = len(filesToDeleteFM)
	// Can't check something like numFiles == numNeeded + numGarbage because we do not know how
	// many database files actually had file_metadata files to begin with
	stats.NumFMNeeded = len(filesFM) - stats.NumFMGarbage
	Log(fmt.Sprintf("Found %d file metadata files for deletion", stats.NumFMGarbage))
	for _, fileFM := range filesToDeleteFM {
		Log("Deleting " + fileFM)
		if err = s.storage.Delete(fileFM); err != nil {
			Log(fmt.Sprintf("Non-fatal error deleting %s from persistent storage: %s", fileFM, err))
			stats.NumErrsDeleting++
		}
	}

	// Now handle files under statsPrefix
	// All .tmp files under statsPrefix+replicaID will be removed
	neededStats := make(map[string]struct{})
	for _, lazy := range lazySnapshotMetadatas {
		neededStats[strings.TrimPrefix(lazy.MetadataPath, metadataPrefix)] = struct{}{}
	}
	statsFiles, err := s.storage.List(statsPrefix+replicaID+"/", maxBackupFilesPerDB)
	if err != nil {
		return &stats, err
	}
	Log(fmt.Sprintf("Found %d backup stats files related to %s", len(statsFiles), replicaID))
	statsToDelete := filesToDeleteFM[:0]
	for _, statsFile := range statsFiles {
		if _, found := neededStats[strings.TrimPrefix(statsFile, statsPrefix)]; !found {
			statsToDelete = append(statsToDelete, statsFile)
		}
	}
	stats.NumStatsGarbage = len(statsToDelete)
	stats.NumStatsNeeded = len(statsFiles) - stats.NumStatsGarbage
	Log(fmt.Sprintf("Found %d stats files for deletion", stats.NumStatsGarbage))
	for _, statsFile := range statsToDelete {
		Log("Deleting " + statsFile)
		if err = s.storage.Delete(statsFile); err != nil {
			Log(fmt.Sprintf("Non-fatal error deleting %s from persistent storage: %s", statsFile, err))
			stats.NumErrsDeleting++
		}
	}

	// Finally, we search for and remove all .tmp files under metadataPrefix+replicaID.
	// This is a cautious approach. The code block could instead delete all
	// metadata files for which goodMetadataPathFormat() is false.
	metadataFiles, err := s.storage.List(metadataPrefix+replicaID+"/", maxBackupFilesPerDB)
	if err != nil {
		return &stats, err
	}
	mdFilesToDelete := statsToDelete[:0]
	for _, mdFile := range metadataFiles {
		if strings.HasSuffix(mdFile, ".tmp") {
			mdFilesToDelete = append(mdFilesToDelete, mdFile)
		}
	}
	Log(fmt.Sprintf("Found %d metadata files .tmp for deletion", len(mdFilesToDelete)))
	for _, mdFile := range mdFilesToDelete {
		Log("Deleting " + mdFile)
		if err = s.storage.Delete(mdFile); err != nil {
			Log(fmt.Sprintf("Non-fatal error deleting %s from persistent storage: %s", mdFile, err))
			stats.NumErrsDeleting++
		}
	}

	Log(fmt.Sprintf("%d out of %d database files were garbage. %d errors deleting database or metadata files.",
		stats.NumGarbage, numFiles, stats.NumErrsDeleting))
	return &stats, nil
}

func (s *SnapshotManager) getFilesDelta(metadata SnapshotMetadata) ([]File, error) {
	files, err := s.storage.List(metadata.ReplicaID+"/", maxBackupFilesPerDB)
	if err != nil {
		return nil, err
	}
	Log(fmt.Sprintf("Found %d persistent files with prefix %s/", len(files), metadata.ReplicaID))

	filesSet := make(map[string]struct{}, len(files))
	for _, file := range files {
		filesSet[file] = struct{}{}
	}

	Log(fmt.Sprintf("%d files in this snapshot", len(metadata.Files)))
	incrementalFiles := make([]File, 0, len(metadata.Files))
	for _, file := range metadata.Files {
		if _, found := filesSet[getFilePath(metadata.ReplicaID, file)]; !found {
			incrementalFiles = append(incrementalFiles, file)
		}
	}
	Log(fmt.Sprintf("%d incremental files in this snapshot", len(incrementalFiles)))
	return incrementalFiles, nil
}

func getFilePath(replicaID string, file File) string {
	return fmt.Sprintf("%s/%s_%s_%d", replicaID, file.Name, file.Checksum, file.Size)
}

func getFileMetadataPath(replicaID string, file File) string {
	return fileMetadataPrefix + getFilePath(replicaID, file)
}

func getMetadataPath(s *SnapshotMetadata) string {
	return fmt.Sprintf("%s%s/%s_%d.json", metadataPrefix, s.ReplicaID, s.ID, s.Time.UnixNano())
}

func getStatsPath(s *SnapshotMetadata) string {
	return fmt.Sprintf("%s%s/%s_%d.json", statsPrefix, s.ReplicaID, s.ID, s.Time.UnixNano())
}

func getSizeFromFilePath(path string) (int64, error) {
	split := strings.Split(path, "_")
	if len(split) == 0 {
		return 0, fmt.Errorf("file path %s not in expected format", path)
	}
	return strconv.ParseInt(split[len(split)-1], 10, 64)
}

// GetInfoFromMetadataPath returns replica ID, snapshot ID, and snapshot time  based on metadataPath
func GetInfoFromMetadataPath(metadataPath string) (replicaID, snapshotID, time string, err error) {
	if !strings.HasPrefix(metadataPath, metadataPrefix) || !strings.HasSuffix(metadataPath, ".json") {
		err = fmt.Errorf("metadata path %s not in expected format", metadataPath)
		return
	}
	trimmed := strings.TrimPrefix(metadataPath, metadataPrefix)
	trimmed = strings.TrimSuffix(trimmed, ".json")
	firstsplit := strings.Split(trimmed, "/")
	if len(firstsplit) != 2 {
		err = fmt.Errorf("metadata path %s not in expected format", trimmed)
		return
	}
	replicaID = firstsplit[0]
	secondsplit := strings.Split(firstsplit[1], "_")
	if len(secondsplit) != 2 {
		err = fmt.Errorf("metadata path %s not in expected format", trimmed)
		return
	}
	snapshotID = secondsplit[0]
	time = secondsplit[1]
	return
}

func goodMetadataPathFormat(metadataPath string) bool {
	_, _, _, err := GetInfoFromMetadataPath(metadataPath)
	return err == nil
}
