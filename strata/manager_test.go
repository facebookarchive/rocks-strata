//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

package strata

import (
	"bufio"
	"io"
	"testing"
	"time"

	"github.com/facebookgo/ensure"
)

// Test our ability to read a snapshot out of the persistent store
func TestStoreAndRetrieveSnapshot(t *testing.T) {
	t.Parallel()
	s, err := NewSnapshotManager(
		NewMockReplica(),
		NewMockStorage(0))
	ensure.Nil(t, err)

	stats, err := s.CreateSnapshot("replset1_host1")
	ensure.Nil(t, err)
	ensure.True(t, stats.NumFiles == MockSnapshotNumFiles)
	ensure.True(t, stats.NumIncrementalFiles == MockSnapshotNumFiles)

	lazyMetadata, err := s.GetLazyMetadata("replset1_host1")
	ensure.Nil(t, err)
	ensure.DeepEqual(t, len(lazyMetadata), 1)

	md0, err := s.GetSnapshotMetadata("replset1_host1", "0")
	ensure.Nil(t, err)
	snapshot, err := s.GetSnapshot(*md0)
	ensure.Nil(t, err)
	content, err := getSnapshotContent(snapshot)
	ensure.Nil(t, err)

	ensure.SameElements(t, content, []string{"a", "b", "c"})
}

// Test our ability to read a snapshot off of the replica after creating it
func TestCreateAndRetrieveSnapshot(t *testing.T) {
	t.Parallel()
	s, err := NewSnapshotManager(
		NewMockReplica(),
		NewMockStorage(0))
	ensure.Nil(t, err)

	stats, err := s.CreateSnapshot("replset1_host1")
	ensure.Nil(t, err)
	ensure.True(t, stats.NumFiles == MockSnapshotNumFiles)
	ensure.True(t, stats.NumIncrementalFiles == MockSnapshotNumFiles)

	lazyMD, err := s.GetLazyMetadata("replset1_host1")
	ensure.Nil(t, err)
	ensure.DeepEqual(t, len(lazyMD), 1)

	metadata, err := s.GetSnapshotMetadata("replset1_host1", "0")
	ensure.Nil(t, err)
	snapshot, err := s.replica.GetSnapshot(*metadata)
	ensure.Nil(t, err)
	content, err := getSnapshotContent(snapshot)
	ensure.Nil(t, err)
	ensure.SameElements(t, content, []string{"a", "b", "c"})
}

func TestGetBackupStats(t *testing.T) {
	t.Parallel()

	delay := 3 * time.Second
	s, err := NewSnapshotManager(
		NewMockReplica(),
		NewMockStorage(delay))
	ensure.Nil(t, err)

	start := time.Now()
	stats, err := s.CreateSnapshot("replset1_host1")
	elapsed := time.Since(start)

	ensure.Nil(t, err)
	ensure.True(t, stats.NumFiles == MockSnapshotNumFiles)
	ensure.True(t, stats.NumIncrementalFiles == MockSnapshotNumFiles)
	ensure.True(t, stats.SizeFiles == MockSnapshotSize)
	ensure.True(t, stats.SizeIncrementalFiles == stats.SizeFiles)
	ensure.True(t, stats.Duration >= delay)
	ensure.True(t, stats.Duration <= elapsed)

	lazyMD, err := s.GetLazyMetadata("replset1_host1")
	ensure.Nil(t, err)
	ensure.DeepEqual(t, len(lazyMD), 1)

	// rstats stands for "retrieved stats"
	rstats, err := s.GetBackupStats(&lazyMD[0], false)
	ensure.True(t, rstats.Duration >= delay)
	// Duration persisted might be slightly less than duration returned by
	// CreateSnapshot, since the duration returned by CreateSnapshot includes
	// the time needed to persist stats.
	ensure.True(t, rstats.Duration <= stats.Duration)
	ensure.True(t, rstats.NumFiles == MockSnapshotNumFiles)
	ensure.True(t, rstats.NumIncrementalFiles == MockSnapshotNumFiles)
	ensure.True(t, rstats.SizeFiles == stats.SizeFiles)
	ensure.True(t, rstats.SizeIncrementalFiles == stats.SizeIncrementalFiles)
}

func TestCreateAndDeleteSnapshot(t *testing.T) {
	t.Parallel()
	s, err := NewSnapshotManager(
		NewMockReplica(),
		NewMockStorage(0))
	ensure.Nil(t, err)

	stats0, err := s.CreateSnapshot("replset1_host1")
	ensure.Nil(t, err)
	ensure.True(t, stats0.NumFiles == MockSnapshotNumFiles)
	ensure.True(t, stats0.NumIncrementalFiles == MockSnapshotNumFiles)

	stats1, err := s.CreateSnapshot("replset1_host1")
	ensure.Nil(t, err)
	ensure.True(t, stats1.NumFiles == MockSnapshotNumFiles)
	ensure.True(t, stats1.NumIncrementalFiles == 0)

	lazyMD, err := s.GetLazyMetadata("replset1_host1")
	ensure.Nil(t, err)
	ensure.DeepEqual(t, len(lazyMD), 2)

	snapToSave, err := lazyMD[0].Get()
	ensure.Nil(t, err)
	snapToDelete, err := lazyMD[1].Get()
	ensure.Nil(t, err)

	err = s.DeleteSnapshot(*snapToDelete)
	ensure.Nil(t, err)

	lazyMD, err = s.GetLazyMetadata("replset1_host1")
	ensure.Nil(t, err)
	// snapToDelete has been marked for deletion, but it hasn't actually been
	// deleted because we haven't called SaveMetadataForReplica()
	ensure.DeepEqual(t, len(lazyMD), 2)
	snap0, err := lazyMD[0].Get()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, snap0, snapToSave)

	err = s.SaveMetadataForReplica("replset1_host1")
	ensure.Nil(t, err)
	err = s.RefreshMetadata()
	ensure.Nil(t, err)

	lazyMD, err = s.GetLazyMetadata("replset1_host1")
	ensure.Nil(t, err)
	// Now snapToDelete is not in MetadataStore
	ensure.DeepEqual(t, len(lazyMD), 1)
}

func TestRestoreSnapshot(t *testing.T) {
	t.Parallel()
	s, err := NewSnapshotManager(
		NewMockReplica(),
		NewMockStorage(0))
	ensure.Nil(t, err)

	_, err = s.CreateSnapshot("replset1_host1")
	ensure.Nil(t, err)

	lazyMD, err := s.GetLazyMetadata("replset1_host1")
	ensure.Nil(t, err)
	ensure.DeepEqual(t, len(lazyMD), 1)

	metadata, err := lazyMD[0].Get()
	ensure.Nil(t, err)
	stats, err := s.RestoreSnapshot("replset1_host2", "/foo", *metadata)
	ensure.True(t, stats.NumFiles > 0)
	ensure.True(t, stats.NumIncrementalFiles > 0)
	ensure.Nil(t, err)
}

func TestCorruptedSnapshot(t *testing.T) {
	t.Parallel()
	storage := NewMockStorage(0)
	s, err := NewSnapshotManager(
		NewMockReplica(),
		storage)
	ensure.Nil(t, err)

	_, err = s.CreateSnapshot("replset1_host1")
	ensure.Nil(t, err)

	// Corrupt an object
	objectKeys, err := storage.List("replset1_host1/", 1)
	ensure.Nil(t, err)
	ensure.True(t, len(objectKeys) == 1)
	storage.Corrupt(objectKeys[0])

	// Verify that restore fails
	lazyMD, err := s.GetLazyMetadata("replset1_host1")
	ensure.Nil(t, err)
	ensure.DeepEqual(t, len(lazyMD), 1)
	metadata, err := lazyMD[0].Get()
	ensure.Nil(t, err)
	_, err = s.RestoreSnapshot("replset1_host2", "/foo", *metadata)
	ensure.NotNil(t, err)
}

func TestSaveSingleReplicaMetadata(t *testing.T) {
	t.Parallel()

	s, err := NewSnapshotManager(
		NewMockReplica(),
		NewMockStorage(0))
	ensure.Nil(t, err)

	_, err = s.CreateSnapshot("replset1_host1")
	ensure.Nil(t, err)
	_, err = s.CreateSnapshot("replset1_host2")
	ensure.Nil(t, err)

	// should see 1 item for each replica ID
	lazyMD, err := s.GetLazyMetadata("replset1_host1")
	ensure.DeepEqual(t, len(lazyMD), 1)
	ensure.Nil(t, err)
	lazyMD, err = s.GetLazyMetadata("replset1_host2")
	ensure.DeepEqual(t, len(lazyMD), 1)
	ensure.Nil(t, err)

	err = s.SaveMetadataForReplica("replset1_host1")
	ensure.Nil(t, err)

	// reload data from persistence manager
	err = s.RefreshMetadata()
	ensure.Nil(t, err)

	lazyMD, err = s.GetLazyMetadata("replset1_host1")
	ensure.DeepEqual(t, len(lazyMD), 1)
	ensure.Nil(t, err)
	// This should now return an error since the metadata wasn't persisted
	lazyMD, err = s.GetLazyMetadata("replset1_host2")
	ensure.DeepEqual(t, len(lazyMD), 0)
	ensure.NotNil(t, err)
}

func getSnapshotContent(snapshot *Snapshot) ([]string, error) {
	content := make([]string, 0, len(snapshot.Metadata.Files))

	for _, file := range snapshot.Metadata.Files {
		reader, err := snapshot.GetReader(file.Name)
		if err != nil {
			return nil, err
		}
		r := bufio.NewReader(reader)
		str, err := r.ReadString(0)
		if err != nil && err != io.EOF {
			return nil, err
		}
		content = append(content, str)
	}

	return content, nil
}
