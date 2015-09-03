package lrs3test

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"testing"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/facebookgo/ensure"
	"github.com/facebookgo/mgotest"
	"github.com/facebookgo/rocks-strata/strata"
	"github.com/facebookgo/rocks-strata/strata/mongo/lreplica"
	"github.com/facebookgo/rocks-strata/strata/s3storage"
)

func skipIfOldMongo(t *testing.T) {
	if os.Getenv("USING_MONGO_3X") != "true" {
		t.Skip("Skipping test. Set USING_MONGO_3X=true to run.")
	}
}

func TestIntegration(t *testing.T) {
	skipIfOldMongo(t)

	dbpath, err := ioutil.TempDir("", "lreplica_s3storage_test_dbpath_")
	ensure.Nil(t, err)
	fmt.Println("Using temporary database path", dbpath)

	s3 := s3storage.NewMockS3(t)
	defer s3.Stop()
	s, err := s3storage.NewStorageWithMockS3(s3)
	ensure.Nil(t, err)

	replicaID := "faux-replicaID"
	minEntryID := -100
	maxEntryID := int(math.Pow(2, 12))
	maxBackgroundCopies := 24

	// Populate database and save snapshots
	{
		mongo := mgotest.NewStartedServer(t, "--storageEngine=rocksdb", "--dbpath="+dbpath)
		defer mongo.Stop()
		collection := mongo.Session().DB("tdb").C("tc")

		r := lreplica.NewMockLocalReplica(mongo, maxBackgroundCopies)
		manager, err := strata.NewSnapshotManager(r, s)
		ensure.Nil(t, err)

		// Snapshot 0 should only have entries with _id % 3 == 0
		for i := 0; i <= maxEntryID; i += 3 {
			if err := collection.Insert(bson.M{"_id": i, "answer": i}); err != nil {
				t.Fatal(err)
			}
		}
		_, err = manager.CreateSnapshot(replicaID)
		ensure.Nil(t, err)

		// Snapshot 1 should have entries with _id % 3 == 0 or 1
		for i := 1; i <= maxEntryID; i += 3 {
			if err := collection.Insert(bson.M{"_id": i, "answer": i}); err != nil {
				t.Fatal(err)
			}
		}
		_, err = manager.CreateSnapshot(replicaID)
		ensure.Nil(t, err)

		// Restore attempt while database is in use should fail cleanly
		err = manager.SaveMetadataForReplica(replicaID)
		ensure.Nil(t, err)
		err = manager.RefreshMetadata()
		ensure.Nil(t, err)
		_, err = restoreSnapshot(s, replicaID, "", "0")
		ensure.True(t, fmt.Sprintf("%s", err) == "resource temporarily unavailable")

		// Snapshot 2 and subsequent snapshots shouldhave entries with negative _id
		// Snapshot 2 will be deleted
		for i := minEntryID; i < 0; i++ {
			if err := collection.Insert(bson.M{"_id": i, "answer": i}); err != nil {
				t.Fatal(err)
			}
		}
		_, err = manager.CreateSnapshot(replicaID)
		ensure.Nil(t, err)

		// Snapshot 3 does not have entries with _id % 4 == 0
		for i := 0; i <= maxEntryID; i += 4 {
			if err := collection.RemoveId(i); err != nil && err != mgo.ErrNotFound {
				t.Fatal(err)
			}
		}
		_, err = manager.CreateSnapshot(replicaID)
		ensure.Nil(t, err)

		// Remove local backups directory. We should see no ill effect.
		backuppath := dbpath + "/backup"
		_, err = os.Stat(backuppath)
		ensure.Nil(t, err)
		err = os.RemoveAll(backuppath)
		ensure.Nil(t, err)

		// Snapshot 4's entries with _id % 4 == 1 have a flipped value for "answer"
		for i := 1; i <= maxEntryID; i += 4 {
			err := collection.UpdateId(i, bson.M{"_id": i, "answer": -i})
			if err != nil && err != mgo.ErrNotFound {
				t.Fatal(err)
			}
		}
		_, err = manager.CreateSnapshot(replicaID)
		ensure.Nil(t, err)

		// Check that manager's metadata store has five snapshots
		lazyMetadatas, err := manager.GetLazyMetadata(replicaID)
		ensure.Nil(t, err)
		ensure.DeepEqual(t, len(lazyMetadatas), 5)

		// Persist to S3
		err = manager.SaveMetadataForReplica(replicaID)
		ensure.Nil(t, err)
		err = manager.RefreshMetadata()
		ensure.Nil(t, err)

		// Collect garbage. There shouldn't be any garbage files.
		gcStats, err := manager.CollectGarbage(replicaID)
		ensure.Nil(t, err)
		ensure.DeepEqual(t, gcStats.NumGarbage, 0)
		ensure.DeepEqual(t, gcStats.NumErrsDeleting, 0)

		// Delete snapshot 2, persist, and refresh metadata
		metadata1p5, err := manager.GetSnapshotMetadata(replicaID, "2")
		ensure.Nil(t, err)
		err = manager.DeleteSnapshot(*metadata1p5)
		ensure.Nil(t, err)
		err = manager.SaveMetadataForReplica(replicaID)
		ensure.Nil(t, err)
		err = manager.RefreshMetadata()
		ensure.Nil(t, err)

		// Check that manager's metadata store has four snapshots
		lazyMetadatas, err = manager.GetLazyMetadata(replicaID)
		ensure.Nil(t, err)
		ensure.DeepEqual(t, len(lazyMetadatas), 4)

		// Collect garbage. There should be garbage files.
		gcStats, err = manager.CollectGarbage(replicaID)
		ensure.Nil(t, err)
		ensure.True(t, gcStats.NumGarbage > 0)
		ensure.DeepEqual(t, gcStats.NumErrsDeleting, 0)

		mongo.Stop()
	}

	// Check snapshot 1
	_, err = restoreSnapshot(s, replicaID, "", "1")
	ensure.Nil(t, err)
	// Check that restore worked
	{
		mongo := mgotest.NewStartedServer(t, "--storageEngine=rocksdb", "--dbpath="+dbpath)
		defer mongo.Stop()
		collection := mongo.Session().DB("tdb").C("tc")
		out := bson.M{}
		for i := minEntryID; i <= maxEntryID; i++ {
			if i >= 0 && (i%3 == 0 || i%3 == 1) {
				if err := collection.FindId(i).One(out); err != nil {
					t.Fatal(err)
				}
				if out["answer"] != i {
					t.Fatalf("did not find expected answer for id %d", i)
				}
			} else {
				if err := collection.FindId(i).One(out); err == nil {
					t.Fatalf("Did not expect %d to be in the collection", i)
				}
			}
		}
		mongo.Stop()
	}

	// Check snapshot 0
	_, err = restoreSnapshot(s, replicaID, "", "0")
	ensure.Nil(t, err)
	// Check that restore worked
	{
		mongo := mgotest.NewStartedServer(t, "--storageEngine=rocksdb", "--dbpath="+dbpath)
		defer mongo.Stop()
		collection := mongo.Session().DB("tdb").C("tc")
		out := bson.M{}
		for i := minEntryID; i <= maxEntryID; i++ {
			if i >= 0 && i%3 == 0 {
				if err := collection.FindId(i).One(out); err != nil {
					t.Fatal(err)
				}
				if out["answer"] != i {
					t.Fatalf("did not find expected answer for id %d", i)
				}
			} else {
				if err := collection.FindId(i).One(out); err == nil {
					t.Fatalf("Did not expect %d to be in the collection", i)
				}
			}
		}
		mongo.Stop()
	}

	// Check snapshot 3
	_, err = restoreSnapshot(s, replicaID, "", "3")
	ensure.Nil(t, err)
	{
		mongo := mgotest.NewStartedServer(t, "--storageEngine=rocksdb", "--dbpath="+dbpath)
		defer mongo.Stop()
		collection := mongo.Session().DB("tdb").C("tc")
		out := bson.M{}
		for i := minEntryID; i <= maxEntryID; i++ {
			if i < 0 || (i%3 == 0 || i%3 == 1) && i%4 != 0 {
				if err := collection.FindId(i).One(out); err != nil {
					t.Fatal(err)
				}
				if out["answer"] != i {
					t.Fatalf("did not find expected answer for id %d", i)
				}
			} else {
				if err := collection.FindId(i).One(out); err == nil {
					t.Fatalf("Did not expect %d to be in the collection", i)
				}
			}
		}
		mongo.Stop()
	}

	// Check snapshot 4
	_, err = restoreSnapshot(s, replicaID, "", "4")
	ensure.Nil(t, err)
	{
		mongo := mgotest.NewStartedServer(t, "--storageEngine=rocksdb", "--dbpath="+dbpath)
		defer mongo.Stop()
		collection := mongo.Session().DB("tdb").C("tc")
		out := bson.M{}
		for i := minEntryID; i <= maxEntryID; i++ {
			if i < 0 || (i%3 == 0 || i%3 == 1) && i%4 != 0 {
				if err := collection.FindId(i).One(out); err != nil {
					t.Fatal(err)
				}
				correct := i
				if i >= 0 && i%4 == 1 {
					correct = -i
				}
				if out["answer"] != correct {
					t.Fatalf("did not find expected answer for id %d", i)
				}
			} else {
				if err := collection.FindId(i).One(out); err == nil {
					t.Fatalf("Did not expect %d to be in the collection", i)
				}
			}
		}
		mongo.Stop()
	}

	err = os.RemoveAll(dbpath)
	ensure.Nil(t, err)
}

// TestCollectGarbage tests that the stats returned by CollectGarbage make sense
func TestCollectGarbage(t *testing.T) {
	skipIfOldMongo(t)

	dbpath, err := ioutil.TempDir("", "lreplica_s3storage_test_dbpath_")
	ensure.Nil(t, err)
	// Don't defer cleanup of dbpath because we want dbpath to be around for debugging if test fails
	fmt.Println("Using temporary database path", dbpath)

	s3 := s3storage.NewMockS3(t)
	defer s3.Stop()
	s, err := s3storage.NewStorageWithMockS3(s3)
	ensure.Nil(t, err)

	replicaID := "faux-replicaID"
	maxEntryID := 256

	mongo := mgotest.NewStartedServer(t, "--storageEngine=rocksdb", "--dbpath="+dbpath)
	defer mongo.Stop()
	collection := mongo.Session().DB("tdb").C("tc")

	r := lreplica.NewMockLocalReplica(mongo, 1)
	manager, err := strata.NewSnapshotManager(r, s)
	ensure.Nil(t, err)

	// There should not be any files yet
	gcStats, err := manager.CollectGarbage(replicaID)
	ensure.Nil(t, err)
	ensure.True(t, gcStats.NumNeeded == 0)
	ensure.True(t, gcStats.NumGarbage == 0)

	for i := 0; i <= maxEntryID; i += 3 {
		if err := collection.Insert(bson.M{"_id": i, "answer": i}); err != nil {
			t.Fatal(err)
		}
	}
	// Snapshot 0
	_, err = manager.CreateSnapshot(replicaID)
	ensure.Nil(t, err)
	err = manager.SaveMetadataForReplica(replicaID)
	ensure.Nil(t, err)
	err = manager.RefreshMetadata()
	ensure.Nil(t, err)
	gcStats0, err := manager.CollectGarbage(replicaID)
	ensure.Nil(t, err)
	ensure.True(t, gcStats0.NumNeeded > 0)
	ensure.True(t, gcStats0.NumNeeded == gcStats0.NumFMNeeded)
	ensure.True(t, gcStats0.NumStatsNeeded == 1)
	ensure.True(t, gcStats0.NumGarbage == 0)
	ensure.True(t, gcStats0.NumFMGarbage == 0)
	ensure.True(t, gcStats0.NumStatsGarbage == 0)
	ensure.True(t, gcStats0.SizeNeeded > 0)
	ensure.True(t, gcStats0.SizeGarbage == 0)

	// Snapshot 1 is a copy of snapshot 0
	_, err = manager.CreateSnapshot(replicaID)
	ensure.Nil(t, err)
	err = manager.SaveMetadataForReplica(replicaID)
	ensure.Nil(t, err)
	err = manager.RefreshMetadata()
	ensure.Nil(t, err)
	gcStats1, err := manager.CollectGarbage(replicaID)
	ensure.Nil(t, err)
	ensure.True(t, gcStats0.NumNeeded == gcStats1.NumNeeded)
	ensure.True(t, gcStats0.NumFMNeeded == gcStats1.NumFMNeeded)
	ensure.True(t, gcStats1.NumStatsNeeded == 2)
	ensure.True(t, gcStats1.NumGarbage == 0)
	ensure.True(t, gcStats1.NumFMGarbage == 0)
	ensure.True(t, gcStats1.NumStatsGarbage == 0)

	// Delete snapshot 1. This shouldn't cause any files to become garbage
	// because snapshot 0 is still around.
	metadata, err := manager.GetSnapshotMetadata(replicaID, "1")
	ensure.Nil(t, err)
	err = manager.DeleteSnapshot(*metadata)
	ensure.Nil(t, err)
	err = manager.SaveMetadataForReplica(replicaID)
	ensure.Nil(t, err)
	err = manager.RefreshMetadata()
	ensure.Nil(t, err)
	gcStats, err = manager.CollectGarbage(replicaID)
	ensure.Nil(t, err)
	ensure.True(t, gcStats0.NumNeeded == gcStats.NumNeeded)
	ensure.True(t, gcStats.NumStatsNeeded == 1)
	ensure.True(t, gcStats.NumGarbage == 0)
	ensure.True(t, gcStats.NumFMGarbage == 0)
	ensure.True(t, gcStats.NumStatsGarbage == 1)

	// New snapshot 1 (ID gets re-used)
	for i := 1; i <= maxEntryID; i += 3 {
		if err := collection.Insert(bson.M{"_id": i, "answer": i}); err != nil {
			t.Fatal(err)
		}
	}
	_, err = manager.CreateSnapshot(replicaID)
	ensure.Nil(t, err)
	err = manager.SaveMetadataForReplica(replicaID)
	ensure.Nil(t, err)
	err = manager.RefreshMetadata()
	ensure.Nil(t, err)

	// There should be new files but no garbage
	gcStats1, err = manager.CollectGarbage(replicaID)
	ensure.Nil(t, err)
	ensure.True(t, gcStats1.NumNeeded > gcStats0.NumNeeded)
	ensure.True(t, gcStats1.NumNeeded == gcStats1.NumFMNeeded)
	ensure.True(t, gcStats1.NumStatsNeeded == 2)
	ensure.True(t, gcStats1.NumGarbage == 0)
	ensure.True(t, gcStats1.NumFMGarbage == 0)
	ensure.True(t, gcStats1.NumStatsGarbage == 0)

	// Delete snapshot 1. This should cause a known number of files to become garbage.
	metadata, err = manager.GetSnapshotMetadata(replicaID, "1")
	ensure.Nil(t, err)
	err = manager.DeleteSnapshot(*metadata)
	ensure.Nil(t, err)
	err = manager.SaveMetadataForReplica(replicaID)
	ensure.Nil(t, err)
	err = manager.RefreshMetadata()
	ensure.Nil(t, err)
	gcStats, err = manager.CollectGarbage(replicaID)
	ensure.Nil(t, err)
	ensure.True(t, gcStats.NumNeeded == gcStats0.NumNeeded)
	ensure.True(t, gcStats.NumFMNeeded == gcStats.NumNeeded)
	ensure.True(t, gcStats.NumStatsNeeded == 1)
	ensure.True(t, gcStats.NumGarbage == gcStats1.NumNeeded-gcStats0.NumNeeded)
	ensure.True(t, gcStats.NumGarbage == gcStats.NumFMGarbage)
	ensure.True(t, gcStats.NumStatsGarbage == 1)
	ensure.True(t, gcStats.NumErrsDeleting == 0)

	// Delete snapshot 0
	metadata, err = manager.GetSnapshotMetadata(replicaID, "0")
	ensure.Nil(t, err)
	err = manager.DeleteSnapshot(*metadata)
	ensure.Nil(t, err)
	err = manager.SaveMetadataForReplica(replicaID)
	ensure.Nil(t, err)
	err = manager.RefreshMetadata()
	ensure.Nil(t, err)
	gcStats, err = manager.CollectGarbage(replicaID)
	ensure.Nil(t, err)
	ensure.True(t, gcStats.NumNeeded == 0)
	ensure.True(t, gcStats.NumFMNeeded == 0)
	ensure.True(t, gcStats.NumStatsNeeded == 0)
	ensure.True(t, gcStats.NumGarbage == gcStats0.NumNeeded)
	ensure.True(t, gcStats.NumFMGarbage == gcStats.NumGarbage)
	ensure.True(t, gcStats.NumStatsGarbage == 1)
	ensure.True(t, gcStats.NumErrsDeleting == 0)

	// Garbage collect again. There shouldn't be any files.
	gcStats, err = manager.CollectGarbage(replicaID)
	ensure.Nil(t, err)
	ensure.True(t, gcStats.NumNeeded == 0)
	ensure.True(t, gcStats.NumGarbage == 0)
	ensure.True(t, gcStats.NumFMNeeded == 0)
	ensure.True(t, gcStats.NumStatsNeeded == 0)

	// Stop mongo before we delete its dbpath
	mongo.Stop()
	err = os.RemoveAll(dbpath)
	ensure.Nil(t, err)
}

func TestRestoreToDifferentPath(t *testing.T) {
	skipIfOldMongo(t)

	dbpath1, err := ioutil.TempDir("", "lreplica_s3storage_test_dbpath_")
	ensure.Nil(t, err)
	fmt.Println("Using temporary database path", dbpath1)
	dbpath2, err := ioutil.TempDir("", "lreplica_s3storage_test_dbpath_")
	ensure.Nil(t, err)
	fmt.Println("Using temporary database path", dbpath2)

	s3 := s3storage.NewMockS3(t)
	defer s3.Stop()
	s, err := s3storage.NewStorageWithMockS3(s3)
	ensure.Nil(t, err)

	replicaID := "faux-replicaID"
	maxEntryID := 100

	// Populate database on dbpath1 and save a snapshot
	{
		mongo := mgotest.NewStartedServer(t, "--storageEngine=rocksdb", "--dbpath="+dbpath1)
		defer mongo.Stop()
		collection := mongo.Session().DB("tdb").C("tc")

		r := lreplica.NewMockLocalReplica(mongo, 1)
		manager, err := strata.NewSnapshotManager(r, s)
		ensure.Nil(t, err)

		// Snapshot 0 should only have entries with _id % 2 == 0
		for i := 0; i <= maxEntryID; i += 2 {
			if err := collection.Insert(bson.M{"_id": i, "answer": i}); err != nil {
				t.Fatal(err)
			}
		}
		_, err = manager.CreateSnapshot(replicaID)
		ensure.Nil(t, err)

		err = manager.SaveMetadataForReplica(replicaID)
		ensure.Nil(t, err)
	}

	// Restore and test snapshot
	_, err = restoreSnapshot(s, replicaID, dbpath2, "0")
	ensure.Nil(t, err)
	{
		mongo := mgotest.NewStartedServer(t, "--storageEngine=rocksdb", "--dbpath="+dbpath2)
		defer mongo.Stop()
		collection := mongo.Session().DB("tdb").C("tc")
		out := bson.M{}
		for i := 0; i <= maxEntryID; i++ {
			if i%2 == 0 {
				if err := collection.FindId(i).One(out); err != nil {
					t.Fatal(err)
				}
				if out["answer"] != i {
					t.Fatalf("did not find expected answer for id %d", i)
				}
			} else {
				if err := collection.FindId(i).One(out); err == nil {
					t.Fatalf("Did not expect %d to be in the collection", i)
				}
			}
		}
		mongo.Stop()
	}

	err = os.RemoveAll(dbpath1)
	ensure.Nil(t, err)
	err = os.RemoveAll(dbpath2)
	ensure.Nil(t, err)
}

// Basically tests manager.GetReplicaIDs
func TestGetReplicaIDs(t *testing.T) {
	skipIfOldMongo(t)

	s3 := s3storage.NewMockS3(t)
	defer s3.Stop()
	s, err := s3storage.NewStorageWithMockS3(s3)
	ensure.Nil(t, err)

	replicaID1 := "faux-replicaID1"
	replicaID2 := "faux-replicaID2"

	mongo := mgotest.NewStartedServer(t, "--storageEngine=rocksdb")
	defer mongo.Stop()

	r := lreplica.NewMockLocalReplica(mongo, 1)
	manager, err := strata.NewSnapshotManager(r, s)
	ensure.Nil(t, err)

	_, err = manager.CreateSnapshot(replicaID1)
	ensure.Nil(t, err)
	_, err = manager.CreateSnapshot(replicaID2)
	ensure.Nil(t, err)
	_, err = manager.CreateSnapshot(replicaID1)
	ensure.Nil(t, err)

	replicaIDs := manager.GetReplicaIDs()
	ensure.DeepEqual(t, len(replicaIDs), 2)
}

func restoreSnapshot(s *s3storage.S3Storage, replicaID, targetPath, snapshotID string) (*strata.SnapshotStats, error) {
	r := lreplica.NewMockLocalReplica(nil, 24)
	manager, err := strata.NewSnapshotManager(r, s)
	if err != nil {
		return nil, err
	}

	metadata, err := manager.GetSnapshotMetadata(replicaID, snapshotID)
	if err != nil {
		return nil, err
	}
	return manager.RestoreSnapshot(replicaID, targetPath, *metadata)
}

// TODO(agf): Perf test: hundreds of replicas with 24 * 30 snapshots each
