package strata

import (
	"testing"

	"github.com/facebookgo/ensure"
)

func TestMetadata(t *testing.T) {
	m := NewMetadataStore()
	md0 := SnapshotMetadata{ReplicaID: "replset1_host1", ID: "1", Path: "/path/foo", Files: []File{{Name: "a"}, {Name: "b"}, {Name: "c"}}}
	md1 := SnapshotMetadata{ReplicaID: "replset1_host1", ID: "2", Path: "/path/foo", Files: []File{{Name: "a"}, {Name: "b"}, {Name: "c"}}}
	md2 := SnapshotMetadata{ReplicaID: "replset2_host2", ID: "1", Path: "/path/foo", Files: []File{{Name: "a"}, {Name: "b"}, {Name: "c"}}}
	err := m.Add(NewLazySMFromM(&md0))
	ensure.Nil(t, err)
	err = m.Add(NewLazySMFromM(&md1))
	ensure.Nil(t, err)
	err = m.Add(NewLazySMFromM(&md2))
	ensure.Nil(t, err)

	ensure.SameElements(t, m.GetReplicaIDs(), []string{"replset1_host1", "replset2_host2"})

	lazyMD, err := m.getForReplica("replset1_host1")
	ensure.Nil(t, err)
	ensure.DeepEqual(t, len(lazyMD), 2)
	mdRetrieved, err := lazyMD[0].Get()
	ensure.Nil(t, err)
	ensure.Subset(t, *mdRetrieved, SnapshotMetadata{ID: "1", Path: "/path/foo", Files: []File{{Name: "a"}, {Name: "b"}, {Name: "c"}}})
	mdRetrieved, err = lazyMD[1].Get()
	ensure.Nil(t, err)
	ensure.Subset(t, *mdRetrieved, SnapshotMetadata{ID: "2", Path: "/path/foo", Files: []File{{Name: "a"}, {Name: "b"}, {Name: "c"}}})

	lazyMD, err = m.getForReplica("replset2_host2")
	ensure.Nil(t, err)
	ensure.DeepEqual(t, len(lazyMD), 1)
	mdRetrieved, err = lazyMD[0].Get()
	ensure.Nil(t, err)
	ensure.Subset(t, *mdRetrieved, SnapshotMetadata{ID: "1", Path: "/path/foo", Files: []File{{Name: "a"}, {Name: "b"}, {Name: "c"}}})

	m.DeleteForReplicaByID("replset1_host1", "1")
	lazyMD, err = m.getForReplica("replset1_host1")
	ensure.Nil(t, err)
	// Both LazySnapshotMetadatas should still be in MetadataStore, but the first one should be marked for delete.
	ensure.DeepEqual(t, len(lazyMD), 2)
	ensure.True(t, lazyMD[0].DeleteMark)
	ensure.False(t, lazyMD[1].DeleteMark)

	mdRetrieved, err = lazyMD[1].Get()
	ensure.Nil(t, err)
	ensure.Subset(t, *mdRetrieved, SnapshotMetadata{ID: "2", Path: "/path/foo", Files: []File{{Name: "a"}, {Name: "b"}, {Name: "c"}}})
}
