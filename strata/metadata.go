package strata

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"time"
)

// SnapshotMetadata stores all information related to the snapshot.
type SnapshotMetadata struct {
	Files []File `json:"files"`
	// ReplicaID should uniquely identify the replica in the environment
	// An example would be combination of replica set and hostname
	ReplicaID string `json:"source_id"`
	// ID should uniquely identify this snapshot within the context
	// of a ReplicaID. It can be as simple as an incrementing counter
	ID string `json:"id"`
	// Path is where the snapshot originated from on disk
	Path string    `json:"path"`
	Tags []string  `json:"tags"`
	Time time.Time `json:"time"`
}

// File contains snapshot file attributes
// All of these attributes can be determined without looking through the whole file
type File struct {
	// The original name of the file
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	Checksum string `json:"checksum"` // This is a partial checksum
}

// FileMetadata stores the complete checksum for a database file.
// Unlike File, which are persisted as part of SnapshotMetadata, each FileMetadata
// are persisted individually.
// Additional attributes may be added in the future.
type FileMetadata struct {
	CompleteChecksum []byte `json:"completechecksum"`
}

// LazySnapshotMetadata is used to lazily handle access to a
// SnapshotMetadata, touching persistent storage only when necessary.
// A LazySnapshotMetadata is in a usable state if MetadataPath is set and readerFunc or
// snapshotMetadata is set.
type LazySnapshotMetadata struct {
	// The path of the persistent-storage file for the SnapshotMetadata
	// Snapshot ID and time should be embedded in path name
	MetadataPath string

	// Indicate that the SnapshotMetadata should be removed from persistent storage
	DeleteMark bool

	// Indicate that the SnapshotMetadata should be saved to persistent storage
	// Overridden by DeleteMark
	SaveMark bool

	readerFunc       ReaderFunc
	snapshotMetadata *SnapshotMetadata
	err              error
}

// NewLazySMFromM constructs a LazySnapshotMetadata from a SnapshotMetadata
func NewLazySMFromM(s *SnapshotMetadata) *LazySnapshotMetadata {
	return &LazySnapshotMetadata{
		MetadataPath:     getMetadataPath(s),
		snapshotMetadata: s,
	}
}

// NewLazySMFromPath constructs a LazySnapshotMetadata from a metadata path and a ReaderFunc.
// The ReaderFunc should return a Reader for the metadata path.
func NewLazySMFromPath(metadataPath string, readerFunc ReaderFunc) *LazySnapshotMetadata {
	return &LazySnapshotMetadata{
		MetadataPath: metadataPath,
		readerFunc:   readerFunc,
	}
}

// Get returns SnapshotMetadata, loading it from persistent storage if necessary
func (z *LazySnapshotMetadata) Get() (*SnapshotMetadata, error) {
	if z.err != nil {
		// There was an error last time we tried to Get().
		// Don't try again, since the error is likely to occur again.
		return nil, z.err
	}
	if z.snapshotMetadata != nil {
		return z.snapshotMetadata, nil
	}

	var reader io.ReadCloser
	z.err = Try(func() error {
		var err error
		reader, err = z.readerFunc()
		return err
	}, "")
	if z.err != nil {
		return nil, z.err
	}
	defer reader.Close()

	mdBytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	z.snapshotMetadata = &SnapshotMetadata{}
	z.err = json.Unmarshal(mdBytes, z.snapshotMetadata)
	if z.err != nil {
		return nil, z.err
	}

	return z.snapshotMetadata, nil
}

// Reset sets snapshotMetadata and err to nil. It does not change DeleteMark or SaveMark.
func (z *LazySnapshotMetadata) Reset() {
	z.snapshotMetadata = nil
	z.err = nil
}

// ByTime is used to sort a slice of LazySnapshotMetadata by time.
// Implements sort.Interface.
type ByTime []LazySnapshotMetadata

// Len is the number of elements in the collection
func (lz ByTime) Len() int {
	return len(lz)
}

// Swap swaps the elements with indexes i and j
func (lz ByTime) Swap(i, j int) {
	lz[i], lz[j] = lz[j], lz[i]
}

// Less reports whether the element with index i should sort before the element with index j
func (lz ByTime) Less(i, j int) bool {
	_, _, itime, err := GetInfoFromMetadataPath(lz[i].MetadataPath)
	if err != nil {
		Log(fmt.Sprintf("Problem 1 with ByTime.Less: %s", err))
		return true
	}
	_, _, jtime, err := GetInfoFromMetadataPath(lz[j].MetadataPath)
	if err != nil {
		Log(fmt.Sprintf("Problem 2 with ByTime.Less: %s", err))
		return true
	}
	itimeInt, err := strconv.ParseInt(itime, 10, 64)
	if err != nil {
		Log(fmt.Sprintf("Problem 3 with ByTime.Less: %s", err))
		return true
	}
	jtimeInt, err := strconv.ParseInt(jtime, 10, 64)
	if err != nil {
		Log(fmt.Sprintf("Problem 4 with ByTime.Less: %s", err))
		return true
	}
	return itimeInt < jtimeInt
}

// MetadataStore uses a map to store one slice of LazySnapshotMetadatas for each replica ID
type MetadataStore struct {
	lazySnapshots map[string][]LazySnapshotMetadata
}

// NewMetadataStore constructs an empty MetadataStore
func NewMetadataStore() MetadataStore {
	return MetadataStore{lazySnapshots: make(map[string][]LazySnapshotMetadata)}
}

// Add inserts a LazySnapshotMetadata into the appropriate slice of MetadataStore
func (m *MetadataStore) Add(lazy *LazySnapshotMetadata) error {
	replicaID, _, _, err := GetInfoFromMetadataPath(lazy.MetadataPath)
	if err != nil {
		return err
	}

	if _, exists := m.lazySnapshots[replicaID]; !exists {
		m.lazySnapshots[replicaID] = []LazySnapshotMetadata{*lazy}
	} else {
		m.lazySnapshots[replicaID] = append(m.lazySnapshots[replicaID], *lazy)
	}
	return nil
}

// getForReplica returns all LazySnapshotMetadata for the given replica ID
func (m *MetadataStore) getForReplica(replicaID string) ([]LazySnapshotMetadata, error) {
	if lazySnapshots, ok := m.lazySnapshots[replicaID]; ok {
		return lazySnapshots, nil
	}
	return make([]LazySnapshotMetadata, 0), ErrNotFound(replicaID)
}

// Delete marks metadata to be deleted from persistent storage the next time that
// SaveMetadataForReplica() is called.
func (m *MetadataStore) Delete(metadata SnapshotMetadata) error {
	lazyMetadatas, err := m.getForReplica(metadata.ReplicaID)
	if err != nil {
		return err
	}
	path := getMetadataPath(&metadata)
	for i, lazy := range lazyMetadatas {
		if lazy.MetadataPath == path {
			m.lazySnapshots[metadata.ReplicaID][i].DeleteMark = true
			return nil
		}
	}
	return ErrNotFound(path)
}

// DeleteEarlierThan finds snapshot metadata from earlier than the specified
// time and marks them to be deleted from persistent storage the next time that
// SaveMetadataForReplica() is called.
// The first return value is a slice of the metadata paths that were marked for
// deletion. Currently, this return value is only used as a statistic.
func (m *MetadataStore) DeleteEarlierThan(replicaID string, t time.Time) ([]string, error) {
	var pathsForDeletion []string
	lazyMetadatas, err := m.getForReplica(replicaID)
	if err != nil {
		return pathsForDeletion, err
	}
	for i, lazy := range lazyMetadatas {
		_, _, snapshotTimeStr, err := GetInfoFromMetadataPath(lazy.MetadataPath)
		snapshotTimeInt, err := strconv.ParseInt(snapshotTimeStr, 10, 64)
		if err != nil {
			return pathsForDeletion, err
		}
		snapshotTime := time.Unix(0, snapshotTimeInt)
		if snapshotTime.Before(t) {
			m.lazySnapshots[replicaID][i].DeleteMark = true
			pathsForDeletion = append(pathsForDeletion, lazy.MetadataPath)
		}
	}
	return pathsForDeletion, nil
}

// DeleteForReplicaByID finds the metadata that corresponds to replicaID and
// id, and marks the metadata to be deleted from persistent storage the next
// time that SaveMetadataForReplica() is called.
func (m *MetadataStore) DeleteForReplicaByID(replicaID string, id string) error {
	lazyMetadatas, err := m.getForReplica(replicaID)
	if err != nil {
		return err
	}
	for i, lazy := range lazyMetadatas {
		_, lazyID, _, err := GetInfoFromMetadataPath(lazy.MetadataPath)
		if err != nil {
			return err
		}
		if lazyID == id {
			m.lazySnapshots[replicaID][i].DeleteMark = true
			return nil
		}
	}
	return ErrNotFound(fmt.Sprintf("Replica ID %s with metadata id %s", replicaID, id))
}

// GetReplicaIDs returns all replica IDs in MetadataStore
func (m *MetadataStore) GetReplicaIDs() []string {
	ids := make([]string, 0, len(m.lazySnapshots))
	for k := range m.lazySnapshots {
		ids = append(ids, k)
	}
	return ids
}
