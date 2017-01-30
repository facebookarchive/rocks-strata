//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

package strata

import (
	"bytes"
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"time"
)

// MockSnapshotNumFiles is the number of files in a mock snapshot
const MockSnapshotNumFiles int = 3

// MockSnapshotSize is the "total size" of the files in mock snapshot. This is
// just the sum of the size fields, which are made up.
const MockSnapshotSize int64 = 6

// MockReplica is a mocked implementation of ReplicaManager for use in tests
type MockReplica struct {
	// store mock snapshots that have been created
	snapshots []SnapshotMetadata
}

// NewMockReplica returns a mocked implementation of ReplicaManager
func NewMockReplica() *MockReplica {
	return &MockReplica{
		snapshots: []SnapshotMetadata{},
	}
}

// PrepareToRestoreSnapshot does nothing
func (*MockReplica) PrepareToRestoreSnapshot(string, string, *Snapshot) (string, error) {
	return "", nil
}

// PutReader consumes reader and does nothing else
func (m *MockReplica) PutReader(path string, reader io.Reader) error {
	_, err := ioutil.ReadAll(reader)
	return err
}

// PutSoftlink does nothing
func (m *MockReplica) PutSoftlink(string, string) error {
	return nil
}

// Delete does nothing
func (*MockReplica) Delete(string) error {
	return nil
}

// List does nothing
func (*MockReplica) List(string, int) ([]string, error) {
	return nil, nil
}

// Same returns false, nil
func (*MockReplica) Same(string, File) (bool, error) {
	return false, nil
}

// CreateSnapshot generates a MockSnapshot with a static set of files
func (m *MockReplica) CreateSnapshot(replicaID, snapshotID string) (*Snapshot, error) {
	s := newMockSnapshotFromMetadata(
		SnapshotMetadata{
			ReplicaID: replicaID,
			ID:        snapshotID,
			Path:      "foo",
			Files: []File{
				{Name: "a", Size: 1},
				{Name: "b", Size: 2},
				{Name: "c", Size: 3},
			},
		},
	)

	m.snapshots = append(m.snapshots, *s.Metadata)

	return s, nil
}

// DeleteSnapshot removes the specified snapshot from the local metadata
func (m *MockReplica) DeleteSnapshot(lz LazySnapshotMetadata) error {
	sd, err := lz.Get()
	if err != nil {
		return err
	}
	for i, ss := range m.snapshots {
		if ss.ID == sd.ID && ss.ReplicaID == sd.ReplicaID {
			m.snapshots = append(m.snapshots[:i], m.snapshots[i+1:]...)
		}
	}
	return nil
}

// GetSnapshot returns a Snapshot given the specified metadata
func (m *MockReplica) GetSnapshot(s SnapshotMetadata) (*Snapshot, error) {
	return newMockSnapshotFromMetadata(s), nil
}

// MaxBackgroundCopies returns 24
func (m *MockReplica) MaxBackgroundCopies() int {
	return 24
}

// MockStorage implements PersistenceManager for mocking
type MockStorage struct {
	objects      map[string][]byte
	objectsMutex sync.RWMutex
	// saveDelay will only apply to the first object saved
	saveDelay time.Duration
}

// NewMockStorage instantiates a MockPersistenceManager
func NewMockStorage(saveDelay time.Duration) *MockStorage {
	return &MockStorage{
		objects:      map[string][]byte{},
		objectsMutex: sync.RWMutex{},
		saveDelay:    saveDelay,
	}
}

// readCloser wraps an io.Reader, adding a Close() method
type readCloser struct {
	R    io.Reader
	Size int64
}

func (r readCloser) Close() error {
	if closer, ok := r.R.(io.ReadCloser); ok {
		return closer.Close()
	}
	return nil
}

func (r readCloser) Read(bytes []byte) (int, error) {
	return r.R.Read(bytes)
}

// Get returns the data stored at key
func (m *MockStorage) Get(key string) (io.ReadCloser, error) {
	m.objectsMutex.RLock()
	defer m.objectsMutex.RUnlock()
	if data, ok := m.objects[key]; ok {
		return readCloser{R: bytes.NewReader(data)}, nil
	}
	return nil, ErrNotFound(key)
}

// Put places the byte slice in the mocked store
func (m *MockStorage) Put(key string, data []byte) error {
	if m.saveDelay != 0 {
		time.Sleep(m.saveDelay)
		m.saveDelay = 0
	}
	m.objectsMutex.Lock()
	defer m.objectsMutex.Unlock()
	m.objects[key] = data
	return nil
}

// PutReader consumes the reader and places it in the mocked store
func (m *MockStorage) PutReader(key string, reader io.Reader) error {
	if m.saveDelay != 0 {
		time.Sleep(m.saveDelay)
		m.saveDelay = 0
	}
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	m.objectsMutex.Lock()
	defer m.objectsMutex.Unlock()
	m.objects[key] = data
	return nil
}

// Delete an item from the mock store
func (m *MockStorage) Delete(key string) error {
	m.objectsMutex.Lock()
	defer m.objectsMutex.Unlock()
	delete(m.objects, key)
	return nil
}

// List returns items matching the given prefix from the mocked store, up to maxSize items
func (m *MockStorage) List(prefix string, maxSize int) ([]string, error) {
	var list []string
	size := 0
	m.objectsMutex.RLock()
	defer m.objectsMutex.RUnlock()
	for k := range m.objects {
		if size == maxSize {
			break
		}
		if strings.HasPrefix(k, prefix) {
			list = append(list, k)
			size++
		}
	}
	return list, nil
}

// Corrupt corrupts the data stored at |key|
func (m *MockStorage) Corrupt(key string) error {
	m.objectsMutex.Lock()
	defer m.objectsMutex.Unlock()
	if _, found := m.objects[key]; !found {
		return ErrNotFound(key)
	}
	m.objects[key] = []byte("No one expects the Spanish Inquisition!")
	return nil
}

// Lock is not implemented
func (m *MockStorage) Lock(key string) error {
	return nil
}

// Unlock is not implemented
func (m *MockStorage) Unlock(key string) error {
	return nil
}

func newMockSnapshotFromMetadata(metadata SnapshotMetadata) *Snapshot {
	s := NewSnapshot(&metadata)

	for i := range metadata.Files {
		name := metadata.Files[i].Name
		s.AddReaderFunc(name, func() (io.ReadCloser, error) {
			return readCloser{R: strings.NewReader(name)}, nil
		})
	}
	return s
}
