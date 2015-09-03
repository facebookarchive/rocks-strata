//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

package strata

import (
	"fmt"
	"log"
	"time"
)

// Log prints logging info
func Log(msg string) {
	log.Println(msg)
}

func mergeFileSets(setA []File, setB []File) []File {
	setMap := map[string]File{}

	for _, item := range setA {
		setMap[item.Name] = item
	}
	for _, item := range setB {
		setMap[item.Name] = item
	}
	newSet := make([]File, 0, len(setMap))
	for _, value := range setMap {
		newSet = append(newSet, value)
	}

	return newSet
}

func subtractFileSets(setA []File, setB []File) []File {
	setMap := map[string]File{}

	for _, item := range setA {
		setMap[item.Name] = item
	}
	for _, item := range setB {
		delete(setMap, item.Name)
	}

	newSet := make([]File, 0, len(setMap))
	for _, value := range setMap {
		newSet = append(newSet, value)
	}

	return newSet
}

// Try provides strata's retry strategy.
// It calls the given function up to five times or until the given function
// returns nil, whichever comes first.
func Try(f func() error, header string) error {
	if header != "" {
		Log(header)
	}
	err := f()
	for tryNum := 2; tryNum <= 5 && err != nil; tryNum++ {
		Log(fmt.Sprintf("%s", err))
		time.Sleep(200 * time.Millisecond)
		Log(fmt.Sprintf("(try number %d) "+header, tryNum))
		err = f()
	}
	return err
}

// ToMB converts bytes to mebibytes
func ToMB(bytes int64) float64 {
	return float64(bytes) / 1048576.0
}

// ToGB converts bytes to Gibibytes
func ToGB(bytes int64) float64 {
	return float64(bytes) / 1073741824.0
}

// ErrNotFound should be returned by Replica or Storage if a given key is not
// found, or by Storage if no object exists at the given path
// This is to help differentiate unknown keys from other errors
type ErrNotFound string

// Error returns the error string for ErrNotFound
func (e ErrNotFound) Error() string {
	return fmt.Sprintf("not found: %s", string(e))
}

// IsErrNotFound returns true if the error is an ErrNotFound
func IsErrNotFound(e error) bool {
	_, ok := e.(ErrNotFound)
	return ok
}

// ErrNoSnapshotMetadata may be returned if there is no snapshot metadata found
// for a replica ID. It is intended for use with drivers.
// Suggestion: The error string should be the replica ID with an optional message appended
type ErrNoSnapshotMetadata string

// Error returns the error string for ErrNoSnapshotMetadata
func (e ErrNoSnapshotMetadata) Error() string {
	return fmt.Sprintf("no snapshot metadata found for replicaID %s", string(e))
}

// IsErrNoSnapshotMetadata returns true if the error is an ErrNoSnapshotMetadata
func IsErrNoSnapshotMetadata(e error) bool {
	_, ok := e.(ErrNoSnapshotMetadata)
	return ok
}
