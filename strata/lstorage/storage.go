//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

package lstorage

import (
	"io"
	"io/ioutil"
	"os"
	"path"
)

// LStorage implements the strata.Storage interface using a locally mounted file system as its storage backing
type LStorage struct {
	mountPoint string
}

func (ls *LStorage) addPrefix(fpath string) string {
	return ls.mountPoint + "/" + fpath
}

// NewLStorage initializes the LStorage
func NewLStorage(mountPoint string) (*LStorage, error) {
	return &LStorage{mountPoint: mountPoint}, nil
}

// Get returns a reader to the specified path
func (ls *LStorage) Get(fpath string) (io.ReadCloser, error) {
	return os.Open(ls.addPrefix(fpath))
}

// Put places the byte slice at the given path.
func (ls *LStorage) Put(fpath string, data []byte) error {
	// Write to temporary file, then do atomic rename
	fpath = ls.addPrefix(fpath)
	tmpName := fpath + ".tmp"
	if err := os.MkdirAll(path.Dir(tmpName), 0777); err != nil {
		return err
	}
	if err := ioutil.WriteFile(tmpName, data, 0777); err != nil {
		return err
	}
	return os.Rename(tmpName, fpath)
}

// PutReader consumes the given reader and stores it at the specified path.
func (ls *LStorage) PutReader(fpath string, reader io.Reader) error {
	fpath = ls.addPrefix(fpath)
	tmpName := fpath + ".tmp"
	if err := os.MkdirAll(path.Dir(tmpName), 0777); err != nil {
		return err
	}
	writer, err := os.Create(tmpName)
	if err != nil {
		return err
	}
	defer writer.Close()
	if _, err := io.Copy(writer, reader); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, fpath)
}

// Delete removes the object at the given path
func (ls *LStorage) Delete(fpath string) error {
	return os.Remove(ls.addPrefix(fpath))
}

// List returns a list of files (up to maxSize) in the given directory.
// Recursively search for files. If the directory does not exist, return an empty list.
func (ls *LStorage) List(dir string, maxSize int) ([]string, error) {
	var list []string
	entries, err := ioutil.ReadDir(ls.addPrefix(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return list, nil
		}
		return nil, err
	}
	for entryNum, entry := range entries {
		if entryNum == maxSize {
			break
		}
		if len(list) == maxSize {
			break
		}

		entryFullName := path.Clean(path.Join(dir, entry.Name()))
		if !entry.IsDir() {
			list = append(list, entryFullName)
		} else {
			subdirList, err := ls.List(entryFullName, maxSize-len(list))
			if err != nil {
				return list, err
			}
			list = append(list, subdirList...)
		}
	}
	return list, nil
}

// Lock is not implemented
func (ls *LStorage) Lock(fpath string) error {
	return nil
}

// Unlock is not implemented
func (ls *LStorage) Unlock(fpath string) error {
	return nil
}
