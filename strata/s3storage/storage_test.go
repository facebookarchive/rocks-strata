//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

package s3storage

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestS3Storage(t *testing.T) {
	t.Parallel()
	s3 := NewMockS3(t)
	defer s3.Stop()

	s, err := NewStorageWithMockS3(s3)
	ensure.Nil(t, err)

	data1 := []byte("abcdef")
	data2 := strings.NewReader("xyz123")

	err = s.Put("a/data1", data1)
	ensure.Nil(t, err)
	err = s.PutReader("b/data2", data2)
	ensure.Nil(t, err)

	items, err := s.List("a", 1000)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, len(items), 1)
	ensure.SameElements(t, items, []string{"a/data1"})
	items, err = s.List("b", 1000)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, len(items), 1)
	ensure.SameElements(t, items, []string{"b/data2"})

	data1Reader, err := s.Get("a/data1")
	ensure.Nil(t, err)
	data2Reader, err := s.Get("b/data2")
	ensure.Nil(t, err)

	content, err := ioutil.ReadAll(data1Reader)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, content, []byte("abcdef"))

	content, err = ioutil.ReadAll(data2Reader)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, content, []byte("xyz123"))

	err = s.Delete("a/data1")
	ensure.Nil(t, err)
	err = s.Delete("b/data2")
	ensure.Nil(t, err)

	items, err = s.List("", 1000)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, len(items), 0)
}

func TestS3StorageManyFiles(t *testing.T) {
	t.Parallel()
	s3 := NewMockS3(t)
	defer s3.Stop()

	s, err := NewStorageWithMockS3(s3)
	ensure.Nil(t, err)

	data := []byte("abcdef")
	for i := 0; i < 15000; i++ {
		err = s.Put(fmt.Sprintf("a/data%d", i), data)
		ensure.Nil(t, err)
	}

	items, err := s.List("a", 2500000)
	ensure.DeepEqual(t, len(items), 15000)

	items, err = s.List("a", 14123)
	ensure.DeepEqual(t, len(items), 14123)
}
