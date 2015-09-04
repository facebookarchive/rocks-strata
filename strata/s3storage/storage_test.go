//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

package s3storage

import (
	"testing"

	"github.com/facebookgo/ensure"
	"github.com/facebookgo/rocks-strata/strata"
)

func TestS3Storage(t *testing.T) {
	t.Parallel()
	s3 := NewMockS3(t)
	defer s3.Stop()

	s, err := NewStorageWithMockS3(s3)
	ensure.Nil(t, err)

	strata.HelpTestStorage(t, s)
}

func TestS3StorageManyFiles(t *testing.T) {
	t.Parallel()
	s3 := NewMockS3(t)
	defer s3.Stop()

	s, err := NewStorageWithMockS3(s3)
	ensure.Nil(t, err)

	strata.HelpTestStorageManyFiles(t, s)

}
