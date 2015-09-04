//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

package lstorage

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/facebookgo/ensure"
	"github.com/facebookgo/rocks-strata/strata"
)

func TestLStorage(t *testing.T) {
	t.Parallel()
	testdir, err := ioutil.TempDir("", "rocks-strata-Storage-test_")
	ensure.Nil(t, err)
	ls, err := NewLStorage(testdir)
	ensure.Nil(t, err)
	defer os.RemoveAll(testdir)
	strata.HelpTestStorage(t, ls)
}

func TestLStorageManyFiles(t *testing.T) {
	t.Parallel()
	testdir, err := ioutil.TempDir("", "rocks-strata-LStorage-test_")
	ensure.Nil(t, err)
	ls, err := NewLStorage(testdir)
	ensure.Nil(t, err)
	defer os.RemoveAll(testdir)
	strata.HelpTestStorageManyFiles(t, ls)
}
