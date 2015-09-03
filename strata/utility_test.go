//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

package strata

import (
	"testing"

	"github.com/facebookgo/ensure"
)

func TestMergeFileSets(t *testing.T) {
	mergedSet := mergeFileSets(
		[]File{{Name: "a"}, {Name: "b"}},
		[]File{{Name: "a"}, {Name: "c"}})
	ensure.SameElements(t, []File{{Name: "a"}, {Name: "b"}, {Name: "c"}}, mergedSet)
	mergedSet = mergeFileSets(
		[]File{{Name: "a"}, {Name: "b"}, {Name: "c"}},
		[]File{})
	ensure.SameElements(t, []File{{Name: "a"}, {Name: "b"}, {Name: "c"}}, mergedSet)
}

func TestSubtractFileSets(t *testing.T) {
	newSet := subtractFileSets(
		[]File{{Name: "a"}, {Name: "b"}, {Name: "c"}},
		[]File{{Name: "a"}, {Name: "c"}})
	ensure.SameElements(t, []File{{Name: "b"}}, newSet)
}
