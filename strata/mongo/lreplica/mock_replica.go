//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

package lreplica

import (
	"github.com/facebookgo/mgotest"

	"gopkg.in/mgo.v2"
)

// MockLocalReplica mocks LocalReplica
type MockLocalReplica struct {
	LocalReplica
}

type mockLocalSessionGetter struct {
	mongo *mgotest.Server
}

func (mlsg *mockLocalSessionGetter) get(string, string, string) (*mgo.Session, error) {
	return mlsg.mongo.Session(), nil
}

// NewMockLocalReplica constructs a MockLocalReplica
func NewMockLocalReplica(mongo *mgotest.Server, maxBackgroundCopies int) *MockLocalReplica {
	nmlr := &MockLocalReplica{}
	nmlr.sessionGetter = &mockLocalSessionGetter{mongo}
	nmlr.maxBackgroundCopies = maxBackgroundCopies
	return nmlr
}
