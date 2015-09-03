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

func (mlsg *mockLocalSessionGetter) get(string) (*mgo.Session, error) {
	return mlsg.mongo.Session(), nil
}

// NewMockLocalReplica constructs a MockLocalReplica
func NewMockLocalReplica(mongo *mgotest.Server, maxBackgroundCopies int) *MockLocalReplica {
	nmlr := &MockLocalReplica{}
	nmlr.sessionGetter = &mockLocalSessionGetter{mongo}
	nmlr.maxBackgroundCopies = maxBackgroundCopies
	return nmlr
}
