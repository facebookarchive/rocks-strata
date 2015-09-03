package s3storage

import (
	"testing"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
	"github.com/AdRoll/goamz/s3/s3test"
	"github.com/facebookgo/ensure"
)

// MockS3 mocks S3Storage
type MockS3 struct {
	auth   aws.Auth
	region aws.Region
	srv    *s3test.Server
	config *s3test.Config
}

// Start starts server used for MockS3
func (s *MockS3) Start(t *testing.T) {
	srv, err := s3test.NewServer(s.config)
	ensure.Nil(t, err)
	ensure.NotNil(t, srv)

	s.srv = srv
	s.region = aws.Region{
		Name:                 "faux-region-1",
		S3Endpoint:           srv.URL(),
		S3LocationConstraint: true, // s3test server requires a LocationConstraint
	}
}

// Stop stops server used for MockS3
func (s *MockS3) Stop() {
	s.srv.Quit()
}

// NewMockS3 constructs a MockS3
func NewMockS3(t *testing.T) *MockS3 {
	m := MockS3{}
	m.Start(t)
	return &m
}

// NewStorageWithMockS3 constructs an S3Storage that uses MockS3
func NewStorageWithMockS3(s *MockS3) (*S3Storage, error) {
	return NewS3Storage(s.region, s.auth, "testbucket", "test", s3.Private)
}
