//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

package s3storage

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"io/ioutil"
	"strings"

	"github.com/facebookgo/rocks-strata/strata"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
)

// S3Storage implements the strata.Storage interface using S3 as its storage backing
type S3Storage struct {
	s3     *s3.S3
	bucket *s3.Bucket
	region aws.Region
	auth   aws.Auth
	prefix string
}

func (s *S3Storage) addPrefix(path string) string {
	return s.prefix + "/" + path
}

func (s *S3Storage) removePrefix(path string) string {
	return path[len(s.prefix)+1:]
}

// NewS3Storage initializes the S3Storage with required AWS arguments
func NewS3Storage(region aws.Region, auth aws.Auth, bucketName string, prefix string, bucketACL s3.ACL) (*S3Storage, error) {
	s3obj := s3.New(auth, region)
	bucket := s3obj.Bucket(bucketName)

	// Running PutBucket too many times in parallel (such as distributed cron) can generate the error:
	// "A conflicting conditional operation is currently in progress against this resource. Please try again"
	// We should only call PutBucket when we suspect that the bucket doesn't exist. Unfortunately, the
	// current AdRoll/goamz lib doesn't implement ListBuckets, so to check that the bucket exists
	// do a List and see if we get an error before calling PutBucket.
	_, err := bucket.List("", "/", "", 1)
	// technically, there are many reasons this could fail (such as access denied, or other network error)
	// but this should sufficiently limit the number of times PutBucket is called in normal operations
	if err != nil {
		err = bucket.PutBucket(bucketACL)
		if err != nil {
			return nil, err
		}
	}
	return &S3Storage{
		s3:     s3obj,
		bucket: bucket,
		region: region,
		auth:   auth,
		prefix: prefix,
	}, nil
}

// Get returns a reader to the specified S3 path.
// The reader is a wrapper around a ChecksummingReader. This protects against network corruption.
func (s *S3Storage) Get(path string) (io.ReadCloser, error) {
	path = s.addPrefix(path)
	resp, err := s.bucket.GetResponse(path)
	if resp == nil || err != nil {
		if err.Error() == "The specified key does not exist." {
			err = strata.ErrNotFound(path)
		}
		return nil, err
	}
	etag, found := resp.Header["Etag"]
	if !found {
		return nil, errors.New("No Etag header")
	}
	if len(etag) == 0 {
		return nil, errors.New("Etag header is empty")
	}
	// Note: s3test does not require the trimming, but real S3 does
	checksum, err := hex.DecodeString(strings.TrimSuffix(strings.TrimPrefix(etag[0], "\""), "\""))
	if err != nil {
		return nil, err
	}
	return strata.NewChecksummingReader(resp.Body, checksum), nil
}

// Put places the byte slice at the given path in S3.
// Put also sends a checksum to protect against network corruption.
func (s *S3Storage) Put(path string, data []byte) error {
	checksum := md5.Sum(data)
	path = s.addPrefix(path)
	err := s.bucket.Put(path, data, "application/octet-stream", s3.Private,
		s3.Options{ContentMD5: base64.StdEncoding.EncodeToString(checksum[:])})
	return err
}

// PutReader consumes the given reader and stores it at the specified path in S3.
// A checksum is used to protect against network corruption.
func (s *S3Storage) PutReader(path string, reader io.Reader) error {
	// TODO(agf): S3 will send a checksum as a response after we do a PUT.
	// We could compute our checksum on the fly by using an ChecksummingReader,
	// and then compare the checksum to the one that S3 sends back. However,
	// goamz does not give us access to the checksum that S3 sends back, so we
	// need to load the data into memory and compute the checksum beforehand.
	// Should fix this in goamz.
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	return s.Put(path, data)
}

// Delete removes the object at the given S3 path
func (s *S3Storage) Delete(path string) error {
	path = s.addPrefix(path)
	err := s.bucket.Del(path)
	return err
}

// List returns a list of objects (up to maxSize) with the given prefix from S3
func (s *S3Storage) List(prefix string, maxSize int) ([]string, error) {
	prefix = s.addPrefix(prefix)
	pathSeparator := ""
	marker := ""

	items := make([]string, 0, 1000)
	for maxSize > 0 {
		// Don't ask for more than 1000 keys at a time. This makes
		// testing simpler because S3 will return at most 1000 keys even if you
		// ask for more, but s3test will return more than 1000 keys if you ask
		// for more. TODO(agf): Fix this behavior in s3test.
		maxReqSize := 1000
		if maxSize < 1000 {
			maxReqSize = maxSize
		}
		contents, err := s.bucket.List(prefix, pathSeparator, marker, maxReqSize)
		if err != nil {
			return nil, err
		}
		maxSize -= maxReqSize

		for _, key := range contents.Contents {
			items = append(items, s.removePrefix(key.Key))
		}
		if contents.IsTruncated {
			marker = s.addPrefix(items[len(items)-1])
		} else {
			break
		}
	}

	return items, nil
}

// Lock is not implemented
func (s *S3Storage) Lock(path string) error {
	return nil
}

// Unlock is not implemented
func (s *S3Storage) Unlock(path string) error {
	return nil
}
