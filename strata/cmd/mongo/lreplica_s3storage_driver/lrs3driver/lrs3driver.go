//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

package lrs3driver

import (
	"errors"
	"os"
	"strconv"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
	"github.com/facebookgo/rocks-strata/strata"
	"github.com/facebookgo/rocks-strata/strata/mongo/lreplica"
	"github.com/facebookgo/rocks-strata/strata/s3storage"
)

// AWSOptions are common to all commands
type AWSOptions struct {
	Region       string `short:"R" long:"region" description:"AWS region name, such as \"us-east-1\"" default:"us-east-1"`
	BucketName   string `short:"b" long:"bucket" description:"Name of S3 bucket used to store the backups" required:"true"`
	BucketPrefix string `short:"p" long:"bucket-prefix" description:"Prefix used when storing and retrieving files. Optional" optional:"true"`
	BucketACL    string `short:"a" long:"bucket-acl" description:"ACL is one of private, public-read, public-read-write, authenticated-read, bucket-owner-read, or bucket-owner-full-control" default:"private"`
}

// ReplicaOptions are used for commands like backup and restore
type ReplicaOptions struct {
	MaxBackgroundCopies int    `long:"max-background-copies" default:"16" description:"Backup and restore actions will use up to this many goroutines to copy files"`
	Port                int    `long:"port" default:"27017" description:"Backup should look for a mongod instance that is listening on this port"`
	Username            string `long:"username" description:"If auth is configured, specify the username with admin privileges here"`
	Password            string `long:"password" description:"Password for the specified user."`
}

// Options define the common options needed by this strata command
type Options struct {
	AWS     AWSOptions     `group:"AWS Options"`
	Replica ReplicaOptions `group:"Replica Options"`
}

// DriverFactory implements strata.DriverFactory
type DriverFactory struct {
	Ops *Options
}

// GetOptions returns the factory's Options
func (factory DriverFactory) GetOptions() interface{} {
	return factory.Ops
}

// Driver uses the DriverFactory's Options to construct a strata.Driver
func (factory DriverFactory) Driver() (*strata.Driver, error) {
	options := factory.GetOptions().(*Options)

	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if accessKey == "" || secretKey == "" {
		return nil, errors.New("Environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set")
	}

	s3storage, err := s3storage.NewS3Storage(
		aws.Regions[options.AWS.Region],
		aws.Auth{AccessKey: accessKey, SecretKey: secretKey},
		options.AWS.BucketName,
		options.AWS.BucketPrefix,
		s3.ACL(options.AWS.BucketACL))
	if err != nil {
		return nil, err
	}
	replica, err := lreplica.NewLocalReplica(
		options.Replica.MaxBackgroundCopies,
		strconv.Itoa(options.Replica.Port),
		options.Replica.Username,
		options.Replica.Password,
	)
	if err != nil {
		return nil, err
	}
	manager, err := strata.NewSnapshotManager(replica, s3storage)
	if err != nil {
		return nil, err
	}
	return &strata.Driver{Manager: manager}, err
}
