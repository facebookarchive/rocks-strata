//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

package lrldriver

import (
	"strconv"

	"github.com/facebookgo/rocks-strata/strata"
	"github.com/facebookgo/rocks-strata/strata/cmd/mongo/lreplica_drivers/lrs3driver"
	"github.com/facebookgo/rocks-strata/strata/mongo/lreplica"
	"github.com/facebookgo/rocks-strata/strata/lstorage"
)

// FsOptions are common to all commands
type FsOptions struct {
	Mountpoint   string `short:"m" long:"mpoint" description:"Mount point name, such as \"~/sbackup\"" default:"~/sbackup"`
}

// Options define the common options needed by this strata command
type Options struct {
	LocalStorage   FsOptions          `group:"Storage Options"`
	Replica lrs3driver.ReplicaOptions `group:"Replica Options"`
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

	fsstore, err := lstorage.NewLStorage(options.LocalStorage.Mountpoint)
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
	manager, err := strata.NewSnapshotManager(replica, fsstore)
	if err != nil {
		return nil, err
	}
	return &strata.Driver{Manager: manager}, err
}
