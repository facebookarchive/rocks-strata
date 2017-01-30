//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

package main

import (
	"os"
	"runtime"
	"strings"

	"github.com/facebookgo/rocks-strata/strata"
	"github.com/facebookgo/rocks-strata/strata/cmd/mongo/lreplica_drivers/lrazureblobdriver"
	"github.com/facebookgo/rocks-strata/strata/cmd/mongo/lreplica_drivers/lrldriver"
	"github.com/facebookgo/rocks-strata/strata/cmd/mongo/lreplica_drivers/lrminiodriver"
	"github.com/facebookgo/rocks-strata/strata/cmd/mongo/lreplica_drivers/lrs3driver"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Use a custom remote storage if set in the environment
	// Use S3 as the default remote storage
	switch strings.ToLower(os.Getenv("REMOTE_STORAGE")) {
	case "minio":
		strata.RunCLI(lrminiodriver.DriverFactory{Ops: &lrminiodriver.Options{}})
	case "local":
		strata.RunCLI(lrldriver.DriverFactory{Ops: &lrldriver.Options{}})
	case "azureblob":
		strata.RunCLI(lrazureblobdriver.DriverFactory{Ops: &lrazureblobdriver.Options{}})
	default:
		strata.RunCLI(lrs3driver.DriverFactory{Ops: &lrs3driver.Options{}})
	}
}
