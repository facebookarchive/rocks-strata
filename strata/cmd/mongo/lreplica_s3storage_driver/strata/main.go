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
	"github.com/facebookgo/rocks-strata/strata/cmd/mongo/lreplica_s3storage_driver/lrminiodriver"
	"github.com/facebookgo/rocks-strata/strata/cmd/mongo/lreplica_s3storage_driver/lrs3driver"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Use a custom storage engine if set in the environment
	// Use S3 as the default storage engine
	switch strings.ToLower(os.Getenv("STORAGE_ENGINE")) {
	case "minio":
		strata.RunCLI(lrminiodriver.DriverFactory{Ops: &lrminiodriver.Options{}})
	default:
		strata.RunCLI(lrs3driver.DriverFactory{Ops: &lrs3driver.Options{}})
	}
}
