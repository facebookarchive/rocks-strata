//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

package main

import (
	"github.com/facebookgo/rocks-strata/strata/cmd/mongo/lreplica_drivers/lrs3driver"
	"github.com/facebookgo/rocks-strata/strata/mongo"
)

func main() {
	mongoq.RunCLI(lrs3driver.DriverFactory{Ops: &lrs3driver.Options{}})
}
