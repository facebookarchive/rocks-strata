/*
Package main provides a driver for the strata framework. It uses lreplica and s3storage.

Main should be run with start-stop-daemon or a similar tool to prevent concurrent executions, which may not be safe.
*/
package main

import (
	"github.com/facebookgo/rocks-strata/strata"
	"github.com/facebookgo/rocks-strata/strata/cmd/mongo/lreplica_s3storage_driver/lrs3driver"
)

func main() {
	strata.RunCLI(lrs3driver.DriverFactory{Ops: &lrs3driver.Options{}})
}
