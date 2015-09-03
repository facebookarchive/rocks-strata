package main

import (
	"github.com/facebookgo/rocks-strata/strata/cmd/mongo/lreplica_s3storage_driver/lrs3driver"
	"github.com/facebookgo/rocks-strata/strata/mongo"
)

func main() {
	mongoq.RunCLI(lrs3driver.DriverFactory{Ops: &lrs3driver.Options{}})
}
