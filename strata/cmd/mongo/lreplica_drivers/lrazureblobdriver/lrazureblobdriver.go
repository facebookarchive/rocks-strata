package lrazureblobdriver

import (
	"errors"
	"os"
	"strconv"

	"github.com/facebookgo/rocks-strata/strata"
	"github.com/facebookgo/rocks-strata/strata/azureblobstorage"
	"github.com/facebookgo/rocks-strata/strata/cmd/mongo/lreplica_drivers/lrs3driver"
	"github.com/facebookgo/rocks-strata/strata/mongo/lreplica"
)

// AzureBlobOptions define basic options of your azure blob storage
type AzureBlobOptions struct {
	Container  string `short:"C" long:"container" description:"Azure Blob Storage container name" required:"true"`
	BlobPrefix string `short:"p" long:"blob-prefix" description:"Prefix used when storing and retrieving files. Optional" optional:"true"`
}

// Options define the common options needed by this strata command
type Options struct {
	AzureBlobOptions AzureBlobOptions          `group:"Azure Blob Options"`
	Replica          lrs3driver.ReplicaOptions `group:"Replica Options"`
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

	accountName := os.Getenv("AZURE_ACCOUNT_NAME")
	accountSecret := os.Getenv("AZURE_ACCOUNT_SECRET")
	if accountName == "" || accountSecret == "" {
		return nil, errors.New("Environment variables AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_SECRET must be set")
	}

	azureBloblStorage, err := azureblobstorage.NewAzureBlobStorage(
		accountName,
		accountSecret,
		options.AzureBlobOptions.Container,
		options.AzureBlobOptions.BlobPrefix)
	if err != nil {
		return nil, err
	}

	replica, err := lreplica.NewLocalReplica(
		options.Replica.MaxBackgroundCopies,
		options.Replica.DatabaseHostname,
		strconv.Itoa(options.Replica.Port),
		options.Replica.Username,
		options.Replica.Password,
		options.Replica.SslAllowInvalidCertificates,
	)
	if err != nil {
		return nil, err
	}
	manager, err := strata.NewSnapshotManager(replica, azureBloblStorage)
	if err != nil {
		return nil, err
	}
	return &strata.Driver{Manager: manager}, err
}
