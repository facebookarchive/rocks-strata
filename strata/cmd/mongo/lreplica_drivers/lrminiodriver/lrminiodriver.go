package lrminiodriver

import (
	"errors"
	"os"
	"strconv"

	"github.com/facebookgo/rocks-strata/strata"
	"github.com/facebookgo/rocks-strata/strata/cmd/mongo/lreplica_drivers/lrs3driver"
	"github.com/facebookgo/rocks-strata/strata/miniostorage"
	"github.com/facebookgo/rocks-strata/strata/mongo/lreplica"
)

// MinioOptions are common to all commands
type MinioOptions struct {
	Region       string `short:"R" long:"region" description:"Minio region name, such as \"us-east-1\"" default:"us-east-1"`
	BucketName   string `short:"b" long:"bucket" description:"Name of Minio bucket used to store the backups" required:"true"`
	BucketPrefix string `short:"p" long:"bucket-prefix" description:"Prefix used when storing and retrieving files. Optional" optional:"true"`
}

// Options define the common options needed by this strata command
type Options struct {
	Minio   MinioOptions              `group:"Minio Options"`
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

	endPoint := os.Getenv("MINIO_ENDPOINT")
	secure := os.Getenv("MINIO_SECURE")
	accessKey := os.Getenv("MINIO_ACCESS_KEY_ID")
	secretKey := os.Getenv("MINIO_SECRET_ACCESS_KEY")
	if endPoint == "" || accessKey == "" || secretKey == "" {
		return nil, errors.New("Environment variables MINIO_ENDPOINT, MINIO_ACCESS_KEY_ID and MINIO_SECRET_ACCESS_KEY must be set")
	}

	if secure == "" {
		secure = "true"
	}

	secureBool, err := strconv.ParseBool(secure)
	if err != nil {
		return nil, errors.New("Valid values for environment variable MINIO_SECURE are 1, t, T, TRUE, true, True, 0, f, F, FALSE, false, False")
	}

	minio, err := miniostorage.NewMinioStorage(
		endPoint,
		accessKey, secretKey,
		options.Minio.BucketName,
		options.Minio.BucketPrefix,
		options.Minio.Region,
		secureBool)

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

	manager, err := strata.NewSnapshotManager(replica, minio)

	if err != nil {
		return nil, err
	}

	return &strata.Driver{Manager: manager}, err
}
