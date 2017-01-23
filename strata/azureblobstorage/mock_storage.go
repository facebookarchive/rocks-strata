package azureblobstorage

import (
	"testing"
	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/facebookgo/ensure"
	"math/rand"
	"fmt"
)


type AzureMock struct {
	azureEmulator *AzureBlobStorage
}

func (azureMock *AzureMock) Start(t *testing.T) {
	azureMock.azureEmulator.blobStorageClient.DeleteContainerIfExists(azureMock.azureEmulator.containerName)
	azureMock.azureEmulator.blobStorageClient.CreateContainer(azureMock.azureEmulator.containerName, storage.ContainerAccessTypePrivate)
}

func (azureMock *AzureMock) Stop() {
}

func NewMockAzure(t *testing.T) *AzureMock {
	azureEmulatorClient, err := storage.NewEmulatorClient()
	ensure.Nil(t, err)

	blobService := azureEmulatorClient.GetBlobService()

	blobMock := AzureBlobStorage {
		blobStorageClient: &blobService,
		client: &azureEmulatorClient,
		containerName: fmt.Sprintf("testcontainer%d", rand.Intn(100000)), //Randomizing since the container in the emulator is "finally" consistent
		prefix: "",
	}

	m := AzureMock {
		azureEmulator: &blobMock,
	}

	m.Start(t)
	return &m
}