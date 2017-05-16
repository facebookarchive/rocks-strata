package azureblobstorage

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/facebookgo/ensure"
)

type AzureMock struct {
	azureEmulator *AzureBlobStorage
}

func (azureMock *AzureMock) Start(t *testing.T) {
	azureMock.azureEmulator.blobContainer.DeleteIfExists(&storage.DeleteContainerOptions{})
	azureMock.azureEmulator.blobContainer.Create(&storage.CreateContainerOptions{Access: storage.ContainerAccessTypePrivate})
}

func (azureMock *AzureMock) Stop() {
}

func NewMockAzure(t *testing.T) *AzureMock {
	azureEmulatorClient, err := storage.NewEmulatorClient()
	ensure.Nil(t, err)

	blobService := azureEmulatorClient.GetBlobService()
	containerName := fmt.Sprintf("testcontainer%d", rand.Intn(100000))
	container := blobService.GetContainerReference(containerName)

	blobMock := AzureBlobStorage{
		blobStorageClient: &blobService,
		client:            &azureEmulatorClient,
		blobContainer:	   container,
		containerName:     containerName, //Randomizing since the container in the emulator is "finally" consistent
		prefix:            "",
	}

	m := AzureMock{
		azureEmulator: &blobMock,
	}

	m.Start(t)
	return &m
}
