package azureblobstorage

import (
	"encoding/base64"
	"fmt"
	"io"
	"math"

	"io/ioutil"

	"github.com/Azure/azure-sdk-for-go/storage"
)

const MAX_BLOCK_SIZE = 1024 * 1024 * 4 // 4MB

type AzureBlobStorage struct {
	client            *storage.Client
	blobStorageClient *storage.BlobStorageClient
	blobContainer     *storage.Container
	containerName     string
	prefix            string
}

func (azureBlob *AzureBlobStorage) addPrefix(path string) string {
	if len(azureBlob.prefix) == 0 {
		return path // Do not append "/" in the start of the path
	}

	return azureBlob.prefix + "/" + path
}

func (azureBlob *AzureBlobStorage) removePrefix(path string) string {
	if len(azureBlob.prefix) == 0 {
		return path // Do not remove the first chars if we didn't append any.
	}

	return path[len(azureBlob.prefix)+1:]
}

func NewAzureBlobStorage(accountName string, accountKey string, containerName string, prefix string) (*AzureBlobStorage, error) {
	azureClient, err := storage.NewBasicClient(accountName, accountKey)
	if err != nil {
		return nil, err
	}

	blobClient := azureClient.GetBlobService()

	container := blobClient.GetContainerReference(containerName)
	containerExists, err := container.Exists()

	if err != nil {
		return nil, err
	}

	if !containerExists {
		err = container.Create(&storage.CreateContainerOptions {Access: storage.ContainerAccessTypePrivate})
		if err != nil {
			return nil, err
		}
	}

	return &AzureBlobStorage{
		client:            &azureClient,
		blobStorageClient: &blobClient,
		blobContainer:	   container,
		containerName:     containerName,
		prefix:            prefix,
	}, nil
}

func (azureBlob *AzureBlobStorage) Get(path string) (io.ReadCloser, error) {
	return azureBlob.blobContainer.GetBlobReference(path).Get(&storage.GetBlobOptions{})
}

func (azureBlob *AzureBlobStorage) Put(path string, data []byte) error {
	data_length := len(data)
	block_count := int(math.Ceil(float64(data_length) / MAX_BLOCK_SIZE))

	block_ids := make([]storage.Block, block_count, block_count)

	for i := 0; i < block_count; i++ {
		block_id := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%10d", i)))

		min_data_range := i * MAX_BLOCK_SIZE
		max_data_range := int(math.Min(float64((i+1)*MAX_BLOCK_SIZE), float64(data_length)))

		data_slice := data[min_data_range:max_data_range]

		err := azureBlob.blobContainer.GetBlobReference(path).PutBlock(block_id, data_slice, &storage.PutBlockOptions{})
		if err != nil {
			return err
		}

		block_ids[i].ID = block_id
		block_ids[i].Status = storage.BlockStatusLatest
	}

	err := azureBlob.blobContainer.GetBlobReference(path).PutBlockList(block_ids, &storage.PutBlockListOptions{})

	return err
}

func (azureBlob *AzureBlobStorage) PutReader(path string, reader io.Reader) error {
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	return azureBlob.Put(path, data)
}

func (azureBlob *AzureBlobStorage) Delete(path string) error {
	path = azureBlob.addPrefix(path)
	err := azureBlob.blobContainer.GetBlobReference(path).Delete(&storage.DeleteBlobOptions{})
	return err
}

func (azureBlob *AzureBlobStorage) List(prefix string, maxSize int) ([]string, error) {
	prefix = azureBlob.addPrefix(prefix)
	pathSeparator := ""
	marker := ""
	remaining_size := maxSize

	items := make([]string, 0, 1000)
	for remaining_size > 0 {
		contents, err := azureBlob.blobContainer.ListBlobs(storage.ListBlobsParameters{Prefix: prefix, Delimiter: pathSeparator, Marker: marker, MaxResults: uint(remaining_size)})

		if err != nil {
			return nil, err
		}

		remaining_size -= len(contents.Blobs)

		for _, key := range contents.Blobs {
			items = append(items, azureBlob.removePrefix(key.Name))
		}

		if len(contents.NextMarker) == 0 { // We're done
			break
		}

		marker = contents.NextMarker
	}

	return items, nil

}

// Lock is not implemented
func (azureBlob *AzureBlobStorage) Lock(path string) error {
	return nil
}

// Unlock is not implemented
func (azureBlob *AzureBlobStorage) Unlock(path string) error {
	return nil
}
