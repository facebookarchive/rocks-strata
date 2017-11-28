package miniostorage

import (
	"bytes"
	"io"
	"io/ioutil"

	minio "github.com/minio/minio-go"
)

// MinioStorage implements the strata.Storage interface using minio as its storage backing
type MinioStorage struct {
	minio  *minio.Client
	region string
	bucket string
	prefix string
}

func (m *MinioStorage) addPrefix(name string) string {
	return m.prefix + "/" + name
}

func (m *MinioStorage) removePrefix(name string) string {
	return name[len(m.prefix)+1:]
}

// NewMinioStorage initializes the MinioStorage with Minio arguments
func NewMinioStorage(endPoint, accessKeyID, secretAccessKey, bucket, prefix, region string, secure bool) (*MinioStorage, error) {

	mc, err := minio.New(endPoint, accessKeyID, secretAccessKey, secure)

	if err != nil {
		return nil, err
	}

	if region == "" {
		region = "us-east-1"
	}

	exists, err := mc.BucketExists(bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		if err = mc.MakeBucket(bucket, region); err != nil {
			// Since the bucket couldn't be created, there's nothing furter to do
			return nil, err
		}
	}

	return &MinioStorage{
		minio:  mc,
		region: region,
		bucket: bucket,
		prefix: prefix,
	}, nil
}

// Get returns a reader to the specified minio object.
func (m *MinioStorage) Get(name string) (io.ReadCloser, error) {

	path := m.addPrefix(name)
	obj, err := m.minio.GetObject(m.bucket, path, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	return obj, nil
}

// Put places the byte slice with the given objectName in the minio bucket
func (m *MinioStorage) Put(name string, data []byte) error {

	path := m.addPrefix(name)
	_, err := m.minio.PutObject(m.bucket, path, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: "application/octet-stream"})

	return err
}

// PutReader consumes the given reader and stores it with the specified name in the minio bucket.
func (m *MinioStorage) PutReader(name string, reader io.Reader) error {

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	return m.Put(name, data)
}

// Delete removes the object from the minio bucket
func (m *MinioStorage) Delete(name string) error {
	path := m.addPrefix(name)
	return m.minio.RemoveObject(m.bucket, path)
}

// List returns a list of objects
func (m *MinioStorage) List(prefix string, maxSize int) ([]string, error) {

	doneCh := make(chan struct{})
	path := m.addPrefix(prefix)

	objCh := m.minio.ListObjectsV2(m.bucket, path, true, doneCh)

	items := []string{}

	for obj := range objCh {
		items = append(items, m.removePrefix(obj.Key))
	}

	return items, nil

}

// Lock is not implemented
func (m *MinioStorage) Lock(path string) error {
	return nil
}

// Unlock is not implemented
func (m *MinioStorage) Unlock(path string) error {
	return nil
}
