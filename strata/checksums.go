package strata

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"math/rand"
)

// ReaderReaderAt has the methods of an io.Reader and an io.ReaderAt
type ReaderReaderAt interface {
	io.Reader
	io.ReaderAt
}

// PartialChecksum computes a checksum based on three 8KB chunks from the
// beginning, middle, and end of the file
func PartialChecksum(reader ReaderReaderAt, filesize int64) ([]byte, error) {
	// Checksum based on 8KB chunks
	var chunksize int64 = 8192

	digest := md5.New()
	buf := make([]byte, chunksize)

	// Chunk from beginning of the file
	if _, err := reader.Read(buf); err != nil {
		return nil, err
	}
	if _, err := io.WriteString(digest, string(buf)); err != nil {
		return nil, err
	}

	// Chunk from random location that does not overlap with beginning or end of file
	if filesize-chunksize*3 > 0 {
		rand.Seed(filesize)
		offset := chunksize + rand.Int63n(filesize-2*chunksize)
		if _, err := reader.ReadAt(buf, offset); err != nil {
			return nil, err
		}
		if _, err := io.WriteString(digest, string(buf)); err != nil {
			return nil, err
		}
	}

	// Chunk from end of the file
	if filesize > chunksize {
		if _, err := reader.ReadAt(buf, filesize-chunksize); err != nil {
			return nil, err
		}
		if _, err := io.WriteString(digest, string(buf)); err != nil {
			return nil, err
		}
	}

	return digest.Sum(nil), nil
}

// ChecksummingReader computes an MD5 sum as it reads.
// If expected is not nil, then the calculated MD5 sum is compared to expected
// when EOF is read, and an error is returned if the sums do not match.
// ChecksummingReader is used to guard against network corruption.
// The checksum calculated happens to be compatible with the checksum that
// Amazon S3 uses in Content-MD5 header
// (see http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html)
type ChecksummingReader struct {
	reader   io.Reader
	expected []byte
	digest   hash.Hash
}

// NewChecksummingReader constructs a ChecksummingReader
func NewChecksummingReader(reader io.Reader, expected []byte) *ChecksummingReader {
	return &ChecksummingReader{reader: reader, expected: expected, digest: md5.New()}
}

// Read calls the Read() function of the underlying Reader and computes an MD5
// sum on the result. If EOF is read and expected is not nil, then the
// calculated MD5 sum is compared to expected, and an error is returned if the
// sums do not match.
func (cr ChecksummingReader) Read(buf []byte) (n int, err error) {
	n, err = cr.reader.Read(buf)
	if err != nil && err != io.EOF {
		return n, err
	}
	_, errChecksum := io.WriteString(cr.digest, string(buf[:n]))
	if errChecksum != nil {
		return n, errChecksum
	}
	if err == io.EOF && cr.expected != nil {
		checksum := cr.Sum()
		if !bytes.Equal(cr.expected, checksum) {
			return n, fmt.Errorf("Expected checksum %s but got %s", cr.expected, checksum)
		}
	}
	return n, err
}

// Close calls Close() on the underlying Reader, if the underling Reader is a
// ReadCloser
func (cr *ChecksummingReader) Close() error {
	if closer, ok := cr.reader.(io.ReadCloser); ok {
		return closer.Close()
	}
	return nil
}

// Sum returns the MD5 sum computed by the ChecksummingReader so far
func (cr *ChecksummingReader) Sum() []byte {
	return cr.digest.Sum(nil)
}
