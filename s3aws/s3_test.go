package s3aws

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"strings"
	"testing"
)

type s3ReadTest struct {
	name             string
	s3Client         SvcClient
	expectedResponse []byte
	expectedError    error
}

type s3InsertTest struct {
	name          string
	s3Client      SvcClient
	expectedPath  *string
	expectedError error
}

type mockS3Client struct {
	getObjectResponse  *s3.GetObjectOutput
	getObjectError     error
	putObjectResponse  *s3.PutObjectOutput
	putObjectError     error
	listObjectResponse *s3.ListObjectsOutput
	listObjectError    error
}

func (mock mockS3Client) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return mock.getObjectResponse, mock.getObjectError
}
func (mock mockS3Client) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	return mock.putObjectResponse, mock.putObjectError
}
func (mock mockS3Client) ListObjects(input *s3.ListObjectsInput) (*s3.ListObjectsOutput, error) {
	return mock.listObjectResponse, mock.listObjectError
}

var (
	defaultPath   = "some/path"
	defaultBucket = "someBucket"
)

type errReader int

func (errReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("test error")
}

func TestRead(t *testing.T) {
	faultyReadCloser := ioutil.NopCloser(errReader(0))

	tests := []s3ReadTest{
		{
			name:          "Fail when trying to get object from s3",
			s3Client:      mockS3Client{getObjectError: errors.New("some s3 error")},
			expectedError: errors.New("some s3 error"),
		},
		{
			name: "Fail when trying to get object from s3",
			s3Client: mockS3Client{getObjectResponse: &s3.GetObjectOutput{
				Body: faultyReadCloser,
			}},
			expectedError: errors.New("test error"),
		},
		{
			name: "Success when trying to get object from s3",
			s3Client: mockS3Client{getObjectResponse: &s3.GetObjectOutput{
				Body: ioutil.NopCloser(strings.NewReader(`[{"TreeElemId": 123}]`)),
			}},
			expectedResponse: []byte(`[{"TreeElemId": 123}]`),
		},
	}

	for _, test := range tests {
		fmt.Println(test.name)

		s3Client := NewS3Client(test.s3Client, "")

		response, err := s3Client.Read(defaultBucket, defaultPath) //<--- function under test

		assert.Equal(t, test.expectedResponse, response)
		assert.Equal(t, test.expectedError, err)
	}
}

func TestInsert(t *testing.T) {
	expectedPath := "some/path"

	tests := []s3InsertTest{
		{
			name:          "Fail when trying to put object in s3 bucket",
			s3Client:      mockS3Client{putObjectError: errors.New("some s3 error")},
			expectedError: errors.New("some s3 error"),
		},
		{
			name:         "Success when trying to put object in s3 bucket",
			expectedPath: &expectedPath,
			s3Client:     mockS3Client{},
		},
	}

	for _, test := range tests {
		fmt.Println(test.name)

		s3Client := NewS3Client(test.s3Client, "")

		path, err := s3Client.Insert("some/path", []byte(``)) //<--- function under test

		assert.Equal(t, test.expectedPath, path)
		assert.Equal(t, test.expectedError, err)
	}
}
