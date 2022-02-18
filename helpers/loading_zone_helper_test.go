package helpers

import (
	"errors"
	"fmt"
	"github.com/skf/etl-base/s3aws"
	"github.com/stretchr/testify/assert"
	"testing"
)

type insertTest struct {
	name          string
	entities      interface{}
	path          string
	expectedPath  string
	expectedError error
	s3Client      s3aws.S3Client
	config        LoadingZoneConfig
}

type s3ClientMock struct {
	readResponse   []byte
	readError      error
	insertResponse *string
	insertError    error
	listResponse   *[]*string
	listError      error
}

func (mock s3ClientMock) Read(bucket, path string) ([]byte, error) {
	return mock.readResponse, mock.readError
}

func (mock s3ClientMock) Insert(path string, content []byte) (*string, error) {
	return mock.insertResponse, mock.insertError
}

func (mock s3ClientMock) ListObjects(path string) (*[]*string, error) {
	return mock.listResponse, mock.listError
}

func TestInsert(t *testing.T) {
	insertResponse := "some/path"

	tests := []insertTest{
		{
			name:     "failed when inserting to the loading zone",
			s3Client: s3ClientMock{insertError: errors.New("some inserting error")},
			config: LoadingZoneConfig{
				LZHelperEnabled: true,
			},
			expectedError: errors.New("some inserting error"),
		},
		{
			name: "Success with the loading zone disable",
			path: "some/path",
			config: LoadingZoneConfig{
				LZHelperEnabled: false,
			},
			expectedPath: "",
		},
		{
			name: "Success with the loading zone enable",
			path: "some/path",
			config: LoadingZoneConfig{
				LZHelperEnabled: true,
			},
			s3Client:     s3ClientMock{insertResponse: &insertResponse},
			expectedPath: "s3://some/path",
		},
	}

	for _, test := range tests {
		fmt.Println(test.name)

		helper := NewLoadingZoneHelper(test.s3Client, test.config)
		response, err := helper.Insert(test.entities, test.path)

		assert.Equal(t, test.expectedPath, response)
		assert.Equal(t, test.expectedError, err)
	}
}
