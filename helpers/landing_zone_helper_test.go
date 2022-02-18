package helpers

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/skf/etl-base/model"
	"github.com/skf/etl-base/s3aws"
	"github.com/stretchr/testify/assert"
	"testing"
)

type readTest struct {
	name             string
	s3Client         s3aws.S3Client
	expectedResponse []byte
	expectedError    error
}

type mockS3Client struct {
	readResponse   []byte
	readError      error
	insertResponse *string
	insertError    error
	listResponse   *[]*string
	listError      error
}

func (mock mockS3Client) Read(bucket, path string) ([]byte, error) {
	return mock.readResponse, mock.readError
}

func (mock mockS3Client) Insert(path string, content []byte) (*string, error) {
	return mock.insertResponse, mock.insertError
}

func (mock mockS3Client) ListObjects(path string) (*[]*string, error) {
	return mock.listResponse, mock.listError
}

var expectedTreeElemResponse = `[{"treeElemId":123,"hierarchyId":123,"branchLevel":123,"slotNumber":123,"name":null,"containerType":null,"description":null,"elementEnable":null,"parentEnable":null,"hierarchyType":null,"alarmFlags":null,"parentId":null,"parentRefId":null,"referenceId":null,"good":null,"alert":null,"danger":null,"overdue":null,"channelEnable":null}]`

func TestRead(t *testing.T) {
	defaultNumber := uint(123)

	treeElemData := []model.TreeElem{
		{
			TreeElemId:  &defaultNumber,
			HierarchyId: &defaultNumber,
			BranchLevel: &defaultNumber,
			SlotNumber:  &defaultNumber,
		},
	}

	treeElemDataOutput, _ := json.Marshal(treeElemData)

	tests := []readTest{
		{
			name:          "Fail when trying to read file from the landing zone",
			s3Client:      mockS3Client{readError: errors.New("some s3 error")},
			expectedError: errors.New("some s3 error"),
		},
		{
			name:             "Success when trying to read file from the landing zone",
			s3Client:         mockS3Client{readResponse: treeElemDataOutput},
			expectedResponse: []byte(expectedTreeElemResponse),
		},
	}

	for _, test := range tests {
		fmt.Println(test.name)

		helper := NewLandingZoneHelper(test.s3Client)

		response, err := helper.Read("someBucket", "some/path")

		assert.Equal(t, test.expectedResponse, response)
		assert.Equal(t, test.expectedError, err)
	}
}
