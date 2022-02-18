package sfnaws

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/sfn"
	"github.com/stretchr/testify/assert"
	"testing"
)

type sfnTest struct {
	name             string
	sfnClient        SFNMessageClient
	expectedResponse []byte
	expectedError    error
}

type sfnClientMock struct {
	sendTaskSuccessOutput *sfn.SendTaskSuccessOutput
	sendTaskSuccessError  error
}

func (mock sfnClientMock) SendTaskSuccess(input *sfn.SendTaskSuccessInput) (*sfn.SendTaskSuccessOutput, error) {
	return mock.sendTaskSuccessOutput, mock.sendTaskSuccessError
}

func TestSendTaskSuccess(t *testing.T) {
	tests := []sfnTest{
		{
			name:          "Fail when sending task success to step function",
			sfnClient:     sfnClientMock{sendTaskSuccessError: errors.New("some step function error")},
			expectedError: errors.New("some step function error"),
		},
		{
			name:      "Fail when sending task success to step function",
			sfnClient: sfnClientMock{},
		},
	}

	for _, test := range tests {
		fmt.Println(test.name)

		sfnClient := New()

		err := sfnClient.SendTaskSuccess("", "", test.sfnClient)

		assert.Equal(t, test.expectedError, err)
	}
}
