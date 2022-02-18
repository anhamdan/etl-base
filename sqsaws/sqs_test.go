package sqsaws

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"testing"
)

type sqsTestCase struct {
	name             string
	msg              *sqs.Message
	sqsMessageClient SQSMessageClient
	expectedError    error
}

type mockSqsClient struct {
	receiveMessageResponse *sqs.ReceiveMessageOutput
	receiveMessageError    error
	deleteMessageResponse  *sqs.DeleteMessageOutput
	deleteMessageError     error
}

func (m mockSqsClient) ReceiveMessage(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	return m.receiveMessageResponse, m.receiveMessageError
}

func (m mockSqsClient) DeleteMessage(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	return m.deleteMessageResponse, m.deleteMessageError
}

func TestPollSuccess(t *testing.T) {
	channel := make(chan *sqs.Message, 1000)

	successMsgString := "message from sqs"
	successMessage := &sqs.Message{
		Attributes:             nil,
		Body:                   &successMsgString,
		MD5OfBody:              nil,
		MD5OfMessageAttributes: nil,
		MessageAttributes:      nil,
		MessageId:              nil,
		ReceiptHandle:          nil,
	}

	fmt.Println("name: Success when reading messages from the aws sqs service")
	mockedSQSMessageClient := mockSqsClient{receiveMessageResponse: &sqs.ReceiveMessageOutput{Messages: []*sqs.Message{successMessage}}}

	sqsClient := New(mockedSQSMessageClient, "")

	go sqsClient.Poll(channel, nil)

	message := <-channel
	assert.Equal(t, "message from sqs", *message.Body)
}

func TestPollFailure(t *testing.T) {
	errChan := make(chan error, 1000)
	expectedError := errors.New("failed to fetch sqs message, error: some sqs error")

	fmt.Println("name: Success when reading messages from the aws sqs service")
	mockedSQSMessageClient := mockSqsClient{receiveMessageError: errors.New("some sqs error")}

	sqsClient := New(mockedSQSMessageClient, "")

	go sqsClient.Poll(nil, errChan)

	err := <-errChan
	assert.Equal(t, expectedError, err)
}

func TestDeleteMessage(t *testing.T) {
	msgID := "123"

	tests := []sqsTestCase{
		{
			name: "Failure when deleting message from sqs queue",
			msg: &sqs.Message{
				Attributes:             nil,
				Body:                   nil,
				MD5OfBody:              nil,
				MD5OfMessageAttributes: nil,
				MessageAttributes:      nil,
				MessageId:              &msgID,
				ReceiptHandle:          nil,
			},
			sqsMessageClient: mockSqsClient{deleteMessageError: errors.New("some delete sqs message error")},
			expectedError:    errors.New(fmt.Sprintf("Deleting message with id: %s failed", msgID)),
		},
		{
			name: "Success when deleting message from sqs queue",
			msg: &sqs.Message{
				Attributes:             nil,
				Body:                   nil,
				MD5OfBody:              nil,
				MD5OfMessageAttributes: nil,
				MessageAttributes:      nil,
				MessageId:              &msgID,
				ReceiptHandle:          nil,
			},
			sqsMessageClient: mockSqsClient{},
		},
	}

	for _, test := range tests {
		fmt.Println(test.name)

		sqsClient := New(test.sqsMessageClient, "")

		err := sqsClient.DeleteMessage(test.msg) //<--- function under test

		assert.Equal(t, test.expectedError, err)
	}
}
