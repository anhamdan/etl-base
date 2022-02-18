package helpers

import (
	"bytes"
	"encoding/json"
	"errors"
	"etl-base/sfnaws"
	"etl-base/sqsaws"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sfn"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

type wfmHelperTestCase struct {
	name          string
	input         interface{}
	sqsClient     sqsaws.SQSClient
	sfnClient     sfnaws.SFNClient
	expectedEvent *ManagerEvent
	expectedError error
	config        WorkflowManagerConfig
}

type sqsClientMock struct {
	successMsg         *string
	errorMsg           error
	deleteMessageError error
}

func (sqsMock sqsClientMock) Poll(chn chan *sqs.Message, errChan chan error) {
	defer close(chn)

	if sqsMock.successMsg != nil {
		chn <- &sqs.Message{
			Attributes:             nil,
			Body:                   sqsMock.successMsg,
			MD5OfBody:              nil,
			MD5OfMessageAttributes: nil,
			MessageAttributes:      nil,
			MessageId:              nil,
			ReceiptHandle:          nil,
		}
	}

	if sqsMock.errorMsg != nil {
		errChan <- sqsMock.errorMsg
	}
}

func (sqsMock sqsClientMock) DeleteMessage(msg *sqs.Message) error {
	return sqsMock.deleteMessageError
}

type sfnClientMock struct {
	sendTaskError error
}

func (sfnMock sfnClientMock) SendTaskSuccess(output, taskToken string, svc sfnaws.SFNMessageClient) error {
	return sfnMock.sendTaskError
}

func (sfnMock sfnClientMock) CreateSFNClient(sess *session.Session, roleArn string) *sfn.SFN {
	return &sfn.SFN{}
}

var wfmConfig WorkflowManagerConfig

func TestReceiveEventsSuccess(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	channel := make(chan *sqs.Message, 1000)
	errorChan := make(chan error, 1000)

	msg := "message from sqs"

	sqsClientMock := sqsClientMock{successMsg: &msg}

	wfmHelper := NewWFMHelper(sqsClientMock, nil, wfmConfig)
	wfmHelper.enabled = true

	wfmHelper.ReceiveEvents(channel, errorChan, wg)

	for message := range channel {
		assert.Equal(t, "message from sqs", *message.Body)
	}
}

func TestReceiveEventsFailure(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	channel := make(chan *sqs.Message, 1000)
	errorChan := make(chan error, 1000)

	sqsClientMock := sqsClientMock{errorMsg: errors.New("some sqs error")}

	wfmHelper := NewWFMHelper(sqsClientMock, nil, wfmConfig)
	wfmHelper.enabled = true

	wfmHelper.ReceiveEvents(channel, errorChan, wg)

	for channelError := range errorChan {
		assert.Equal(t, errors.New("some sqs error"), channelError)
		close(errorChan)
	}
}

func TestDeleteMessage(t *testing.T) {
	tests := []wfmHelperTestCase{
		{
			name:          "Fail when trying to delete message from the sqs queue",
			sqsClient:     sqsClientMock{deleteMessageError: errors.New("failed to delete sqs message")},
			expectedError: errors.New("failed to delete sqs message"),
		},
		{
			name:      "Success when trying to delete message from the sqs queue",
			sqsClient: sqsClientMock{},
		},
	}

	for _, test := range tests {
		wfmHelper := NewWFMHelper(test.sqsClient, nil, wfmConfig)
		wfmHelper.enabled = true

		err := wfmHelper.DeleteMessage(&sqs.Message{}) //<--- function under test

		assert.Equal(t, test.expectedError, err)
	}
}

func TestParseEvent(t *testing.T) {
	var tempEvent ManagerEvent
	expectedJsonErr := json.Unmarshal([]byte(`>`), &tempEvent)

	tests := []wfmHelperTestCase{
		{
			name:          "Fail when trying to parse sqs body from the event",
			input:         []byte(`>`),
			expectedError: expectedJsonErr,
		},
		{
			name:          "Fail when trying to parse sqs body message from the event",
			input:         []byte(`{"Message": ">"}`),
			expectedError: expectedJsonErr,
		},
		{
			name:          "Success when trying to parse sqs body message to an event",
			input:         []byte(getManagerEvent()),
			expectedEvent: getExpectedManagerEvent(),
		},
	}

	for _, test := range tests {
		wfmHelper := NewWFMHelper(test.sqsClient, nil, wfmConfig)
		wfmHelper.enabled = true
		event, err := wfmHelper.ParseEvent(test.input.([]byte)) //<--- function under test

		assert.Equal(t, test.expectedEvent, event)
		assert.Equal(t, test.expectedError, err)

	}
}

func TestSendEvent(t *testing.T) {
	errChannel := make(chan string)
	_, err := json.Marshal(errChannel)

	tests := []wfmHelperTestCase{
		{
			name:          "Fail sending event to wfm",
			input:         errChannel,
			expectedError: err,
			config: WorkflowManagerConfig{
				WorkFlowManagerEnabled: true,
			},
		},
		{
			name:  "Fail sending task success to step functions",
			input: []byte(`{"Message": ">"}`),
			sfnClient: sfnClientMock{
				sendTaskError: errors.New("some send task error"),
			},
			expectedError: errors.New("some send task error"),
			config: WorkflowManagerConfig{
				WorkFlowManagerEnabled: true,
			},
		},
		{
			name:      "Success when sending event helper enabled",
			input:     []byte(`{"Message": ">"}`),
			sfnClient: sfnClientMock{},
			config: WorkflowManagerConfig{
				WorkFlowManagerEnabled: true,
			},
		},
		{
			name: "Success when sending event helper disabled",
			config: WorkflowManagerConfig{
				WorkFlowManagerEnabled: false,
			},
		},
		{
			name:      "Success when sending event helper disabled",
			sfnClient: sfnClientMock{},
			config: WorkflowManagerConfig{
				WorkFlowManagerEnabled: true,
			},
		},
	}
	for _, test := range tests {
		wfmHelper := NewWFMHelper(test.sqsClient, test.sfnClient, test.config)

		err := wfmHelper.SendEvent(test.input, nil, "", "") //<--- function under test

		assert.Equal(t, test.expectedError, err)
	}

}

func getManagerEvent() string {
	var buffer bytes.Buffer

	buffer.WriteString("{")
	buffer.WriteString(`"Type": "",`)
	buffer.WriteString(`"MessageId": "",`)
	buffer.WriteString(`"SequenceNumber": "",`)
	buffer.WriteString(`"TopicArn": "",`)
	buffer.WriteString(`"Message": "{`)
	buffer.WriteString(`\"inputFiles\": [\"some/kind/of/path\"],`)
	buffer.WriteString(`\"dataSource\": \"analyst\",`)
	buffer.WriteString(`\"providerID\": \"123\",`)
	buffer.WriteString(`\"importJobID\": \"456\",`)
	buffer.WriteString(`\"processID\": \"789\",`)
	buffer.WriteString(`\"filesByOrder\": true,`)
	buffer.WriteString(`\"loadType\": \"initial\",`)
	buffer.WriteString(`\"taskToken\": \"101\",`)
	buffer.WriteString(`\"roleArn\": \"roleArn\",`)
	buffer.WriteString(`\"etlSpecificData\": \"something\"`)
	buffer.WriteString(`}"`)
	buffer.WriteString("}")
	fmt.Println(buffer.String())

	return buffer.String()
}

func getExpectedManagerEvent() *ManagerEvent {
	return &ManagerEvent{
		InputFiles:      []string{"some/kind/of/path"},
		DataSource:      "analyst",
		ProviderID:      "123",
		ImportJobID:     "456",
		ProcessID:       "789",
		FilesByOrder:    true,
		LoadType:        "initial",
		TaskToken:       "101",
		RoleArn:         "roleArn",
		EtlSpecificData: "something",
	}
}
