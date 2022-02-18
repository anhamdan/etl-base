package sqsaws

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
	"time"
)

type SQSClient interface {
	Poll(chn chan *sqs.Message, errChan chan error)
	DeleteMessage(msg *sqs.Message) error
}

type SQSMessageClient interface {
	ReceiveMessage(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error)
}

type sqsClient struct {
	sqs SQSMessageClient
	url string
}

func New(sqs SQSMessageClient, url string) sqsClient {
	return sqsClient{sqs: sqs, url: url}
}

func (client sqsClient) Poll(chn chan *sqs.Message, errChan chan error) {
	defer close(chn)
	defer close(errChan)

	log.Printf("Listening on stack queue: %s", client.url)

	for {
		output, err := client.sqs.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:            &client.url,
			MaxNumberOfMessages: aws.Int64(1),
			WaitTimeSeconds:     aws.Int64(2),
		})

		if err != nil {
			errChan <- errors.New(fmt.Sprintf("failed to fetch sqs message, error: %s", err.Error()))
			time.Sleep(5 * time.Second)
		}

		for _, message := range output.Messages {
			chn <- message
		}
	}
}

func (client sqsClient) DeleteMessage(msg *sqs.Message) error {
	fmt.Printf("Deleting message with id: %s\n", *msg.MessageId)

	_, err := client.sqs.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &client.url,
		ReceiptHandle: msg.ReceiptHandle,
	})
	if err != nil {
		return errors.New(fmt.Sprintf("Deleting message with id: %s failed", *msg.MessageId))
	}

	return nil
}
