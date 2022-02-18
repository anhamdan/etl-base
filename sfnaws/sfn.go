package sfnaws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sfn"
)

type SFNClient interface {
	SendTaskSuccess(output, taskToken string, svc SFNMessageClient) error
	CreateSFNClient(sess *session.Session, roleArn string) *sfn.SFN
}

type sfnClient struct{}

type SFNMessageClient interface {
	SendTaskSuccess(input *sfn.SendTaskSuccessInput) (*sfn.SendTaskSuccessOutput, error)
}

func New() *sfnClient {
	return &sfnClient{}
}

func (client sfnClient) SendTaskSuccess(output, taskToken string, svc SFNMessageClient) error {
	_, err := svc.SendTaskSuccess(&sfn.SendTaskSuccessInput{
		Output:    aws.String(output),
		TaskToken: aws.String(taskToken),
	})

	if err != nil {
		return err
	}

	return nil
}

func (client sfnClient) CreateSFNClient(sess *session.Session, roleArn string) *sfn.SFN {
	credentials := stscreds.NewCredentials(sess, roleArn)
	sfnClient := sfn.New(sess, &aws.Config{Credentials: credentials})

	return sfnClient
}
