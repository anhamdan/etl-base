package helpers

import (
	"github.com/anhamdan/etl-base/config"
	"github.com/anhamdan/etl-base/s3aws"
	"github.com/anhamdan/etl-base/sfnaws"
	"github.com/anhamdan/etl-base/sqsaws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
	"sync"
)

type BaseHelper interface {
}
type baseHelper struct {
	kola string
}

func NewBaseHelper(kola string) *baseHelper {
	helper := baseHelper{
		kola: kola,
	}
	return &helper
}

func initAwsSession() (*session.Session, error) {
	importConfig := initConfig("analyst")
	awsSession, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable, // Must be set to enable
		Profile:           importConfig.AWSConfig.Profile,
	})
	if err != nil {
		log.Fatalf("error: %+v\n", err)
	}

	return awsSession, nil
}
func initConfig(Type string) config.Config {
	return config.Initialize(Type)
}

func initLandingZone(awsSession *session.Session, importConfig config.Config) *landingZoneHelper {
	s3LandingZoneSession := s3.New(awsSession)
	s3LandingZoneClient := s3aws.NewS3Client(s3LandingZoneSession, importConfig.LandingZoneConfig.S3Bucket)
	return NewLandingZoneHelper(s3LandingZoneClient)
}

func initLoadingZone(awsSession *session.Session, importConfig config.Config) *loadingZoneHelper {
	s3LoadingZoneSession := s3.New(awsSession)
	s3LoadingZoneClient := s3aws.NewS3Client(s3LoadingZoneSession, importConfig.LoadingZoneConfig.S3Bucket)
	return NewLoadingZoneHelper(s3LoadingZoneClient, importConfig.LoadingZoneConfig)
}

func initWfmHelper(awsSession *session.Session, importConfig config.Config) interface{} {
	// SQS
	sqsSession := sqs.New(awsSession)
	sqsClient := sqsaws.New(sqsSession, importConfig.WorkflowManagerConfig.SQSURL)

	// Step function
	sfnClient := sfnaws.New()

	return NewWFMHelper(sqsClient, sfnClient, importConfig.WorkflowManagerConfig)
}

func initChannels() (chan *sqs.Message, chan error) {
	return make(chan *sqs.Message), make(chan error)
}

func handleErrMsg(errChan chan error, wg *sync.WaitGroup) {
	defer wg.Done()

	for err := range errChan {
		log.Printf("There was an error when trying to receive events: %s", err.Error())
	}
}
