package main

import (
	"etl-base/config"
	"etl-base/helpers"
	"etl-base/s3aws"
	"etl-base/sfnaws"
	"etl-base/sqsaws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
	"sync"
)

var awsSession *session.Session

func main() {
	// Set up the functionality for concurrent message handling
	wg := &sync.WaitGroup{}
	wg.Add(2)
	wg.Wait()
}

func initConfig(Type string) config.Config {
	return config.Initialize(Type)
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

func initLandingZone(awsSession *session.Session, importConfig config.Config) interface{} {
	s3LandingZoneSession := s3.New(awsSession)
	s3LandingZoneClient := s3aws.NewS3Client(s3LandingZoneSession, importConfig.LandingZoneConfig.S3Bucket)
	return helpers.NewLandingZoneHelper(s3LandingZoneClient)
}

func initLoadingZone(awsSession *session.Session, importConfig config.Config) interface{} {
	s3LoadingZoneSession := s3.New(awsSession)
	s3LoadingZoneClient := s3aws.NewS3Client(s3LoadingZoneSession, importConfig.LoadingZoneConfig.S3Bucket)
	return helpers.NewLoadingZoneHelper(s3LoadingZoneClient, importConfig.LoadingZoneConfig)
}

func initWfmHelper(awsSession *session.Session, importConfig config.Config) interface{} {
	// SQS
	sqsSession := sqs.New(awsSession)
	sqsClient := sqsaws.New(sqsSession, importConfig.WorkflowManagerConfig.SQSURL)

	// Step function
	sfnClient := sfnaws.New()

	return helpers.NewWFMHelper(sqsClient, sfnClient, importConfig.WorkflowManagerConfig)
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
