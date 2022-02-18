package config

import (
	"github.com/anhamdan/etl-base/helpers"
)

type Config struct {
	LandingZoneConfig     helpers.LandingZoneConfig
	LoadingZoneConfig     helpers.LoadingZoneConfig
	WorkflowManagerConfig helpers.WorkflowManagerConfig
	AWSConfig             AWSConfig
}

type AWSConfig struct {
	Profile string
	Region  string
}

func Initialize(Type string) Config {
	if Type == "analyst" {
		return Config{
			LandingZoneConfig: helpers.LandingZoneConfig{
				S3Bucket: GetAsString("S3_BUCKET_LANDING_ZONE", "landing-zone-poc"),
			},
			LoadingZoneConfig: helpers.LoadingZoneConfig{
				S3Bucket:        GetAsString("S3_BUCKET_LOADING_ZONE", "enlight-loading-zone-poc"),
				LZHelperEnabled: GetAsBool("LOADING_ZONE_HELPER_ENABLED", true),
			},
			WorkflowManagerConfig: helpers.WorkflowManagerConfig{
				// todo this needs to be changed when we get notified of the real sqs queue
				SQSURL: GetAsString("SQS_URL", ""),
				// todo this needs to be changed when we send event to the real topic
				TopicARN:               GetAsString("MANAGER_TOPIC_ARN", ""),
				GroupID:                GetAsString("MANAGER_GROUP_ID", "123"),
				WorkFlowManagerEnabled: GetAsBool("MANAGER_ENABLED", true),
			},
			AWSConfig: AWSConfig{
				Profile: GetAsString("AWS_PROFILE", "default"),
				Region:  GetAsString("AWS_REGION", "eu-west-1"),
			},
		}
	}
	return Config{}
}
