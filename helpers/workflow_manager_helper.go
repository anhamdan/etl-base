package helpers

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/skf/etl-base/constants"
	"github.com/skf/etl-base/sfnaws"
	"github.com/skf/etl-base/sqsaws"
	"log"
	"sync"
	"time"
)

type ManagerEvent struct {
	InputFiles      []string `json:"inputFiles"`
	DataSource      string   `json:"dataSource"`
	ProviderID      string   `json:"providerID"`
	ImportJobID     string   `json:"importJobID"`
	ProcessID       string   `json:"processID"`
	FilesByOrder    bool     `json:"filesByOrder"`
	LoadType        string   `json:"loadType"`
	TaskToken       string   `json:"taskToken"`
	RoleArn         string   `json:"roleArn"`
	EtlSpecificData string   `json:"etlSpecificData"`
}

type ManagerOutputEvent struct {
	OutputFiles []string `json:"outputFiles"`
}

type WorkflowManagerHelper interface {
	ReceiveEvents(chn chan *sqs.Message, errChan chan error, wg *sync.WaitGroup)
	SendEvent(outputEvent interface{}, sess *session.Session, roleARN, taskToken string) error
	DeleteMessage(msg *sqs.Message) error
	GetEvent(chnMessages chan *sqs.Message) (*ManagerEvent, error)
	ParseEvent(msg []byte) (*ManagerEvent, error)
	IsEnabled() bool
}

type workflowManagerHelper struct {
	sqsClient sqsaws.SQSClient
	sfnClient sfnaws.SFNClient
	topic     string
	groupID   string
	enabled   bool
}

type WorkflowManagerConfig struct {
	WorkFlowManagerEnabled bool
	SQSURL                 string
	TopicARN               string
	GroupID                string
}

type sqsBody struct {
	Type           string    `json:"Type"`
	MessageID      string    `json:"MessageId"`
	SequenceNumber string    `json:"SequenceNumber"`
	TopicArn       string    `json:"TopicArn"`
	Message        string    `json:"Message"`
	Timestamp      time.Time `json:"Timestamp"`
	UnsubscribeURL string    `json:"UnsubscribeURL"`
}

func NewWFMHelper(sqsClient sqsaws.SQSClient, sfnClient sfnaws.SFNClient, config WorkflowManagerConfig) *workflowManagerHelper {
	return &workflowManagerHelper{sqsClient: sqsClient, sfnClient: sfnClient, enabled: config.WorkFlowManagerEnabled, topic: config.TopicARN, groupID: config.GroupID}
}

func (helper *workflowManagerHelper) ReceiveEvents(chn chan *sqs.Message, errChan chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	if helper.enabled {
		helper.sqsClient.Poll(chn, errChan)
	}
}

func (helper *workflowManagerHelper) SendEvent(outputEvent interface{}, sess *session.Session, roleARN, taskToken string) error {
	if helper.enabled {
		msg, err := json.Marshal(outputEvent)
		if err != nil {
			return err
		}

		awsSFNClient := helper.sfnClient.CreateSFNClient(sess, roleARN)

		err = helper.sfnClient.SendTaskSuccess(string(msg), taskToken, awsSFNClient)
		if err != nil {
			return err
		}

	} else {
		log.Println("Downstream events disabled, not sending message to workflow manager")
	}
	return nil
}

func (helper *workflowManagerHelper) DeleteMessage(msg *sqs.Message) error {
	if helper.enabled {
		if err := helper.sqsClient.DeleteMessage(msg); err != nil {
			return err
		}

	} else {
		log.Println("Work Flow Manager disabled, not deleting message from queue")
	}
	return nil
}

func (helper *workflowManagerHelper) GetEvent(chnMessages chan *sqs.Message) (*ManagerEvent, error) {
	var event *ManagerEvent
	var err error
	if helper.enabled {
		message := <-chnMessages
		event, err = helper.ParseEvent([]byte(*message.Body))

		if err := helper.DeleteMessage(message); err != nil {
			log.Printf("Could not delete message from the sqs queue %s", err.Error())
		}
	} else {
		body := constants.EmptyString
		message := &sqs.Message{Body: &body}
		event, _ = helper.ParseEvent([]byte(*message.Body))

		//Todo this is only temporary for dev purposes. It is needed when receiving events from the step function
		// state machine
		/*fileNames, err := landingZoneHelper.GetFilenames("adam/analyst/")
		if err != nil {
			log.Fatalf("error: %+v\n", err)
		}*/
		event = &ManagerEvent{}
		event.InputFiles = []string{
			/*"s3://landing-zone-poc/analyst/data_init_2021.11.25_11:12:09.644.json",
			"s3://landing-zone-poc/analyst/data_init_2021.11.25_11:12:10.687.json",
			"s3://landing-zone-poc/analyst/data_init_2021.11.25_11:12:10.891.json",*/
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:39:49.210.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:39:52.691.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:39:53.580.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:39:55.765.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:39:58.006.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:40:05.896.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:40:08.725.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:40:10.684.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:40:13.215.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:40:16.324.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:40:20.703.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:40:26.109.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:40:45.640.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:40:48.713.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:40:52.384.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:40:56.855.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:41:00.822.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:41:05.756.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:41:14.144.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:41:15.782.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:41:24.404.json",
			"s3://landing-zone-poc/adam/analyst/data_init_2022-01-09T16:41:27.272.json",
		}
	}
	return event, err
}

func (helper *workflowManagerHelper) ParseEvent(msg []byte) (*ManagerEvent, error) {
	var sqsBody sqsBody

	if err := json.Unmarshal(msg, &sqsBody); err != nil {
		return nil, err
	}

	var event ManagerEvent
	if err := json.Unmarshal([]byte(sqsBody.Message), &event); err != nil {
		return nil, err
	}

	return &event, nil
}

func (helper *workflowManagerHelper) IsEnabled() bool {
	return helper.enabled
}
