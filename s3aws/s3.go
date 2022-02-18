package s3aws

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
)

type S3Client interface {
	Read(bucket, path string) ([]byte, error)
	Insert(path string, content []byte) (*string, error)
	ListObjects(path string) (*[]*string, error)
}

type SvcClient interface {
	GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error)
	ListObjects(input *s3.ListObjectsInput) (*s3.ListObjectsOutput, error)
}

type s3Client struct {
	svc    SvcClient
	bucket string
}

func NewS3Client(svc SvcClient, bucket string) *s3Client {
	return &s3Client{svc: svc, bucket: bucket}
}

func (s3Client s3Client) Read(bucket, path string) ([]byte, error) {

	requestInput := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
	}

	result, err := s3Client.svc.GetObject(requestInput)
	if err != nil {
		return nil, err
	}

	defer result.Body.Close()
	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

func (s3Client s3Client) ListObjects(path string) (*[]*string, error) {
	requestInput := &s3.ListObjectsInput{
		Bucket: aws.String(s3Client.bucket),
		Marker: aws.String(path),
		Prefix: aws.String(path),
	}

	result, err := s3Client.svc.ListObjects(requestInput)
	if err != nil {
		return nil, err
	}

	fileNames := make([]*string, len(result.Contents))
	for i, output := range result.Contents {
		fileNames[i] = output.Key
	}

	return &fileNames, nil
}

func (s3Client s3Client) Insert(path string, content []byte) (*string, error) {
	body := bytes.NewReader(content)

	input := s3.PutObjectInput{
		Bucket: aws.String(s3Client.bucket),
		Key:    aws.String(path),
		Body:   body,
	}

	_, err := s3Client.svc.PutObject(&input)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Inserting into s3 bucket: %s on path: %s\n\n", s3Client.bucket, path)

	outputPath := s3Client.bucket + path

	return &outputPath, nil
}
