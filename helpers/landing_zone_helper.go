package helpers

import (
	"github.com/anhamdan/etl-base/s3aws"
)

type LandingZoneHelper interface {
	Read(bucket, path string) ([]byte, error)
}

type landingZoneHelper struct {
	s3Client s3aws.S3Client
	path     string
}

type LandingZoneConfig struct {
	S3Bucket string
	Path     string
}

func NewLandingZoneHelper(s3Client s3aws.S3Client) *landingZoneHelper {
	helper := landingZoneHelper{
		s3Client: s3Client,
	}
	return &helper
}

func (lzh *landingZoneHelper) SetPath(path string) {
	lzh.path = path
}

func (lzh *landingZoneHelper) Read(bucket, path string) ([]byte, error) {
	body, err := lzh.s3Client.Read(bucket, path)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func (lzh *landingZoneHelper) GetFilenames(path string) (*[]*string, error) {
	fileNames, err := lzh.s3Client.ListObjects(path)
	if err != nil {
		return nil, err
	}
	return fileNames, nil
}
