package helpers

import (
	"encoding/json"
	"github.com/skf/etl-base/constants"
	"github.com/skf/etl-base/s3aws"
	"log"
)

type LoadingZoneHelper interface {
	Insert(entities interface{}, fileName string) (string, error)
}

type loadingZoneHelper struct {
	s3Client s3aws.S3Client
	enabled  bool
}

type LoadingZoneConfig struct {
	S3Bucket        string
	LZHelperEnabled bool
}

func NewLoadingZoneHelper(s3Client s3aws.S3Client, config LoadingZoneConfig) *loadingZoneHelper {
	helper := loadingZoneHelper{
		s3Client: s3Client,
		enabled:  config.LZHelperEnabled,
	}
	return &helper
}

func (lzh *loadingZoneHelper) Insert(entities interface{}, path string) (string, error) {
	if !lzh.enabled {
		log.Printf("Loading zone helper disabled, not populating file: %s to the landing zone \n", path)
		return "", nil
	}

	content := convertToJson(entities)
	outputPath, err := lzh.s3Client.Insert(path, content)
	if err != nil {
		return constants.EmptyString, err
	}
	return getOutputPath(*outputPath), nil
}

func convertToJson(entities interface{}) []byte {
	content, _ := json.MarshalIndent(entities, "", "  ")

	return content
}

func getOutputPath(path string) string {
	return "s3://" + path
}
