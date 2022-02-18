package config

import (
	"log"
	"os"
	"strconv"
)

// MustGetAsString returns value for given environment variable
func MustGetAsString(variableName string) string {
	value := os.Getenv(variableName)
	if value == "" {
		log.Panicf("System variable %s not set", variableName)
	}
	return value
}

// GetAsString returns value for given environment variable, with default if not found
func GetAsString(variableName string, defaultValue string) string {
	value := os.Getenv(variableName)
	if value == "" {
		return defaultValue
	}
	return value
}

// EnsureValue ensures that the specified environment variable is set, with default if not found
func EnsureValue(variableName string, defaultValue string) string {
	value := os.Getenv(variableName)
	if value == "" {
		os.Setenv(variableName, defaultValue)
		return defaultValue
	}
	return value
}

// GetAsFloat returns value for given environment variable, with default if not found
func GetAsFloat(variableName string, defaultValue float64) (floatValue float64) {
	stringValue := os.Getenv(variableName)
	if stringValue == "" {
		return defaultValue
	}
	var err error
	if floatValue, err = strconv.ParseFloat(stringValue, 64); err != nil {
		log.Panicf("Failed to parse string %s to float - %+v\n", stringValue, err)
	}
	return
}

// GetAsInt returns value for given environment variable, with default if not found
func GetAsInt(variableName string, defaultValue int) (intValue int) {
	stringValue := os.Getenv(variableName)
	if stringValue == "" {
		return defaultValue
	}
	var err error
	if intValue, err = strconv.Atoi(stringValue); err != nil {
		log.Panicf("Failed to parse string %s to int - %+v\n", stringValue, err)
	}
	return
}

// GetAsBool returns value for given environment variable, with default if not found
func GetAsBool(variableName string, defaultValue bool) (boolValue bool) {
	stringValue := os.Getenv(variableName)
	if stringValue == "" {
		return defaultValue
	}
	var err error
	if boolValue, err = strconv.ParseBool(stringValue); err != nil {
		log.Panicf("Failed to parse string %s to bool - %+v\n", stringValue, err)
	}
	return
}
