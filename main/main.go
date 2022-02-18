package main

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"sync"
)

var awsSession *session.Session

func main() {
	// Set up the functionality for concurrent message handling
	wg := &sync.WaitGroup{}
	wg.Add(2)
	wg.Wait()
}
