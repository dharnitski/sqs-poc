package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

var client *sqs.Client
var queueURL = ""
var delayMS = 0

func handler(ctx context.Context, sqsEvent events.SQSEvent) error {
	for _, message := range sqsEvent.Records {
		sMInput := &sqs.SendMessageInput{
			MessageBody: &message.Body,
			QueueUrl:    &queueURL,
		}

		if delayMS > 0 {
			time.Sleep(time.Duration(delayMS) * time.Millisecond)
		}

		_, err := client.SendMessage(ctx, sMInput)
		if err != nil {
			return err
		}
		fmt.Printf("The message %s for event source %s length %d \n", message.MessageId, message.EventSource, len(message.Body))
	}

	return nil
}

// IMPORTANT!
// code posts to SAME queue
// intentionally implements infinite loop to do performance testings

func main() {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}
	cfg.Region = "us-east-1"

	queueName := os.Getenv("QUEUE")
	if queueName == "" {
		log.Fatalf("empty env QUEUE")
	}

	sDelay := os.Getenv("PROCESSING_TIME_MS")
	if sDelay != "" {
		delayMS, err = strconv.Atoi(sDelay)
		if err != nil {
			log.Fatalf("cannot parse PROCESSING_TIME_MS %q: %v", sDelay, err)
		}
	}

	client = sqs.NewFromConfig(cfg)

	gQInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}

	result, err := client.GetQueueUrl(ctx, gQInput)
	if err != nil {
		log.Fatalf("getting URL error: %v", err)
	}
	queueURL = *result.QueueUrl

	lambda.Start(handler)
}
