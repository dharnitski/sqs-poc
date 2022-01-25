package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jessevdk/go-flags"
)

type Options struct {
	DNSFile string `short:"q" long:"queue" description:"SQS Queue name" required:"true"`
	Threads int    `short:"t" long:"threads" description:"Redis data threads" required:"false"`
}

var options Options
var parser = flags.NewParser(&options, flags.Default)

const (
	minLen = 1_000
	maxLen = 10_000
)

var counter uint64

func main() {
	rand.Seed(time.Now().UnixNano())
	_, err := parser.Parse()
	if err != nil {
		panic(err)
	}
	if options.Threads == 0 {
		options.Threads = 1000
	}
	ctx := context.Background()
	err = run(ctx, options)
	if err != nil {
		log.Fatal(err)
	}
}

func mesLen() int {
	return rand.Intn(maxLen-minLen) + minLen
}

func run(ctx context.Context, options Options) error {

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}
	cfg.Region = "us-east-1"

	client := sqs.NewFromConfig(cfg)

	// Get URL of queue
	gQInput := &sqs.GetQueueUrlInput{
		QueueName: &options.DNSFile,
	}

	result, err := client.GetQueueUrl(ctx, gQInput)
	if err != nil {
		return err
	}

	queueURL := result.QueueUrl
	log.Printf("starting %d workers against %q...", parser.Options, *queueURL)

	for i := 0; i < options.Threads; i++ {
		go func(i int) {
			err := post(ctx, client, queueURL)
			if err != nil {
				log.Printf("worker %d failed: %v", i, err)
			}
		}(i)
	}
	prev := counter
	for {

		time.Sleep(1 * time.Second)
		fmt.Printf("rps: %d, posted: %d\n", counter-prev, counter)
		prev = counter
	}
}

func post(ctx context.Context, client *sqs.Client, queueURL *string) error {
	for {
		message := RandStringBytesMaskImpr(mesLen())
		sMInput := &sqs.SendMessageInput{
			MessageBody: aws.String(message),
			QueueUrl:    queueURL,
		}

		_, err := client.SendMessage(ctx, sMInput)
		if err != nil {
			return err
		}
		atomic.AddUint64(&counter, 1)
	}
}

// https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func RandStringBytesMaskImpr(n int) string {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}
