build_lambda:
	GOOS=linux GOARCH=amd64 go build -o bin/lambda/sqs-poc/main ./cmd/lambda
	zip -j bin/sqs-poc.zip bin/lambda/sqs-poc/main