module example

go 1.25.0

require (
	github.com/appleboy/graceful v1.3.0
	github.com/golang-queue/nsq v0.3.0
	github.com/golang-queue/queue v0.5.0
)

require (
	github.com/golang/snappy v1.0.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/nsqio/go-nsq v1.1.0 // indirect
)

replace github.com/golang-queue/nsq => ../../
