module example

go 1.18

require (
	github.com/appleboy/graceful v0.0.4
	github.com/golang-queue/nsq v0.0.0-00010101000000-000000000000
	github.com/golang-queue/queue v0.0.13-0.20220420024737-03d90b78b732
)

require (
	github.com/golang/snappy v0.0.1 // indirect
	github.com/nsqio/go-nsq v1.1.0 // indirect
)

replace github.com/golang-queue/nsq => ../../
