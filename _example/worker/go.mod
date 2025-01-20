module example

go 1.22

replace github.com/golang-queue/nsq => ../../

require (
	github.com/golang-queue/nsq v0.0.0-00010101000000-000000000000
	github.com/golang-queue/queue v0.3.0
)

require (
	github.com/golang/snappy v0.0.4 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/nsqio/go-nsq v1.1.0 // indirect
)
