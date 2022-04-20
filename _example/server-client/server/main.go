package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/golang-queue/nsq"
	"github.com/golang-queue/queue"
)

type job struct {
	Message string
}

func (j *job) Bytes() []byte {
	b, err := json.Marshal(j)
	if err != nil {
		panic(err)
	}
	return b
}

func main() {
	taskN := 5

	// define the worker
	w := nsq.NewWorker(
		nsq.WithAddr("127.0.0.1:4150"),
		nsq.WithTopic("example"),
		nsq.WithChannel("foobar"),
		nsq.WithDisableConsumer(),
	)

	// define the queue
	q := queue.NewPool(
		0,
		queue.WithWorker(w),
	)

	// assign tasks in queue
	for i := 0; i < taskN; i++ {
		go func(i int) {
			if err := q.Queue(&job{
				Message: fmt.Sprintf("handle the job: %d", i+1),
			}); err != nil {
				log.Fatal(err)
			}
		}(i)
	}

	time.Sleep(1 * time.Second)
	// shutdown the service and notify all the worker
	q.Release()
}
