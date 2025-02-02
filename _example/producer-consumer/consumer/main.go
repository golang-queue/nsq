package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/golang-queue/nsq"
	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"

	"github.com/appleboy/graceful"
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
	taskN := 10000
	rets := make(chan string, taskN)

	m := graceful.NewManager()

	// define the worker
	w := nsq.NewWorker(
		nsq.WithAddr("127.0.0.1:4150"),
		nsq.WithTopic("example"),
		nsq.WithChannel("foobar"),
		nsq.WithRunFunc(func(ctx context.Context, m core.TaskMessage) error {
			var v *job
			if err := json.Unmarshal(m.Payload(), &v); err != nil {
				return err
			}
			rets <- v.Message
			fmt.Println("got message:", v.Message)
			fmt.Println("wait 10 seconds")
			time.Sleep(10 * time.Second)
			return nil
		}),
	)

	// define the queue
	q := queue.NewPool(
		1,
		queue.WithWorker(w),
	)

	m.AddRunningJob(func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				select {
				case m := <-rets:
					fmt.Println("message:", m)
				default:
				}
				return nil
			case m := <-rets:
				fmt.Println("message:", m)
				time.Sleep(50 * time.Millisecond)
			}
		}
	})

	m.AddShutdownJob(func() error {
		// shutdown the service and notify all the worker
		q.Release()
		return nil
	})

	<-m.Done()
}
