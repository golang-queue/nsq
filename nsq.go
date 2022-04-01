package nsq

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-queue/queue"

	"github.com/nsqio/go-nsq"
)

var _ queue.Worker = (*Worker)(nil)

// Worker for NSQ
type Worker struct {
	q         *nsq.Consumer
	p         *nsq.Producer
	stopOnce  sync.Once
	stop      chan struct{}
	stopFlag  int32
	startFlag int32
	opts      options
	tasks     chan *nsq.Message
}

// NewWorker for struc
func NewWorker(opts ...Option) *Worker {
	w := &Worker{
		opts:  newOptions(opts...),
		stop:  make(chan struct{}),
		tasks: make(chan *nsq.Message, 1),
	}

	cfg := nsq.NewConfig()
	cfg.MaxInFlight = w.opts.maxInFlight

	if err := w.startProducer(cfg); err != nil {
		panic(err)
	}

	if err := w.startConsumer(cfg); err != nil {
		panic(err)
	}

	return w
}

func (w *Worker) startProducer(cfg *nsq.Config) error {
	var err error

	w.p, err = nsq.NewProducer(w.opts.addr, cfg)

	return err
}

func (w *Worker) startConsumer(cfg *nsq.Config) error {
	if w.opts.disableConsumer {
		return nil
	}

	var err error

	w.q, err = nsq.NewConsumer(w.opts.topic, w.opts.channel, cfg)
	if err != nil {
		return err
	}

	w.q.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		// re-queue the job if worker has been shutdown.
		if atomic.LoadInt32(&w.stopFlag) == 1 {
			msg.Requeue(-1)
			return nil
		}

		if len(msg.Body) == 0 {
			// Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
			// In this case, a message with an empty body is simply ignored/discarded.
			return nil
		}

		select {
		case w.tasks <- msg:
		case <-w.stop:
			if msg != nil {
				// re-queue the job if worker has been shutdown.
				msg.Requeue(-1)
			}
		}

		return nil
	}))

	err = w.q.ConnectToNSQD(w.opts.addr)
	if err != nil {
		return err
	}

	atomic.CompareAndSwapInt32(&w.startFlag, 0, 1)
	return nil
}

func (w *Worker) handle(job queue.Job) error {
	// create channel with buffer size 1 to avoid goroutine leak
	done := make(chan error, 1)
	panicChan := make(chan interface{}, 1)
	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), job.Timeout)
	defer func() {
		cancel()
	}()

	// run the job
	go func() {
		// handle panic issue
		defer func() {
			if p := recover(); p != nil {
				panicChan <- p
			}
		}()

		// run custom process function
		done <- w.opts.runFunc(ctx, job)
	}()

	select {
	case p := <-panicChan:
		panic(p)
	case <-ctx.Done(): // timeout reached
		return ctx.Err()
	case <-w.stop: // shutdown service
		// cancel job
		cancel()

		leftTime := job.Timeout - time.Since(startTime)
		// wait job
		select {
		case <-time.After(leftTime):
			return context.DeadlineExceeded
		case err := <-done: // job finish
			return err
		case p := <-panicChan:
			panic(p)
		}
	case err := <-done: // job finish
		return err
	}
}

// Run start the worker
func (w *Worker) Run(task queue.QueuedMessage) error {
	data, _ := task.(queue.Job)

	if err := w.handle(data); err != nil {
		return err
	}

	return nil
}

// Shutdown worker
func (w *Worker) Shutdown() error {
	if !atomic.CompareAndSwapInt32(&w.stopFlag, 0, 1) {
		return queue.ErrQueueShutdown
	}

	w.stopOnce.Do(func() {
		// notify shtdown event to worker and consumer
		close(w.stop)
		// stop producer and consumer
		if atomic.LoadInt32(&w.startFlag) == 1 {
			w.q.ChangeMaxInFlight(0)
			w.q.Stop()
			<-w.q.StopChan
			w.p.Stop()
		}
		// close task channel
		close(w.tasks)
	})
	return nil
}

// Queue send notification to queue
func (w *Worker) Queue(job queue.QueuedMessage) error {
	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return queue.ErrQueueShutdown
	}

	err := w.p.Publish(w.opts.topic, job.Bytes())
	if err != nil {
		return err
	}

	return nil
}

// Request fetch new task from queue
func (w *Worker) Request() (queue.QueuedMessage, error) {
	clock := 0
loop:
	for {
		select {
		case task, ok := <-w.tasks:
			if !ok {
				return nil, queue.ErrQueueHasBeenClosed
			}
			var data queue.Job
			_ = json.Unmarshal(task.Body, &data)
			return data, nil
		case <-time.After(1 * time.Second):
			if clock == 5 {
				break loop
			}
			clock += 1
		}
	}

	return nil, queue.ErrNoTaskInQueue
}
