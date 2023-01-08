package nsq

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
	"github.com/golang-queue/queue/job"

	"github.com/nsqio/go-nsq"
)

var _ core.Worker = (*Worker)(nil)

// Worker for NSQ
type Worker struct {
	q         *nsq.Consumer
	p         *nsq.Producer
	cfg       *nsq.Config
	stopOnce  sync.Once
	startOnce sync.Once
	stop      chan struct{}
	stopFlag  int32
	opts      Options
	tasks     chan *nsq.Message
}

// NewWorker for struc
func NewWorker(opts ...Option) *Worker {
	w := &Worker{
		opts:  newOptions(opts...),
		stop:  make(chan struct{}),
		tasks: make(chan *nsq.Message),
	}

	w.cfg = nsq.NewConfig()
	w.cfg.MaxInFlight = w.opts.maxInFlight

	if err := w.startProducer(); err != nil {
		panic(err)
	}

	return w
}

func (w *Worker) startProducer() error {
	var err error

	w.p, err = nsq.NewProducer(w.opts.addr, w.cfg)

	return err
}

func (w *Worker) startConsumer() (err error) {
	w.startOnce.Do(func() {
		w.q, err = nsq.NewConsumer(w.opts.topic, w.opts.channel, w.cfg)
		if err != nil {
			return
		}

		w.q.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
			if len(msg.Body) == 0 {
				// Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
				// In this case, a message with an empty body is simply ignored/discarded.
				return nil
			}

		loop:
			for {
				select {
				case w.tasks <- msg:
					break loop
				case <-w.stop:
					if msg != nil {
						// re-queue the job if worker has been shutdown.
						msg.Requeue(-1)
					}
					break loop
				case <-time.After(2 * time.Second):
					msg.Touch()
				}
			}

			return nil
		}))

		err = w.q.ConnectToNSQD(w.opts.addr)
		if err != nil {
			return
		}
	})

	return err
}

// Run start the worker
func (w *Worker) Run(ctx context.Context, task core.QueuedMessage) error {
	return w.opts.runFunc(ctx, task)
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
		if w.q != nil {
			w.q.ChangeMaxInFlight(0)
			w.q.Stop()
			<-w.q.StopChan
		}
		w.p.Stop()

		// close task channel
		close(w.tasks)
	})
	return nil
}

// Queue send notification to queue
func (w *Worker) Queue(job core.QueuedMessage) error {
	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return queue.ErrQueueShutdown
	}

	return w.p.Publish(w.opts.topic, job.Bytes())
}

// Request fetch new task from queue
func (w *Worker) Request() (core.QueuedMessage, error) {
	if err := w.startConsumer(); err != nil {
		return nil, err
	}

	clock := 0
loop:
	for {
		select {
		case task, ok := <-w.tasks:
			if !ok {
				return nil, queue.ErrQueueHasBeenClosed
			}
			var data job.Message
			_ = json.Unmarshal(task.Body, &data)
			return &data, nil
		case <-time.After(1 * time.Second):
			if clock == 5 {
				break loop
			}
			clock += 1
		}
	}

	return nil, queue.ErrNoTaskInQueue
}

// Stats retrieves the current connection and message statistics for a Consumer
func (w *Worker) Stats() *nsq.ConsumerStats {
	if w.q == nil {
		return nil
	}

	return w.q.Stats()
}
