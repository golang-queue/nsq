package nsq

import (
	"context"
	"encoding/json"
	"runtime"
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
	startOnce sync.Once
	stopOnce  sync.Once
	stop      chan struct{}
	stopFlag  int32
	startFlag int32
	opts      options
}

func (w *Worker) incBusyWorker() {
	w.opts.metric.IncBusyWorker()
}

func (w *Worker) decBusyWorker() {
	w.opts.metric.DecBusyWorker()
}

// BusyWorkers return count of busy workers currently.
func (w *Worker) BusyWorkers() uint64 {
	return w.opts.metric.BusyWorkers()
}

// NewWorker for struc
func NewWorker(opts ...Option) *Worker {
	defaultOpts := options{
		addr:        "127.0.0.1:4150",
		topic:       "gorush",
		channel:     "ch",
		maxInFlight: runtime.NumCPU(),

		logger: queue.NewLogger(),
		runFunc: func(context.Context, queue.QueuedMessage) error {
			return nil
		},
		metric: queue.NewMetric(),
	}

	// Loop through each option
	for _, opt := range opts {
		// Call the option giving the instantiated
		opt(&defaultOpts)
	}

	w := &Worker{
		opts: defaultOpts,
		stop: make(chan struct{}),
	}

	w.startProducerAndConsumer()

	return w
}

func (w *Worker) startProducerAndConsumer() {
	if w.opts.disable {
		return
	}

	var err error
	cfg := nsq.NewConfig()
	cfg.MaxInFlight = w.opts.maxInFlight
	w.q, err = nsq.NewConsumer(w.opts.topic, w.opts.channel, cfg)
	if err != nil {
		panic(err)
	}

	w.p, err = nsq.NewProducer(w.opts.addr, cfg)
	if err != nil {
		panic(err)
	}
}

// BeforeRun run script before start worker
func (w *Worker) BeforeRun() error {
	return nil
}

// AfterRun run script after start worker
func (w *Worker) AfterRun() error {
	w.startOnce.Do(func() {
		time.Sleep(100 * time.Millisecond)
		err := w.q.ConnectToNSQD(w.opts.addr)
		if err != nil {
			panic("Could not connect nsq server: " + err.Error())
		}

		atomic.CompareAndSwapInt32(&w.startFlag, 0, 1)
	})

	return nil
}

func (w *Worker) handle(job queue.Job) error {
	// create channel with buffer size 1 to avoid goroutine leak
	done := make(chan error, 1)
	panicChan := make(chan interface{}, 1)
	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), job.Timeout)
	w.incBusyWorker()
	defer func() {
		cancel()
		w.decBusyWorker()
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
func (w *Worker) Run() error {
	wg := &sync.WaitGroup{}
	panicChan := make(chan interface{}, 1)
	w.q.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		wg.Add(1)
		defer func() {
			wg.Done()
			if p := recover(); p != nil {
				panicChan <- p
			}
		}()

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

		var data queue.Job
		_ = json.Unmarshal(msg.Body, &data)
		return w.handle(data)
	}))

	// wait close signal
	select {
	case <-w.stop:
	case err := <-panicChan:
		w.opts.logger.Error(err)
	}

	// wait job completed
	wg.Wait()

	return nil
}

// Shutdown worker
func (w *Worker) Shutdown() error {
	if !atomic.CompareAndSwapInt32(&w.stopFlag, 0, 1) {
		return queue.ErrQueueShutdown
	}

	w.stopOnce.Do(func() {
		if atomic.LoadInt32(&w.startFlag) == 1 {
			w.q.ChangeMaxInFlight(0)
			w.q.Stop()
			<-w.q.StopChan
			w.p.Stop()
		}

		close(w.stop)
	})
	return nil
}

// Capacity for channel
func (w *Worker) Capacity() int {
	return 0
}

// Usage for count of channel usage
func (w *Worker) Usage() int {
	return 0
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
