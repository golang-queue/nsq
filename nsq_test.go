package nsq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"testing"
	"time"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

var host = "127.0.0.1"

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type mockMessage struct {
	Message string
}

func (m mockMessage) Bytes() []byte {
	return []byte(m.Message)
}

func TestNSQDefaultFlow(t *testing.T) {
	m := &mockMessage{
		Message: "foo",
	}
	w := NewWorker(
		WithAddr(host+":4150"),
		WithTopic("test1"),
		WithChannel("test1"),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	assert.NoError(t, q.Queue(m))
	m.Message = "bar"
	assert.NoError(t, q.Queue(m))
	time.Sleep(1 * time.Second)
	q.Release()
}

func TestNSQShutdown(t *testing.T) {
	w := NewWorker(
		WithAddr(host+":4150"),
		WithTopic("test2"),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(1 * time.Second)
	q.Shutdown()
	// check shutdown once
	assert.Error(t, w.Shutdown())
	assert.Equal(t, queue.ErrQueueShutdown, w.Shutdown())
	q.Wait()
}

func TestNSQCustomFuncAndWait(t *testing.T) {
	m := &mockMessage{
		Message: "foo",
	}
	w := NewWorker(
		WithAddr(host+":4150"),
		WithTopic("test3"),
		WithMaxInFlight(10),
		WithRunFunc(func(ctx context.Context, m core.QueuedMessage) error {
			time.Sleep(500 * time.Millisecond)
			return nil
		}),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(10),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(100 * time.Millisecond)
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	time.Sleep(1000 * time.Millisecond)
	q.Release()
	// you will see the execute time > 1000ms
}

func TestEnqueueJobAfterShutdown(t *testing.T) {
	m := mockMessage{
		Message: "foo",
	}
	w := NewWorker(
		WithAddr(host + ":4150"),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(400 * time.Millisecond)
	q.Shutdown()
	// can't queue task after shutdown
	err = q.Queue(m)
	assert.Error(t, err)
	assert.Equal(t, queue.ErrQueueShutdown, err)
	q.Wait()
}

func TestJobReachTimeout(t *testing.T) {
	m := mockMessage{
		Message: "foo",
	}
	w := NewWorker(
		WithAddr(host+":4150"),
		WithTopic("timeout"),
		WithMaxInFlight(2),
		WithRunFunc(func(ctx context.Context, m core.QueuedMessage) error {
			for {
				select {
				case <-ctx.Done():
					log.Println("get data:", string(m.Bytes()))
					if errors.Is(ctx.Err(), context.Canceled) {
						log.Println("queue has been shutdown and cancel the job")
					} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
						log.Println("job deadline exceeded")
					}
					return nil
				default:
				}
				time.Sleep(50 * time.Millisecond)
			}
		}),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(400 * time.Millisecond)
	assert.NoError(t, q.QueueWithTimeout(20*time.Millisecond, m))
	time.Sleep(2 * time.Second)
	q.Release()
}

func TestCancelJobAfterShutdown(t *testing.T) {
	m := mockMessage{
		Message: "test",
	}
	w := NewWorker(
		WithAddr(host+":4150"),
		WithTopic("cancel"),
		WithLogger(queue.NewLogger()),
		WithRunFunc(func(ctx context.Context, m core.QueuedMessage) error {
			for {
				select {
				case <-ctx.Done():
					log.Println("get data:", string(m.Bytes()))
					if errors.Is(ctx.Err(), context.Canceled) {
						log.Println("queue has been shutdown and cancel the job")
					} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
						log.Println("job deadline exceeded")
					}
					return nil
				default:
				}
				time.Sleep(50 * time.Millisecond)
			}
		}),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(400 * time.Millisecond)
	assert.NoError(t, q.QueueWithTimeout(3*time.Second, m))
	time.Sleep(2 * time.Second)
	q.Release()
}

func TestGoroutineLeak(t *testing.T) {
	m := mockMessage{
		Message: "foo",
	}
	w := NewWorker(
		WithAddr(host+":4150"),
		WithTopic("GoroutineLeak"),
		WithLogger(queue.NewEmptyLogger()),
		WithRunFunc(func(ctx context.Context, m core.QueuedMessage) error {
			for {
				select {
				case <-ctx.Done():
					log.Println("get data:", string(m.Bytes()))
					if errors.Is(ctx.Err(), context.Canceled) {
						log.Println("queue has been shutdown and cancel the job")
					} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
						log.Println("job deadline exceeded")
					}
					return nil
				default:
					log.Println("get data:", string(m.Bytes()))
					time.Sleep(50 * time.Millisecond)
					return nil
				}
			}
		}),
	)
	q, err := queue.NewQueue(
		queue.WithLogger(queue.NewEmptyLogger()),
		queue.WithWorker(w),
		queue.WithWorkerCount(10),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(400 * time.Millisecond)
	for i := 0; i < 500; i++ {
		m.Message = fmt.Sprintf("foobar: %d", i+1)
		assert.NoError(t, q.Queue(m))
	}
	time.Sleep(2 * time.Second)
	q.Release()
	time.Sleep(2 * time.Second)
	fmt.Println("number of goroutines:", runtime.NumGoroutine())
}

func TestGoroutinePanic(t *testing.T) {
	m := mockMessage{
		Message: "foo",
	}
	w := NewWorker(
		WithAddr(host+":4150"),
		WithTopic("GoroutinePanic"),
		WithRunFunc(func(ctx context.Context, m core.QueuedMessage) error {
			panic("missing something")
		}),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(400 * time.Millisecond)
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	time.Sleep(2 * time.Second)
	q.Shutdown()
	assert.Error(t, q.Queue(m))
	q.Wait()
}

func TestHandleTimeout(t *testing.T) {
	job := queue.Job{
		Timeout: 100 * time.Millisecond,
		Payload: []byte("foo"),
	}
	w := NewWorker(
		WithRunFunc(func(ctx context.Context, m core.QueuedMessage) error {
			time.Sleep(200 * time.Millisecond)
			return nil
		}),
		WithDisableConsumer(),
	)

	err := w.handle(job)
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)

	job = queue.Job{
		Timeout: 150 * time.Millisecond,
		Payload: []byte("foo"),
	}

	w = NewWorker(
		WithRunFunc(func(ctx context.Context, m core.QueuedMessage) error {
			time.Sleep(200 * time.Millisecond)
			return nil
		}),
		WithDisableConsumer(),
	)

	done := make(chan error)
	go func() {
		done <- w.handle(job)
	}()

	assert.NoError(t, w.Shutdown())

	err = <-done
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
}

func TestJobComplete(t *testing.T) {
	job := queue.Job{
		Timeout: 100 * time.Millisecond,
		Payload: []byte("foo"),
	}
	w := NewWorker(
		WithRunFunc(func(ctx context.Context, m core.QueuedMessage) error {
			return errors.New("job completed")
		}),
		WithDisableConsumer(),
	)

	err := w.handle(job)
	assert.Error(t, err)
	assert.Equal(t, errors.New("job completed"), err)

	job = queue.Job{
		Timeout: 250 * time.Millisecond,
		Payload: []byte("foo"),
	}

	w = NewWorker(
		WithRunFunc(func(ctx context.Context, m core.QueuedMessage) error {
			time.Sleep(200 * time.Millisecond)
			return errors.New("job completed")
		}),
	)

	done := make(chan error)
	go func() {
		done <- w.handle(job)
	}()

	assert.NoError(t, w.Shutdown())

	err = <-done
	assert.Error(t, err)
	assert.Equal(t, errors.New("job completed"), err)
}

func TestNSQStatsinQueue(t *testing.T) {
	m := mockMessage{
		Message: "foo",
	}
	w := NewWorker(
		WithAddr(host+":4150"),
		WithTopic("nsq_stats"),
		WithRunFunc(func(ctx context.Context, m core.QueuedMessage) error {
			log.Println("get message")
			return nil
		}),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(1),
	)
	assert.NoError(t, err)
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	q.Start()
	assert.Equal(t, int(1), w.Stats().Connections)
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, uint64(2), w.Stats().MessagesReceived)
	assert.Equal(t, uint64(2), w.Stats().MessagesFinished)
	q.Release()
	assert.Equal(t, int(0), w.Stats().Connections)
}

func TestNSQStatsInWorker(t *testing.T) {
	m := mockMessage{
		Message: "foo",
	}
	w := NewWorker(
		WithAddr(host+":4150"),
		WithTopic("nsq_stats_queue"),
	)

	assert.Equal(t, int(0), len(w.tasks))
	assert.NoError(t, w.Queue(m))
	assert.NoError(t, w.Queue(m))
	assert.NoError(t, w.Queue(m))
	assert.Equal(t, int(1), w.Stats().Connections)
	assert.Equal(t, int(1), len(w.tasks))

	time.Sleep(300 * time.Millisecond)

	assert.Equal(t, uint64(3), w.Stats().MessagesReceived)
	assert.Equal(t, uint64(1), w.Stats().MessagesFinished)
	assert.Equal(t, uint64(0), w.Stats().MessagesRequeued)

	_ = w.Shutdown()
}
