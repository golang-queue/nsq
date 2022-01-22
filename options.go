package nsq

import (
	"context"
	"runtime"

	"github.com/golang-queue/queue"
)

// Option for queue system
type Option func(*options)

type options struct {
	maxInFlight int
	addr        string
	topic       string
	channel     string
	runFunc     func(context.Context, queue.QueuedMessage) error
	logger      queue.Logger
	metric      queue.Metric
	disable     bool
}

// WithAddr setup the addr of NSQ
func WithAddr(addr string) Option {
	return func(w *options) {
		w.addr = addr
	}
}

// WithTopic setup the topic of NSQ
func WithTopic(topic string) Option {
	return func(w *options) {
		w.topic = topic
	}
}

// WithChannel setup the channel of NSQ
func WithChannel(channel string) Option {
	return func(w *options) {
		w.channel = channel
	}
}

// WithRunFunc setup the run func of queue
func WithRunFunc(fn func(context.Context, queue.QueuedMessage) error) Option {
	return func(w *options) {
		w.runFunc = fn
	}
}

// WithMaxInFlight Maximum number of messages to allow in flight (concurrency knob)
func WithMaxInFlight(num int) Option {
	return func(w *options) {
		w.maxInFlight = num
	}
}

// WithLogger set custom logger
func WithLogger(l queue.Logger) Option {
	return func(w *options) {
		w.logger = l
	}
}

// WithMetric set custom Metric
func WithMetric(m queue.Metric) Option {
	return func(w *options) {
		w.metric = m
	}
}

func withDisable() Option {
	return func(w *options) {
		w.disable = true
	}
}

func newOptions(opts ...Option) options {
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

	return defaultOpts
}