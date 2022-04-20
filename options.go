package nsq

import (
	"context"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
)

// An Option configures a mutex.
type Option interface {
	Apply(*Options)
}

// OptionFunc is a function that configures a queue.
type OptionFunc func(*Options)

// Apply calls f(option)
func (f OptionFunc) Apply(option *Options) {
	f(option)
}

type Options struct {
	maxInFlight     int
	addr            string
	topic           string
	channel         string
	runFunc         func(context.Context, core.QueuedMessage) error
	logger          queue.Logger
	disableConsumer bool
}

// WithAddr setup the addr of NSQ
func WithAddr(addr string) Option {
	return OptionFunc(func(o *Options) {
		o.addr = addr
	})
}

// WithTopic setup the topic of NSQ
func WithTopic(topic string) Option {
	return OptionFunc(func(o *Options) {
		o.topic = topic
	})
}

// WithChannel setup the channel of NSQ
func WithChannel(channel string) Option {
	return OptionFunc(func(o *Options) {
		o.channel = channel
	})
}

// WithRunFunc setup the run func of queue
func WithRunFunc(fn func(context.Context, core.QueuedMessage) error) Option {
	return OptionFunc(func(o *Options) {
		o.runFunc = fn
	})
}

// WithMaxInFlight Maximum number of messages to allow in flight (concurrency knob)
func WithMaxInFlight(num int) Option {
	return OptionFunc(func(o *Options) {
		o.maxInFlight = num
	})
}

// WithLogger set custom logger
func WithLogger(l queue.Logger) Option {
	return OptionFunc(func(o *Options) {
		o.logger = l
	})
}

// WithDisableConsumer disable consumer
func WithDisableConsumer() Option {
	return OptionFunc(func(o *Options) {
		o.disableConsumer = true
	})
}

func newOptions(opts ...Option) Options {
	defaultOpts := Options{
		addr:        "127.0.0.1:4150",
		topic:       "gorush",
		channel:     "ch",
		maxInFlight: 1,

		logger: queue.NewLogger(),
		runFunc: func(context.Context, core.QueuedMessage) error {
			return nil
		},
	}

	// Loop through each option
	for _, opt := range opts {
		// Call the option giving the instantiated
		opt.Apply(&defaultOpts)
	}

	return defaultOpts
}
