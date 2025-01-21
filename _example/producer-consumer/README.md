# Producer-Consumer Example

This example demonstrates how to use the `golang-queue` library with NSQ to implement a producer-consumer pattern.

## Prerequisites

- Go 1.22 or later
- [NSQ](https://nsq.io/) installed and running

## Running the Producer

The producer sends tasks to the NSQ topic.

Navigate to the producer directory:

```sh
cd _example/producer-consumer/producer
```

Run the producer:

```sh
go run main.go
```

## Running the Consumer

The consumer processes tasks from the NSQ topic.

Navigate to the consumer directory:

```sh
cd _example/producer-consumer/consumer
```

Run the consumer:

```sh
go run main.go
```

## Explanation

- The producer creates a pool of workers and assigns tasks to the queue.
- The consumer listens to the NSQ topic and processes the tasks.

Ensure that NSQ is running and accessible at `127.0.0.1:4150` before starting the producer and consumer.
