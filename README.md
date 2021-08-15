# NSQ

[![Run Tests](https://github.com/golang-queue/nsq/actions/workflows/go.yml/badge.svg?branch=master)](https://github.com/golang-queue/nsq/actions/workflows/go.yml)
[![codecov](https://codecov.io/gh/golang-queue/nsq/branch/master/graph/badge.svg?token=V8A1WA0P5E)](https://codecov.io/gh/golang-queue/nsq)

NSQ as backend with [Queue package](https://github.com/golang-queue/queue) (A realtime distributed messaging platform)

## Setup

start the NSQ lookupd

```sh
nsqlookupd
```

start the NSQ server

```sh
nsqd --lookupd-tcp-address=localhost:4160
```

start the NSQ admin dashboard

```sh
nsqadmin --lookupd-http-address localhost:4161
```

## Testing

```sh
go test -v ./...
```
