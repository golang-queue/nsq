# NSQ

[![Run CI Lint](https://github.com/golang-queue/nsq/actions/workflows/lint.yml/badge.svg)](https://github.com/golang-queue/nsq/actions/workflows/lint.yml)
[![Run Testing](https://github.com/golang-queue/nsq/actions/workflows/testing.yml/badge.svg)](https://github.com/golang-queue/nsq/actions/workflows/testing.yml)
[![codecov](https://codecov.io/gh/golang-queue/nsq/branch/main/graph/badge.svg?token=D3CUES8M62)](https://codecov.io/gh/golang-queue/nsq)

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
