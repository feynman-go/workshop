package cache

import "time"

type Tracers interface {
	OpenTracer(error) Span
}

const (
	ReadFromCache     ReadProcessType = 1
	ReadFromStore     ReadProcessType = 1
	ReadFromDowngrade ReadProcessType = 1
)

type ReadProcessType int

type Span struct {
	Hit             bool
	ReadProcessType ReadProcessType
	CacheRead       time.Duration
	StoreRead       time.Duration
	DowngradeRead   time.Duration
	WaitDuration    time.Duration
}
