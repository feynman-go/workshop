package schedule

import (
	"context"
	"math"
	"time"
)

const (
	MAX_DELAYS_DURTION = time.Duration(math.MaxInt64)
)

type Receiver interface {
	WaitAwaken(ctx context.Context) (Awaken, error)
	Commit(ctx context.Context, err error)
	CloseWithContext(ctx context.Context) error
}

type Scheduler interface {
	PostSchedule(ctx context.Context, schedule Schedule) error
}

type Awaken interface {
	Spec() Schedule
}

type Schedule struct {
	ID         string            `json:"id"`
	ExpectTime time.Time         `json:"expectTime"`
	CreateTime time.Time         `json:"startTime"`
	MaxDelays  time.Duration     `json:"maxDelays"`
	Labels     map[string]string `json:"labels"`
}