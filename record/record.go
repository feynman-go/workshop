package record

import (
	"context"
	"github.com/pkg/errors"
	"sync/atomic"
	"time"
)

type Recorder interface {
	Commit(err error, labels Labels)
}

type Labels map[string]string

type Factory interface {
	ActionRecorder(ctx context.Context, name string) Recorder
}

type WrapFactory struct {
	Wrapper
	Factory Factory
}

func (mr WrapFactory) ActionRecorder(ctx context.Context, name string) Recorder {
	ar := mr.ActionRecorder(ctx, name)
	if mr.Wrapper != nil {
		ar = mr.Wrapper.WrapRecord(ctx, name, ar)
	}
	return ar
}

type Wrapper interface {
	WrapRecord(ctx context.Context, name string, record Recorder) Recorder
}

type MaxDurationWrap struct {
	MaxDuration time.Duration
}

func (wrap MaxDurationWrap) WrapRecord(ctx context.Context, name string, record Recorder) Recorder {
	cn := make(chan struct{})
	rd := &maxDurationRecorder {
		cn: cn,
	}
	go func(ctx context.Context, committed *int32, cn chan struct{}) {
		if wrap.MaxDuration != 0 {
			ctx, _ = context.WithTimeout(ctx, wrap.MaxDuration)
		}

		timer := time.NewTimer(wrap.MaxDuration)
		defer timer.Stop()
		select {
		case <- cn:
		case <- ctx.Done():
			if atomic.CompareAndSwapInt32(committed, 0, 1) {
				record.Commit(errors.New("over max time"), nil)
				close(cn)
			}
		}
	}(ctx, &rd.commitFlag, cn)
	return rd
}

type maxDurationRecorder struct {
	commitFlag int32
	inner      Recorder
	cn  	   chan struct{}
}

func (recorder *maxDurationRecorder) Commit(err error, labels Labels) {
	if atomic.CompareAndSwapInt32(&recorder.commitFlag, 0, 1) {
		recorder.inner.Commit(err, labels)
		close(recorder.cn)
	}
}

func ChainFactory(factorys ...Factory) Factory {
	return chainFactory(factorys)
}

type chainFactory []Factory

func (cf chainFactory) ActionRecorder(ctx context.Context, name string) Recorder {
	records := make([]Recorder, 0, len(cf))
	for _, f := range cf {
		records = append(records, f.ActionRecorder(ctx, name))
	}
	return chainRecorder(records)
}

type chainRecorder []Recorder

func (cr chainRecorder) Commit(err error, labels Labels) {
	for _, rd := range cr {
		rd.Commit(err, labels)
	}
}
