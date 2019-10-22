package record

import (
	"context"
	"github.com/pkg/errors"
	"strconv"
	"sync/atomic"
	"time"
)

type Factory interface {
	ActionRecorder(ctx context.Context, name string, fields ...Field) (Recorder, context.Context)
}

type Recorder interface {
	Commit(err error, fields ...Field)
}

type FileValue interface {
	Marshal() string
	Value() interface{}
}

func StringField(name, value string) Field {
	return Field{
		Name: name,
		value: stringField(value),
	}
}

func IntegerField(name string, value int64) Field {
	return Field{
		Name: name,
		value: intField(value),
	}
}

func NumberField(name string, number float64) Field {
	return Field{
		Name: name,
		value: numberField(number),
	}
}

func BoolField(name string, value bool) Field {
	return Field{
		Name: name,
		value: boolField(value),
	}
}

type Field struct {
	Name 	string
	value   FileValue
}

func (f Field) Value() interface{} {
	return f.value.Value()
}

func (f Field) StringValue() string {
	return f.value.Marshal()
}

type FactoryWrapper struct {
	Wrapper
	Factory Factory
}

func (mr FactoryWrapper) ActionRecorder(ctx context.Context, name string, fields ...Field) (Recorder, context.Context) {
	ar, ctx := mr.ActionRecorder(ctx, name)
	if mr.Wrapper != nil {
		ar, ctx = mr.Wrapper.WrapRecord(ctx, name, ar)
	}
	return ar, ctx
}

type Wrapper interface {
	WrapRecord(ctx context.Context, name string, record Recorder) (Recorder, context.Context)
}

type MaxDurationWrapper struct {
	MaxDuration time.Duration
}

func (wrap MaxDurationWrapper) WrapRecord(ctx context.Context, name string, record Recorder) (Recorder, context.Context) {
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
				record.Commit(errors.New("over max time"))
				close(cn)
			}
		}
	}(ctx, &rd.commitFlag, cn)
	return rd, ctx
}

type maxDurationRecorder struct {
	commitFlag int32
	inner      Recorder
	cn  	   chan struct{}
}

func (recorder *maxDurationRecorder) Commit(err error, fields ...Field) {
	if atomic.CompareAndSwapInt32(&recorder.commitFlag, 0, 1) {
		recorder.inner.Commit(err, fields...)
		close(recorder.cn)
	}
}

func ChainFactory(factorys ...Factory) Factory {
	return chainFactory(factorys)
}

type chainFactory []Factory

func (cf chainFactory) ActionRecorder(ctx context.Context, name string, fields ...Field) (Recorder, context.Context) {
	var retCtx = ctx
	records := make([]Recorder, 0, len(cf))
	var rd Recorder
	for _, f := range cf {
		rd, retCtx = f.ActionRecorder(retCtx, name, fields...)
		records = append(records, rd)
	}
	return chainRecorder(records), ctx
}

type chainRecorder []Recorder

func (cr chainRecorder) Commit(err error, fields ...Field) {
	for _, rd := range cr {
		rd.Commit(err, fields...)
	}
}

type intField int64

func (f intField) Marshal() string {
	return strconv.FormatInt(int64(f), 64)
}

func (f intField) Value() interface{} {
	return int64(f)
}

type stringField string

func (f stringField) Marshal() string {
	return string(f)
}

func (f stringField) Value() interface{} {
	return string(f)
}

type numberField float64

func (f numberField) Marshal() string {
	return strconv.FormatFloat(float64(f), 'e', -1, 64)
}

func (f numberField) Value() interface{} {
	return float64(f)
}

type boolField bool

func (f boolField) Marshal() string {
	return strconv.FormatBool(bool(f))
}

func (f boolField) Value() interface{} {
	return bool(f)
}


