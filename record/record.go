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

type FieldType uint8

const (
	FieldTypeUnknown = 0
	FieldTypeNumber = 1
	FieldTypeString = 2
	FieldTypeBool = 3
)

func StringField(name, value string) Field {
	return Field{
		Name: name,
		Type: FieldTypeString,
		String: value,
	}
}

func NumberField(name string, number float64) Field {
	return Field{
		Name: name,
		Type: FieldTypeNumber,
		Number: number,
	}
}

func BoolField(name string, value bool) Field {
	var number float64 = 0
	if value {
		number = 1
	}
	return Field{
		Name: name,
		Type: FieldTypeBool,
		Number: number,
	}
}

type Field struct {
	Name 	string
	Type    FieldType
	Number  float64
	String  string
}

func (f Field) Value() interface{} {
	switch f.Type{
	case FieldTypeNumber:
		return f.Number
	case FieldTypeString:
		return f.String
	case FieldTypeBool:
		return f.Bool()
	default:
		return nil
	}
}

func (f Field) Bool() bool {
	return f.Number > 0
}

func (f Field) StringValue() string {
	switch f.Type {
	case FieldTypeNumber:
		return strconv.FormatFloat(f.Number, 'f', 3, 64)
	case FieldTypeString:
		return f.String
	case FieldTypeBool:
		return strconv.FormatBool(f.Number > 0)
	default:
		return ""
	}
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
