package record

import (
	"context"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"strconv"
	"time"
	tracerLog "github.com/opentracing/opentracing-go/log"
)

func EasyRecorders(desc string, factory ...Factory) Factory {
	fs := chainFactory{}
	fs = append(fs, NewLoggerRecorderFactory(zap.L(), false, desc))
	fs = append(fs, NewPromRecorderFactory(desc))
	fs = append(fs, NewTracerFactory(opentracing.GlobalTracer()))

	fs = append(fs, factory...)
	return fs
}

type PromFactory struct {
	fields map[string]bool
	hv *prometheus.HistogramVec
}

func NewPromRecorderFactory(name string, fields ...string) *PromFactory {
	fields = append(fields, "err", "name")

	fs := make(map[string]bool)
	for _, f := range fields {
		fs[f] = true
	}

	hv := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: name,
		Help: name,
	}, fields)

	return &PromFactory{
		fields: fs,
		hv: hv,
	}
}

func (factory *PromFactory) ActionRecorder(ctx context.Context, name string, fields ...Field) (Recorder, context.Context) {
	if factory.hv == nil {
		return skipRecorder{}, ctx
	}
	return &PromRecorder{
		fields: fields,
		factory: factory,
		startTime: time.Now(),
		name: name,
	}, ctx
}

func (factory *PromFactory) buildLabel(name string, err error, fields []Field) prometheus.Labels {
	lbs := prometheus.Labels{
		"err": strconv.FormatBool(err != nil),
		"name": name,
	}
	for _, f := range fields {
		if factory.fields[f.Name] {
			lbs[f.Name] = f.StringValue()
		}
	}
	return lbs
}

func (factory *PromFactory) commit(startTime time.Time, labels prometheus.Labels) {
	factory.hv.With(labels).Observe(float64(time.Now().Sub(startTime) / time.Millisecond))
}

type PromRecorder struct {
	fields []Field
	factory *PromFactory
	startTime time.Time
	name string
}

func (recorder PromRecorder) Commit(err error, fields ...Field) {
	labels := recorder.factory.buildLabel(recorder.name, err, append(recorder.fields, fields...))
	recorder.factory.commit(recorder.startTime, labels)
}


type LoggerFactory struct {
	logger *zap.Logger
	recordNoErr bool
	desc string
}

func NewLoggerRecorderFactory(logger *zap.Logger, recordNoErr bool, messageDesc string) *LoggerFactory {
	return &LoggerFactory {
		logger: logger,
		recordNoErr: recordNoErr,
		desc: messageDesc,
	}
}

func (factory *LoggerFactory) ActionRecorder(ctx context.Context, name string, fields ...Field) (Recorder, context.Context) {
	if factory.logger == nil {
		return skipRecorder{}, ctx
	}
	return LoggerRecorder{
		fields:  fields,
		factory: factory,
		startTime: time.Now(),
		name: name,
	}, ctx
}

func (factory *LoggerFactory) commit(name string, startTime time.Time, err error, fields []Field) {
	logger := factory.logger
	if logger == nil {
		logger = zap.L()
	}
	if logger == nil {
		return
	}

	var fs []zap.Field
	if err != nil {
		fs = make([]zap.Field,0, len(fields) + 4)
		fs = append(fs, zap.Error(err))
	} else {
		fs = make([]zap.Field,0, len(fields) + 3)
	}

	fs = append(fs, zap.String("name", name))
	fs = append(fs, zap.String("duration", time.Now().Sub(startTime).String()))
	fs = append(fs, zap.Time("startTime", startTime))

	for _, f := range fields {
		fs = append(fs, zap.String(f.Name, f.StringValue()))
	}
	if err == nil && !factory.recordNoErr {
		return
	}

	if err == nil {
		factory.logger.Info(factory.desc, fs...)
	} else {
		factory.logger.Error(factory.desc, fs...)
	}
}

type LoggerRecorder struct {
	fields []Field
	factory *LoggerFactory
	startTime time.Time
	name string
}

func (recorder LoggerRecorder) Commit(err error, fields ...Field) {
	recorder.factory.commit(recorder.name, recorder.startTime, err, fields)
}

type TracerFactory struct {
	tracer opentracing.Tracer
}

func NewTracerFactory(tracer opentracing.Tracer) *TracerFactory  {
	return &TracerFactory{
		tracer: tracer,
	}
}

func (factory *TracerFactory) ActionRecorder(ctx context.Context, name string, fields ...Field) (Recorder, context.Context) {
	tracer := factory.tracer
	if tracer == nil {
		tracer = opentracing.GlobalTracer()
	}

	if tracer == nil {
		return skipRecorder{}, ctx
	}

	opt := tracerOption{
		startTime: time.Now(),
		fields: fields,
	}

	span, ctx := opentracing.StartSpanFromContextWithTracer(ctx, tracer, name, opt)

	return &TracerRecorder {
		span: span,
	}, ctx

}


type TracerRecorder struct {
	span opentracing.Span
}

func (recorder TracerRecorder) Commit(err error, fields ...Field) {
	if err != nil {
		recorder.span.SetTag("err", true)
		recorder.span.LogFields(tracerLog.Error(err))
	}
	for _, f := range fields {
		recorder.span.SetTag(f.Name, f.Value())
	}
	recorder.span.Finish()
}

type tracerOption struct {
	startTime time.Time
	fields []Field
}

func (opt tracerOption) Apply(options *opentracing.StartSpanOptions) {
	if opt.startTime.IsZero() {
		opt.startTime = time.Now()
	}
	options.StartTime = opt.startTime
	if options.Tags == nil {
		options.Tags = map[string]interface{}{}
	}
	for _, f := range opt.fields {
		options.Tags[f.Name] = f.Value()
	}
}

type skipRecorder struct {}
func (recorder skipRecorder) Commit(err error, fields ...Field) {}