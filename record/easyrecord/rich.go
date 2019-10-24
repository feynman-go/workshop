package easyrecord

import (
	"context"
	"github.com/feynman-go/workshop/record"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/uber/jaeger-client-go"
	"go.uber.org/zap"
	"strconv"
	"time"
)

func EasyRecorders(desc string, factory ...record.Factory) record.Factory {
	fs := []record.Factory{}
	fs = append(fs, NewTracerFactory(nil))
	fs = append(fs, NewLoggerRecorderFactory(nil, desc))
	fs = append(fs, NewPromRecorderFactory(desc))
	fs = append(fs, factory...)
	return wrapperEasy{record.ChainFactory(fs...)}
}

type easyRecordKey struct {}

func EasyRecordFromContext(ctx context.Context, dft record.Factory) record.Factory {
	v := ctx.Value(easyRecordKey{})
	records, _ := v.(record.Factory)
	if records == nil {
		records = dft
	}
	return records
}

func ContextWithRecords(ctx context.Context, factory record.Factory) context.Context {
	return context.WithValue(ctx, easyRecordKey{}, factory)
}

type wrapperEasy struct {
	f record.Factory
}

func (easy wrapperEasy) ActionRecorder(ctx context.Context, name string, fields ...record.Field) (record.Recorder, context.Context) {
	if EasyRecordFromContext(ctx, nil) == nil {
		return skipRecorder{}, ContextWithRecords(ctx, easy.f)
	}
	return skipRecorder{}, ctx
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
	}, fields)

	prometheus.MustRegister(hv)
	return &PromFactory{
		fields: fs,
		hv: hv,
	}
}

func (factory *PromFactory) ActionRecorder(ctx context.Context, name string, fields ...record.Field) (record.Recorder, context.Context) {
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

func (factory *PromFactory) buildLabel(name string, err error, fields []record.Field) prometheus.Labels {
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
	fields []record.Field
	factory *PromFactory
	startTime time.Time
	name string
}

func (recorder PromRecorder) Commit(err error, fields ...record.Field) {
	labels := recorder.factory.buildLabel(recorder.name, err, append(recorder.fields, fields...))
	recorder.factory.commit(recorder.startTime, labels)
}

type LoggerFactory struct {
	logger *zap.Logger
	recordNoErr bool
	desc string
}

func NewLoggerRecorderFactory(logger *zap.Logger, messageDesc string) *LoggerFactory {
	return &LoggerFactory {
		logger: logger,
		desc: messageDesc,
	}
}

func (factory *LoggerFactory) ActionRecorder(ctx context.Context, name string, fields ...record.Field) (record.Recorder, context.Context) {
	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		spanCtx, _ := span.Context().(jaeger.SpanContext)
		if spanCtx.IsValid() {
			fields = append(fields,
				record.StringField("trace_id", spanCtx.TraceID().String()),
				record.StringField("span_id", spanCtx.SpanID().String()),
			)
		}
	}
	if factory.logger == nil && zap.L() == nil {
		return skipRecorder{}, ctx
	}
	return LoggerRecorder{
		fields:  fields,
		factory: factory,
		startTime: time.Now(),
		name: name,
	}, ctx
}

func (factory *LoggerFactory) commit(name string, startTime time.Time, err error, fields []record.Field) {
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
		fs = make([]zap.Field, 0, len(fields) + 3)
	}

	fs = append(fs, zap.String("name", name))
	fs = append(fs, zap.String("duration", time.Now().Sub(startTime).String()))
	fs = append(fs, zap.Time("startTime", startTime))

	for _, f := range fields {
		fs = append(fs, zap.String(f.Name, f.StringValue()))
	}

	if err == nil {
		logger.Info(factory.desc, fs...)
	} else {
		logger.Error(factory.desc, fs...)
	}
}

type LoggerRecorder struct {
	fields []record.Field
	factory *LoggerFactory
	startTime time.Time
	name string
}

func (recorder LoggerRecorder) Commit(err error, fields ...record.Field) {
	recorder.factory.commit(recorder.name, recorder.startTime, err, append(recorder.fields, fields...))
}

type TracerFactory struct {
	tracer opentracing.Tracer
}

func NewTracerFactory(tracer opentracing.Tracer) *TracerFactory  {
	return &TracerFactory{
		tracer: tracer,
	}
}

func (factory *TracerFactory) ActionRecorder(ctx context.Context, name string, fields ...record.Field) (record.Recorder, context.Context) {
	tracer := factory.tracer
	if tracer == nil {
		tracer = opentracing.GlobalTracer()
	}

	if tracer == nil {
		return skipRecorder{}, ctx
	}

	span, ctx := opentracing.StartSpanFromContextWithTracer(ctx, tracer, name)
	return &TracerRecorder {
		fields: fields,
		span: span,
	}, ctx
}


type TracerRecorder struct {
	fields []record.Field
	span opentracing.Span
}

func (recorder TracerRecorder) Commit(err error, fields ...record.Field) {
	for _, f := range recorder.fields {
		recorder.span.SetTag(f.Name, f.Value())
	}
	for _, f := range fields {
		recorder.span.SetTag(f.Name, f.Value())
	}
	if err != nil {
		recorder.span.SetTag("err", err.Error())
	}
	recorder.span.Finish()
}

type skipRecorder struct {}
func (recorder skipRecorder) Commit(err error, fields ...record.Field) {}