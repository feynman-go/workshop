package tracer

import (
	"context"
	"github.com/feynman-go/workshop/promise"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
)

type OpenTracer struct {
	Name       string
	RecordHead bool
}

func (tracer *OpenTracer) Wrap(processFunc promise.ProcessFunc) promise.ProcessFunc {
	return promise.ProcessFunc(func(ctx context.Context, req promise.Request) promise.Result {
		sp, ctx := opentracing.StartSpanFromContext(ctx, tracer.Name)
		defer sp.Finish()
		res := processFunc(ctx, req)
		if req.Partition() {
			sp.LogFields(
				log.Bool("partition", req.Partition()),
			)
		}
		if req.PromiseKey() != 0 {
			sp.LogFields(
				log.Int("promise_key", req.PromiseKey()),
			)
		}
		if tracer.RecordHead {
			req.RangeHead(func(k, v string) bool {
				sp.LogFields(
					log.String(k, v),
				)
				return true
			})
		}
		return res
	})
}
