package metric

import (
	"context"
	"fmt"
	"github.com/feynman-go/workshop/promise"
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

type PromMetric struct {
	Name string
	hv *prometheus.HistogramVec
	heads []string
}

func NewPromMetric(name string, heads []string) *PromMetric {
	var labels = []string{
		"promise_key", "partition", "err",
	}
	labels = append(labels, heads...)
	vec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: name,
	}, labels)

	return &PromMetric {
		Name: name,
		hv: vec,
		heads: heads,
	}
}

func (metric *PromMetric) Wrap(processFunc promise.ProcessFunc) promise.ProcessFunc {
	return promise.ProcessFunc(func(ctx context.Context, req promise.Request) promise.Result {
		var (
			start = time.Now()
			err error
			labels = prometheus.Labels{}
			res promise.Result
		)
		defer func() {
			rc := recover()
			if rc != nil {
				err = fmt.Errorf("%v", rc)
			}
			if err == nil && res.Err != nil {
				err = res.Err
			}
			if err != nil {
				labels["err"] = fmt.Sprint(err != nil)
			}
			metric.hv.With(labels).Observe(float64(time.Now().Sub(start) / time.Millisecond))
		}()

		res = processFunc(ctx, req)
		if req.Partition() {
			labels["partition"] = fmt.Sprint(req.Partition())
		}
		if req.PromiseKey() != 0 {
			labels["promise_key"] = fmt.Sprint(req.PromiseKey())
		}
		for _, h := range metric.heads {
			labels[h] = req.GetHead(h)
		}
		return res
	})
}

func (metric *PromMetric) MustRegister() {
	prometheus.MustRegister(metric.hv)
}