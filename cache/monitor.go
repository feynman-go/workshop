package cache

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

const (
	maxUpdateDuration    = 5 * time.Hour
	checkReleaseDuration = 1 * time.Minute
)

type PrometheusMonitor struct {
	hv *prometheus.HistogramVec
}

func NewPrometheusMonitor(name string) *PrometheusMonitor {
	hv := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: name,
	}, []string{"hit", "category", "err", "downgrade"})
	return &PrometheusMonitor{
		hv: hv,
	}
}

func (monitor *PrometheusMonitor) AddFindRecord(key ResourceKey, hit bool, downgrade bool, err error, duration time.Duration) {
	v := float64(duration) / float64(time.Millisecond)
	monitor.hv.With(map[string]string{
		"hit":      fmt.Sprint(hit),
		"category": key.Category,
		"err": fmt.Sprint(err != nil),
		"downgrade": fmt.Sprint(downgrade),
	}).Observe(v)
}


func (monitor *PrometheusMonitor) Controller() prometheus.Collector {
	return monitor.hv
}
