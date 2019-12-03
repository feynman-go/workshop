package easyhealth

import (
	"encoding/json"
	"fmt"
	"github.com/feynman-go/workshop/health"
	"go.uber.org/zap"
	"net/http"
)

type Endpoints struct {
	IsUp string
	Available string
	Status string
	MaxBadHighLevelCount int32
}

func BuildHttpEndpoints(manager *health.HealthManager, endpoints Endpoints, logger *zap.Logger) http.Handler {
	mx := http.NewServeMux()
	mx.HandleFunc(endpoints.Status, func(w http.ResponseWriter, r *http.Request) {
		report, err := manager.GetReport()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		bs, err := json.Marshal(report)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		_, err = w.Write(bs)
		if err != nil {
			logger.Error("write message err:", zap.Error(err))
		}
		return
	})
	mx.HandleFunc(endpoints.IsUp, func(w http.ResponseWriter, r *http.Request) {
		if manager.IsUp() {
			w.Write([]byte("OK"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})

	mx.HandleFunc(endpoints.Available, func(w http.ResponseWriter, r *http.Request) {
		if !manager.IsUp() {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("not up"))
			return
		}
		report, err := manager.GetReport()
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(err.Error()))
			return
		}

		if report.BadCritical > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(fmt.Sprintf("bad critical count: %v", report.BadCritical)))
			return
		}

		if report.BadHeighLevel > endpoints.MaxBadHighLevelCount {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(fmt.Sprintf("bad critical count: %v", report.BadHeighLevel)))
			return
		}

		w.Write([]byte("OK"))
		return
	})

	return mx
}

