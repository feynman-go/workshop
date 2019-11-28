package prom

import (
	"context"
	"github.com/feynman-go/workshop/health"
	"github.com/feynman-go/workshop/server/httpsrv"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"net/http"
	"time"
)

const (
	_promCloseDuration = 3 * time.Second
)

type Instance struct {
	exportPath    string
	exportAddr    string
	logger        *zap.Logger
	srv *httpsrv.Launcher
}

type InstanceOption struct {
	Logger *zap.Logger
	HealthReporter *health.StatusReporter
}

func New(exportAddr string, exportPath string, option InstanceOption) *Instance {
	if option.Logger == nil {
		option.Logger = zap.L()
	}
	ins := &Instance{
		exportPath, exportAddr, option.Logger, nil,
	}

	sm := http.NewServeMux()
	sm.Handle(ins.exportPath, promhttp.Handler())

	srv := httpsrv.New(&http.Server{
		Addr:    ins.exportAddr,
		Handler: sm,
	}, httpsrv.LaunchOption{
		ServerName:     "prom",
		CloseDuration:  _promCloseDuration,
		Logger:         option.Logger,
		HealthReporter: option.HealthReporter,
	})

	ins.srv = srv
	return ins
}

func (instance *Instance) Start() {
	instance.srv.Start()
}

func (instance *Instance) CloseWithContext(ctx context.Context) error {
	return instance.srv.CloseWithContext(ctx)
}