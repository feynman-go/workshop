package prom

import (
	"context"
	"github.com/feynman-go/workshop/server/httpsrv"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"net/http"
	"time"
)

type Instance struct {
	exportPath    string
	exportAddr    string
	logger        *zap.Logger
	srv *httpsrv.Server
}

func New(exportAddr string, exportPath string, logger *zap.Logger, closeDuration time.Duration) *Instance {
	ins := &Instance{
		exportPath, exportAddr, logger, nil,
	}

	sm := http.NewServeMux()
	sm.Handle(ins.exportPath, promhttp.Handler())

	srv := httpsrv.New(&http.Server{
		Addr:    ins.exportAddr,
		Handler: sm,
	}, ins.logger, closeDuration)

	ins.srv = srv
	return ins
}

func (instance *Instance) Start() {
	instance.srv.Start()
}

func (instance *Instance) CloseWithContext(ctx context.Context) error {
	return instance.srv.CloseWithContext(ctx)
}