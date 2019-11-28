package httpsrv

import (
	"context"
	"errors"
	"github.com/feynman-go/workshop/health"
	"github.com/feynman-go/workshop/syncrun"
	"github.com/feynman-go/workshop/syncrun/prob"
	"go.uber.org/zap"
	"net/http"
	"time"
)

type LaunchOption struct {
	ServerName string
	CloseDuration time.Duration
	Logger *zap.Logger
	HealthReporter *health.StatusReporter
}

type Launcher struct {
	srv *http.Server
	pb *prob.Prob
	logger *zap.Logger
	closeDuration time.Duration
	healthReporter *health.StatusReporter
	name string
}

func New(server *http.Server, opt LaunchOption) *Launcher {
	if opt.Logger == nil {
		opt.Logger = zap.L()
	}
	srv := &Launcher{
		srv: server,
		logger: opt.Logger,
		closeDuration: opt.CloseDuration,
		healthReporter: opt.HealthReporter,
		name: opt.ServerName,
	}
	srv.pb = prob.New(srv.run)
	return srv
}

func (srv *Launcher) Start() {
	srv.pb.Start()
}

func (srv *Launcher) CloseWithContext(ctx context.Context) error {
	srv.pb.Stop()
	select {
	case <- srv.pb.Stopped():
		return nil
	case <- ctx.Done():
		return ctx.Err()
	}
}

func (srv *Launcher) run(ctx context.Context) {
	f := syncrun.FuncWithReStart(func(ctx context.Context) bool {
		syncrun.RunAsGroup(ctx, func(ctx context.Context) {
			srv.logger.Info("http server start listening", zap.String("addr", srv.srv.Addr), zap.String("name", srv.name))
			srv.reportStatus("start listening", health.StatusUp)

			// start serve
			err := srv.srv.ListenAndServe()
			var (
				status = health.StatusDown
				detail = "down"
			)
			if err != nil {
				if !errors.Is(err, http.ErrServerClosed) {
					status = health.StatusFatal
				}
				srv.logger.Error("http server start stop", zap.String("addr", srv.srv.Addr), zap.Error(err), zap.String("name", srv.name))
				detail = "stop: " + err.Error()
			}
			srv.reportStatus(detail, status)

		}, func(ctx context.Context) {
			select {
			case <- ctx.Done():
				dur := srv.closeDuration
				if dur == 0 {
					dur = 10 * time.Second
				}
				closeCtx, _ := context.WithTimeout(context.Background(), dur)
				err := srv.srv.Shutdown(closeCtx)
				if err != nil {
					srv.logger.Error("shut down server err", zap.String("addr", srv.srv.Addr), zap.Error(err))
				}
			}
		})
		return true
	}, syncrun.RandRestart(3 * time.Second, 6 * time.Second))

	f(ctx)
	if srv.healthReporter != nil {
		srv.healthReporter.ReportStatus("context overt", health.StatusDown)
	}
}

func (srv *Launcher) reportStatus(detail string, status health.StatusCode) {
	if srv.healthReporter != nil {
		srv.healthReporter.ReportStatus(detail, status)
	}
}