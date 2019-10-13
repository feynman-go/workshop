package httpsrv

import (
	"context"
	"github.com/feynman-go/workshop/syncrun"
	"github.com/feynman-go/workshop/syncrun/prob"
	"go.uber.org/zap"
	"net/http"
	"time"
)

type Server struct {
	srv *http.Server
	pb *prob.Prob
	logger *zap.Logger
	closeDuration time.Duration
}

func New(server *http.Server, logger *zap.Logger, closeDuration time.Duration) *Server {
	srv := &Server{
		srv: server,
		logger: logger,
		closeDuration: closeDuration,
	}
	srv.pb = prob.New(srv.run)
}

func (srv *Server) Start() {
	srv.pb.Start()
}

func (srv *Server) CloseWithContext(ctx context.Context) error {
	srv.pb.Stop()
	select {
	case <- srv.pb.Stopped():
		return nil
	case <- ctx.Done():
		return ctx.Err()
	}
}

func (srv *Server) run(ctx context.Context) {
	syncrun.RunAsGroup(ctx, func(ctx context.Context) {
		srv.logger.Info("http server start listening", zap.String("addr", srv.srv.Addr))
		err := srv.srv.ListenAndServe()
		if err != nil {
			srv.logger.Error("http server start stop", zap.String("addr", srv.srv.Addr), zap.Error(err))
		}
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
}