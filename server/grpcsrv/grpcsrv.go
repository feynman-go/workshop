package grpcsrv

import (
	"context"
	"errors"
	"fmt"
	"github.com/feynman-go/workshop/health"
	"github.com/feynman-go/workshop/syncrun"
	"github.com/feynman-go/workshop/syncrun/prob"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
	"sync"
	"time"
)

type GrpcLauncher struct {
	listenerAddr   string
	server         *grpc.Server
	rw             sync.RWMutex
	pb             *routine.Prob
	statusReporter *health.StatusReporter
	logger *zap.Logger
}

func NewGrpcLauncher(listenerAddr string, server *grpc.Server, statusReporter *health.StatusReporter, logger *zap.Logger) *GrpcLauncher {
	return &GrpcLauncher{
		listenerAddr:   listenerAddr,
		server:         server,
		rw:             sync.RWMutex{},
		statusReporter: statusReporter,
		logger: logger,
	}
}


func (r *GrpcLauncher) Start() error {
	r.rw.Lock()
	defer r.rw.Unlock()

	if r.pb != nil {
		return errors.New("err")
	}

	r.pb = routine.New(r.runLoop)
	r.pb.Start()
	return nil
}

func (r *GrpcLauncher) runLoop(ctx context.Context) {
	runFunc := syncrun.FuncWithReStart(func(ctx context.Context) bool {
		lis, err := net.Listen("tcp", r.listenerAddr)
		if err != nil {
			r.reportStatus(fmt.Sprintf("start listener err: %v", err), health.StatusFatal)
			return true
		}

		var runCtx, cancel = context.WithCancel(ctx)
		go func(ctx context.Context) {
			select {
			case <- ctx.Done():
				r.server.GracefulStop()
			}
		}(runCtx)
		r.reportStatus("start serve", health.StatusUp)
		err = r.server.Serve(lis)
		r.logger.Error("server serve return", zap.Error(err))
		r.reportStatus(fmt.Sprintf("serve stoped: %v", err), health.StatusFatal)

		cancel()
		err = lis.Close()
		if err != nil {
			r.logger.Error("stop listener err", zap.Error(err))
		}
		return true
	}, syncrun.RandRestart(2 * time.Second, 5 * time.Second))

	runFunc(ctx)
	r.reportStatus("server down", health.StatusDown)
}

func (r *GrpcLauncher) reportStatus(detail string, code health.StatusCode) {
	if r.statusReporter != nil {
		r.statusReporter.ReportStatus(detail, code)
	}
}

func (r *GrpcLauncher) Close() error {
	r.pb.Stop()
	return nil
}

