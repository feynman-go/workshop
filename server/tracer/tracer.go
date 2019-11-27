package tracer

import (
	"context"
	"errors"
	"fmt"
	"github.com/feynman-go/workshop/healthcheck"
	opentracingHttp "github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	jaeger_config "github.com/uber/jaeger-client-go/config"
	"go.uber.org/zap"
	"io"
	"net/http"
	"sync"
)


type Tracer struct {
	config *jaeger_config.Configuration
	serviceName string
	logger *zap.Logger
	reporter *health.StatusReporter
	closer io.Closer
	rw sync.RWMutex
}

func TracerFromEnv(serviceName string, logger *zap.Logger, reporter *health.StatusReporter) *Tracer {
	config, _ := jaeger_config.FromEnv()
	tracer := &Tracer {
		config: config,
		serviceName: serviceName,
		logger: logger,
		reporter: reporter,
	}
	return tracer
}

func (tracer *Tracer) Start(ctx context.Context) error {
	tracer.rw.Lock()
	defer tracer.rw.Unlock()

	if tracer.config == nil {
		tracer.reporter.ReportStatus("empty config", health.StatusFatal)
		return errors.New("tracer config is nil")
	}

	if tracer.closer != nil {
		closer, err := tracer.config.InitGlobalTracer(tracer.serviceName, jaeger_config.Logger(jaeger.StdLogger))
		if err != nil {
			err = fmt.Errorf("init tracer: %w", err)
			tracer.reporter.ReportStatus(err.Error(), health.StatusFatal)
			return err
		}
		tracer.closer = closer
		tracer.reporter.ReportStatus("up", health.StatusOk)
	}

	return nil
}

func (tracer *Tracer) CloseWithContext(ctx context.Context) error {
	tracer.rw.Lock()
	defer tracer.rw.Unlock()
	if tracer.closer != nil {
		if tracer.reporter != nil {
			tracer.reporter.ReportStatus("down", health.StatusDown)
		}
		return tracer.closer.Close()
	}
	return nil
}


func (tracer *Tracer) run(ctx context.Context) {
	var err error
	func() {
		var (
			detail = "context down"
			statusCode = health.StatusDown
		)
		if err != nil {
			detail = err.Error()
			statusCode = health.StatusFatal
			tracer.logger.Error("run tracer", zap.Error(err))
		}
		defer tracer.reporter.ReportStatus(detail, statusCode)
	}()

	var (
		closer io.Closer
	)

	if tracer.config == nil {
		tracer.logger.Warn("tracer config is nil")
		return
	}
	closer, err = tracer.config.InitGlobalTracer(tracer.serviceName, jaeger_config.Logger(jaeger.StdLogger))
	if err != nil {

		err = fmt.Errorf("init tracer: %w", err)
		return
	}

	select {
	case <- ctx.Done():
		if closer != nil {
			closer.Close()
		}
		return
	}
}

func GetHttpOpenTracingOptions(componentName string) []opentracingHttp.MWOption {
	opentracingHttpOptions := []opentracingHttp.MWOption{
		opentracingHttp.MWComponentName(componentName),
		opentracingHttp.OperationNameFunc(func(r *http.Request) string {
			return r.Method + ":" + r.RequestURI
		}),
	}
	return opentracingHttpOptions
}

func TracingHttpHandler(componentName string, handler http.Handler) http.Handler {
	handler = opentracingHttp.Middleware(opentracing.GlobalTracer(), handler, GetHttpOpenTracingOptions(componentName)...)
	return handler
}
