package tracer

import (
	"context"
	"github.com/feynman-go/workshop/syncrun/prob"
	opentracingHttp "github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	jaeger_config "github.com/uber/jaeger-client-go/config"
	"go.uber.org/zap"
	"net/http"
)


type Tracer struct {
	pb *prob.Prob
	config *jaeger_config.Configuration
	serviceName string
	logger *zap.Logger
}

func TracerFromEnv(serviceName string, logger *zap.Logger) *Tracer {
	config, _ := jaeger_config.FromEnv()
	tracer := &Tracer{
		config: config,
		serviceName: serviceName,
		logger: logger,
	}
	tracer.pb = prob.New(tracer.run)

}

func (tracer *Tracer) Start() {
	tracer.pb.Start()
}

func (tracer *Tracer) CloseWithContext(ctx context.Context) error {
	return tracer.pb.StopAndWait(ctx)
}


func (tracer *Tracer) run(ctx context.Context) {
	if tracer.config == nil {
		tracer.logger.Warn("tracer config is nil")
		return
	}
	closer, err := tracer.config.InitGlobalTracer(tracer.serviceName, jaeger_config.Logger(jaeger.StdLogger))
	if err != nil {
		tracer.logger.Error("InitGlobalTracer err", zap.Error(err))
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
