package easyhealth

import (
	"context"
	"github.com/feynman-go/workshop/breaker"
	"github.com/feynman-go/workshop/health"
	"github.com/feynman-go/workshop/closes"
	"github.com/feynman-go/workshop/syncrun/routine"
)

func StartBreakerReport(bk *breaker.Breaker, reporter *health.StatusReporter) closes.WithContextCloser {
	pb := routine.New(func(ctx context.Context) {
		for ctx.Err() == nil {
			select {
			case <- bk.OnChan():
				reporter.ReportStatus("on", health.StatusUp)
				select{
				case <- ctx.Done():
				case <- bk.OffChan():
				}
			case <- bk.OffChan():
				reporter.ReportStatus("off", health.StatusDown)
				select{
				case <- ctx.Done():
				case <- bk.OnChan():
				}
			}
		}
	})

	pb.Start()
	return routine.WrapCloser(pb)
}
