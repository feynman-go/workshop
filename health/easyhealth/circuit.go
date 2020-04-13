package easyhealth

import (
	"context"
	"github.com/feynman-go/workshop/breaker/circuit"
	"github.com/feynman-go/workshop/health"
	"github.com/feynman-go/workshop/closes"
	"github.com/feynman-go/workshop/syncrun/routine"
)

func StartCircuitReport(cc *circuit.Circuit, reporter *health.StatusReporter) closes.WithContextCloser {
	pb := routine.New(func(ctx context.Context) {
		defer func() {
			reporter.ReportStatus("", health.StatusDown)
		}()
		for ctx.Err() == nil {
			status := cc.Status()
			switch status {
			case circuit.STATUS_WAITING_RECOVERY:
				reporter.ReportStatus("", health.StatusAbnormal)
			case circuit.STATUS_OPEN:
				reporter.ReportStatus("", health.StatusOk)
			}
			select {
			case <- cc.StatusChange():
			case <- ctx.Done():
			}
		}
	})

	pb.Start()
	return routine.WrapCloser(pb)
}

