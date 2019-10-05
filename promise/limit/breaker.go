package limit

import (
	"context"
	"errors"
	"fmt"
	"github.com/feynman-go/workshop/breaker"
	"github.com/feynman-go/workshop/promise"
)

func WrapWithBreakerStatus(status *breaker.Status, processFunc promise.ProcessFunc) promise.ProcessFunc {
	return promise.ProcessFunc(func(ctx context.Context, req promise.Request) (res promise.Result) {
		if status.StatusCode() == breaker.StatusCodeOpening {
			return promise.Result{
				Err: errors.New("breaker status abnormal"),
			}
		}
		defer func() {
			rv := recover()
			if rv != nil {
				res.Err = fmt.Errorf("%v", rv)
			}
			if res.Err != nil {
				status.Record(true)
			} else {
				status.Record(false)
			}
		}()
		res = processFunc(ctx, req)
		return
	})
}
