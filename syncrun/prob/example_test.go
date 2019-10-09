package prob

import (
	"context"
	"time"
)

func ExampleBasicUse() {
	// 创建一个探针
	pb := New(func(ctx context.Context) {
		// do something cost long time
		select {
		case <- ctx.Done():
		case <- time.After(time.Minute):
		}
	})

	pb.Start()
	// do something else

	// stop will cancel context
	pb.Stop()

	// wait goroutine stop
	<- pb.Stopped()
}
