package parallel

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestParallelRoot(t *testing.T) {
	var status int32

	rt := NewRoot()
	rt.Fork(func (ctx context.Context) bool {
		atomic.StoreInt32(&status, 1)
		<- ctx.Done()
		atomic.StoreInt32(&status, 2)
		return true
	})

	rt.Start()
	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&status) != 1 {
		t.Fatal("expect status is 1")
	}

	rt.Close()

	<- rt.Closed()
}