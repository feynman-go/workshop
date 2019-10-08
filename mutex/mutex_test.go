package mutex

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMutexHoldAndRelease(t *testing.T) {
	mx := &Mutex{}
	if !mx.Hold(context.Background()) {
		t.Fatal("hold mutex err")
	}

	ctx, _ := context.WithTimeout(context.Background(), 100 * time.Millisecond)

	if mx.Hold(ctx) {
		t.Fatal("hold mutex should failed")
	}

	mx.Release()
	if !mx.Hold(context.Background()) {
		t.Fatal("hold mutex failed")
	}
}

func TestMutexWaitAndHold(t *testing.T) {
	mx := &Mutex{}
	if !mx.Hold(context.Background()) {
		t.Fatal("hold mutex err")
	}

	func () {
		time.Sleep(100 * time.Millisecond)
		mx.Release()
	}()

	ctx, _ := context.WithTimeout(context.Background(), 200 * time.Millisecond)

	if !mx.Hold(ctx) {
		t.Fatal("hold mutex failed")
	}
}

func TestMutexHoldOnMultiRoutine(t *testing.T) {
	mx := &Mutex{}
	var count int32= 0
	group := &sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		group.Add(1)
		go func() {
			ctx, _ := context.WithTimeout(context.Background(), 100 * time.Millisecond)
			if mx.Hold(ctx) {
				atomic.AddInt32(&count, 1)
			}
			group.Done()
		}()
	}
	group.Wait()
	if count != 1 {
		t.Fatal("bad hold count", count)
	}
}

func TestMutexHoldReleaseOnMultiRoutine(t *testing.T) {
	mx := &Mutex{}
	var count int32= 0
	group := &sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		group.Add(1)
		go func(i int) {
			var startTime = time.Now()
			if mx.Hold(context.Background()) {
				atomic.AddInt32(&count, 1)
				mx.Release()
			}
			if time.Now().Sub(startTime) > 100 *time.Millisecond {
				t.Fatal("hold/release cost too many time", i)
			}
			group.Done()
		}(i)
	}
	group.Wait()
	if count != 100 {
		t.Fatal("bad hold count", count)
	}
}
