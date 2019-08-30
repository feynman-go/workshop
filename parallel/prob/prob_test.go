package prob

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestProbRunAndStop(t *testing.T) {
	prob := New()

	var status int32

	go Run(prob, func(ctx context.Context) {
		atomic.StoreInt32(&status, 1)
		<- ctx.Done()
		atomic.StoreInt32(&status, 2)
	})

	prob.Start()

	time.Sleep(50 * time.Millisecond)
	if !prob.Running() {
		t.Fatal("expect prob is running")
	}
	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt32(&status) != 1 {
		t.Fatal("expect status is 1")
	}
	prob.Stop()

	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt32(&status) != 2 {
		t.Fatal("expect status is 2")
	}
	if prob.Running() {
		t.Fatal("expect prob is stopped")
	}
}

func TestProbRunAndClose(t *testing.T) {
	prob := New()

	var status int32

	go Run(prob, func(ctx context.Context) {
		atomic.StoreInt32(&status, 1)
		<- ctx.Done()
		atomic.StoreInt32(&status, 2)
	})

	prob.Start()

	time.Sleep(50 * time.Millisecond)
	if !prob.Running() {
		t.Fatal("expect prob is running")
	}
	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt32(&status) != 1 {
		t.Fatal("expect status is 1")
	}

	prob.Close()

	select {
	case <- prob.Closed():
		if atomic.LoadInt32(&status) != 2 {
			t.Fatal("expect status is 2")
		}
		if prob.Running() {
			t.Fatal("expect prob is stopped & closed")
		}
	}
}


func TestProbRunMulti(t *testing.T) {
	prob := New()

	var status int32

	go Run(prob, func(ctx context.Context) {
		atomic.StoreInt32(&status, 1)
		<- ctx.Done()
		atomic.StoreInt32(&status, 2)
	})

	for i := 0; i < 10; i++{
		prob.Start()
		time.Sleep(50 * time.Millisecond)
		if !prob.Running() {
			t.Fatal("expect prob is running")
		}
		if atomic.LoadInt32(&status) != 1 {
			t.Fatal("expect status is 1")
		}

		prob.Stop()

		time.Sleep(50 * time.Millisecond)
		if atomic.LoadInt32(&status) != 2 {
			t.Fatal("expect status is 2")
		}
		if prob.Running() {
			t.Fatal("expect prob is stopped & closed")
		}
	}


}
