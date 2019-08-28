package schedule

import (
	"context"
	"github.com/feynman-go/workshop/promise"
	"sync"
	"testing"
	"time"
)

func TestScheduleBasic(t *testing.T) {
	p := promise.NewPool(4)

	s := New(p)
	go s.Run(context.Background())

	var callTime time.Time
	var now = time.Now()
	group := &sync.WaitGroup{}
	group.Add(1)
	s.AddPlan("", now, now.Add(time.Second), func(ctx context.Context) error {
		callTime = time.Now()
		group.Done()
		return nil
	})

	group.Wait()
	if callTime.Sub(now) > 100 * time.Millisecond {
		t.Fatal("bad call time")
	}

	now = time.Now()
	group.Add(1)
	s.AddPlan("", now.Add(time.Second), now.Add(2 * time.Second), func(ctx context.Context) error {
		callTime = time.Now()
		group.Done()
		return nil
	})

	group.Wait()
	if callTime.Sub(now) > time.Second + 100 * time.Millisecond {
		t.Fatal("bad call time")
	}
}


func TestScheduleInsert(t *testing.T) {
	p := promise.NewPool(4)

	s := New(p)
	go s.Run(context.Background())

	var firstCall, secondCall time.Time
	var firstStart = time.Now()
	group := &sync.WaitGroup{}
	group.Add(1)

	s.AddPlan("", firstStart.Add(1 * time.Second), firstStart.Add(3 * time.Second), func(ctx context.Context) error {
		firstCall = time.Now()
		group.Done()
		return nil
	})

	time.Sleep(time.Millisecond * 100)

	var secondStart = time.Now()
	group.Add(1)
	s.AddPlan("", secondStart, secondStart.Add(time.Second), func(ctx context.Context) error {
		secondCall = time.Now()
		group.Done()
		return nil
	})

	group.Wait()
	if d := firstCall.Sub(firstStart); d > time.Second + 100 * time.Millisecond || d < time.Second{
		t.Fatal("bad call time", firstCall, d)
	}

	if d := secondCall.Sub(secondStart); d > 100 * time.Millisecond {
		t.Fatal("bad call time", secondCall, d)
	}
}