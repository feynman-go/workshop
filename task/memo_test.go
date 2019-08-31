package task

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestManagerBasic(t *testing.T) {
	rep := NewMemoRepository()
	sch := NewMemoScheduler(1 * time.Second)

	g := &sync.WaitGroup{}
	g.Add(1)
	manager := NewManager(rep, sch, FuncExecutor(func(cb Context) error {
		g.Done()
		return cb.Callback(cb, ExecResult{
			ExecResultType: ExecResultTypeSuccess,
			ResultInfo:     "",
		})
	}))

	go func() {
		err := manager.Run(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	}()

	_, err := manager.ApplyNewTask(context.Background(), Desc{
		TaskKey: "1",
		Strategy: ExecStrategy{
			ExpectStartTime: time.Now(),
			MaxRetryTimes:   3,
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	g.Wait()

	time.Sleep(2 * time.Second)

	task, err := rep.ReadTask(context.Background(), "1")
	if err != nil {
		t.Fatal(err)
	}
	if task != nil {
		t.Fatal("status not closed")
	}

	sm, err := rep.GetTaskSummary(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	t.Log(sm.StatusCount)
}

func TestManagerExpectTime(t *testing.T) {
	rep := NewMemoRepository()
	sch := NewMemoScheduler(1 * time.Second)

	var flag int32
	manager := NewManager(rep, sch, FuncExecutor(func(cb Context) error {
		atomic.StoreInt32(&flag, 1)
		defer atomic.StoreInt32(&flag, 2)
		time.Sleep(2 * time.Second)
		return cb.Callback(cb, ExecResult{
			ExecResultType: ExecResultTypeSuccess,
			ResultInfo:     "",
		})
	}))

	go func() {
		err := manager.Run(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	}()

	_, err := manager.ApplyNewTask(context.Background(), Desc{
		TaskKey: "1",
		Strategy: ExecStrategy{
			ExpectStartTime: time.Now().Add(time.Second),
			MaxRetryTimes:   3,
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	var start = time.Now()
	for {
		time.Sleep(100 * time.Millisecond)
		if time.Now().After(start.Add(1 * time.Second)) {
			time.Sleep(100 * time.Millisecond)
			if atomic.LoadInt32(&flag) != 1 {
				t.Fatal("expect flag is 1")
			}
			break
		}
	}

	time.Sleep(2 * time.Second)

	task, err := rep.ReadTask(context.Background(), "1")
	if err != nil {
		t.Fatal(err)
	}
	if task != nil {
		t.Fatal("status not closed")
	}

	sm, err := rep.GetTaskSummary(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	t.Log(sm.StatusCount)
}

func TestTaskRetry(t *testing.T) {
	rep := NewMemoRepository()
	sch := NewMemoScheduler(1 * time.Second)

	var count int32

	manager := NewManager(rep, sch, FuncExecutor(func(cb Context) error {
		atomic.AddInt32(&count, 1)
		return cb.Callback(cb, ExecResult{
			ExecResultType: ExecResultTypeSuccess,
			ResultInfo:     "",
		})
	}))

	go func() {
		err := manager.Run(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	}()

	_, err := manager.ApplyNewTask(context.Background(), Desc{
		TaskKey: "1",
		Strategy: ExecStrategy{
			ExpectStartTime: time.Now(),
			MaxRetryTimes:   3,
		},
	})

	if err != nil {
		t.Fatal(err)
	}



	g.Wait()

	time.Sleep(2 * time.Second)

	task, err := rep.ReadTask(context.Background(), "1")
	if err != nil {
		t.Fatal(err)
	}
	if task != nil {
		t.Fatal("status not closed")
	}

	sm, err := rep.GetTaskSummary(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	t.Log(sm.StatusCount)
}

