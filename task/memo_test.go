package task

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestManagerBasic(t *testing.T) {
	sch := NewMemoScheduler(10 * time.Second)

	g := &sync.WaitGroup{}
	g.Add(1)
	manager := NewManager(sch, FuncExecutor(func(cb Context) error {
		g.Done()
		return cb.Callback(cb, ExecResult{
			ResultInfo:     "",
		})
	}), 2 * time.Second)

	go func() {
		err := manager.Run(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	}()

	err := manager.ApplyNewTask(context.Background(), Desc{
		TaskKey: "1",
		ExecDesc: ExecConfig{
			ExpectStartTime: time.Now(),
			RemainExecCount:    3,
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	g.Wait()

	time.Sleep(2 * time.Second)

	task, err := sch.ReadTask(context.Background(), "1")
	if err != nil {
		t.Fatal(err)
	}
	if task != nil {
		t.Fatal("status not closed")
	}

	sm, err := sch.TaskSummery(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	t.Log(sm.StatusCount)
}

func TestManagerExpectTime(t *testing.T) {
	sch := NewMemoScheduler(1 * time.Second)

	var flag int32
	manager := NewManager(sch, FuncExecutor(func(cb Context) error {
		atomic.StoreInt32(&flag, 1)
		defer atomic.StoreInt32(&flag, 2)
		time.Sleep(1 * time.Second)
		return cb.Callback(cb, ExecResult{
			ResultInfo:     "",
		})
	}), 2 * time.Second)

	go func() {
		err := manager.Run(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	}()

	err := manager.ApplyNewTask(context.Background(), Desc{
		TaskKey: "1",
		ExecDesc: ExecConfig{
			ExpectStartTime: time.Now().Add(time.Second),
			RemainExecCount:    3,
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

	task, err := sch.ReadTask(context.Background(), "1")
	if err != nil {
		t.Fatal(err)
	}
	if task != nil {
		t.Fatal("status not closed")
	}

	sm, err := sch.TaskSummery(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	t.Log(sm.StatusCount)
}

func TestTaskRetry(t *testing.T) {
	sch := NewMemoScheduler(1 * time.Second)

	var count int32

	manager := NewManager(sch, FuncExecutor(func(cb Context) error {
		n := atomic.AddInt32(&count, 1)
		ed := ExecConfig{
			ExpectStartTime: time.Now().Add(time.Second),
			MaxExecDuration: time.Second,
			RemainExecCount: 1,
		}

		if n >= 3 {
			ed.RemainExecCount = 0
		}
		return cb.Callback(cb, ExecResult{
			ResultInfo: "",
			NextExec:   ed,
		})
	}), 2 * time.Second)

	go func() {
		err := manager.Run(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	}()

	err := manager.ApplyNewTask(context.Background(), Desc{
		TaskKey: "1",
		ExecDesc: ExecConfig{
			ExpectStartTime: time.Now(),
			RemainExecCount:    10,
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(4 * time.Second)

	if ct := atomic.LoadInt32(&count); ct != 3 {
		t.Fatal("retry count expect 3， but", ct)
	}

	task, err := sch.ReadTask(context.Background(), "1")
	if err != nil {
		t.Fatal(err)
	}
	if task != nil {
		t.Fatal("status not closed")
	}
}

