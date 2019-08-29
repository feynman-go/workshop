package task

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestManagerBasic(t *testing.T) {
	rep := NewMemoRepository()
	sch := NewMemoScheduler()

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
		Unique: "1",
		Strategy: ExecStrategy{
			ExpectStartTime: time.Now(),
			MaxRetryTimes:   3,
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	g.Wait()
}
