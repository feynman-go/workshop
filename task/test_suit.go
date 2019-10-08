package task

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type ScheduleTestSuit struct {
	New func() Scheduler
}

func (suit *ScheduleTestSuit) TestSchedulerBasic(t *testing.T) {
	ctx := context.Background()
	scheduler := suit.New()

	newTask := Task{
		Key: "task_key",
		Stage: 1,
		Schedule: Schedule{
			AwakenTime: time.Now(),
			CompensateDuration: time.Second * 2,
		},
		Meta: Meta {

		},
		Execution: Execution{

		},
	}

	err := scheduler.ScheduleTask(ctx, newTask, true)
	if err != nil {
		t.Fatal(err)
	}

	var awaken Awaken
	if !checkInDuration(100 * time.Millisecond, 0, func() {
		awaken, err = scheduler.WaitTaskAwaken(ctx)
	}) {
		t.Fatal("awaken time out")
	}

	if newTask.Key != awaken.TaskKey {
		t.Fatal("bad task")
	}

	if !checkInDuration(2 * time.Second + 100 * time.Millisecond, 2 * time.Second, func() {
		awaken, err = scheduler.WaitTaskAwaken(ctx)
	}) {
		t.Fatal("awaken time out")
	}
}

func (suit *ScheduleTestSuit) TestSchedulerOverWriteTime(t *testing.T) {
	ctx := context.Background()
	scheduler := suit.New()

	newTask := Task{
		Key: "task_key",
		Stage: 1,
		Schedule: Schedule{
			AwakenTime: time.Now().Add(time.Second),
			CompensateDuration: time.Second * 2,
		},
		Meta: Meta{

		},
		Execution: Execution{

		},
	}

	err := scheduler.ScheduleTask(ctx, newTask, true)
	if err != nil {
		t.Fatal(err)
	}

	newTask.Schedule.AwakenTime = time.Now().Add(2 * time.Second)

	err = scheduler.ScheduleTask(ctx, newTask, true)
	if err != nil {
		t.Fatal(err)
	}

	var awaken Awaken
	if !checkInDuration(2 * time.Second + 100 * time.Millisecond, 2 * time.Second - 100 * time.Millisecond, func() {
		awaken, err = scheduler.WaitTaskAwaken(ctx)
	}) {
		t.Fatal("awaken time out")
	}

	if newTask.Key != awaken.TaskKey {
		t.Fatal("bad task")
	}
}

func compareTask(from, to Task) error {
	if from.Key != to.Key {
		return fmt.Errorf("key not equal")
	}
	return nil
}

func checkInDuration(maxDuration, minDuration time.Duration, f func()) bool {
	var wait = make(chan struct{})
	start := time.Now()
	go func() {
		f()
		close(wait)
	}()

	select {
	case <- wait:
	case <- time.After(maxDuration):
		return false
	}
	delta := time.Now().Sub(start)
	return delta <= maxDuration && delta >= minDuration
}
