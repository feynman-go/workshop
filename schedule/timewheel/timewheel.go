package timewheel

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrOverMaxDuration = errors.New("err over max duration")

type TimeWheel struct {
	rw sync.RWMutex
	tasks []taskEntry
	curIndex int
	curMs int64
	tickMs int64
}

func NewTimeWheel(tickMs int64) *TimeWheel {
	return &TimeWheel{
		tasks:    nil,
		curIndex: 0,
		curMs:    0,
		tickMs:   tickMs,
	}
}

type TimerTask struct {
	ExpectUnixMs int64
	Key string
	Payload interface{}
}

func (wheel *TimeWheel) AddTask(task TimerTask) error {
	wheel.rw.Lock()
	defer wheel.rw.Unlock()

	offset := wheel.getTaskOffset(task)
	if offset < 0 {
		return ErrOverMaxDuration
	}
	wheel.insertTask(offset, task)
	return nil
}

func (wheel *TimeWheel) getTaskOffset(task TimerTask) int {
	delta := task.ExpectUnixMs - wheel.curMs
	if delta < 0 {
		// add in current
		return 0
	}

	capability := wheel.tickMs * int64(len(wheel.tasks))
	if delta >= capability {
		return -1
	}

	return int(delta / wheel.tickMs)
}

func (wheel *TimeWheel) insertTask(offset int, task TimerTask) {
	l := len(wheel.tasks)
	entry := wheel.tasks[(wheel.curIndex + offset) % l]
	entry.SetTask(task)
}

func (wheel *TimeWheel) WaitAvailableTask(ctx context.Context) ([]TimerTask, error){
	tm := time.NewTimer(0)
	for ctx.Err() == nil {
		select {
		case <- ctx.Done():
			return nil, nil
		case <- tm.C:
			tasks := wheel.proceed(ctx)
			if len(tasks) != 0 {
				return tasks, nil
			}
		}
	}
	return nil, nil
}

func (wheel *TimeWheel) proceed(ctx context.Context) []TimerTask {
	now := time.Now().UnixNano() / int64(time.Millisecond)
	tasks := []TimerTask{}

	wheel.rw.Lock()
	defer wheel.rw.Unlock()

	for wheel.curMs + wheel.tickMs <= now {
		entry := wheel.moveNext()
		tasks := make([]TimerTask, 0, len(entry.tasks))
		for _, t := range entry.tasks {
			tasks = append(tasks, t)
		}
	}
	return tasks
}

func (wheel *TimeWheel) indexByOffset(offset int) int {
	return (wheel.curIndex + offset) % len(wheel.tasks)
}

func (wheel *TimeWheel) moveNext() taskEntry {
	curIndex := wheel.curIndex
	entry := wheel.tasks[curIndex]
	nextIndex := wheel.indexByOffset(1)
	wheel.curIndex = nextIndex
	wheel.curMs += wheel.tickMs
	return entry
}


type taskEntry struct {
	tasks map[string]TimerTask
}

func (entry taskEntry) SetTask(task TimerTask) {
	entry.tasks[task.Key] = task
}
