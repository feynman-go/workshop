package task

import (
	"context"
	"errors"
	"sync"
	"time"
)

var _ Scheduler = (*MemoScheduler)(nil)

type inner struct {
	rw sync.RWMutex
	task Task
	overlap *Task
	timer *time.Timer
}

func (in *inner) UpdateTask(task Task) (ok bool) {
	in.rw.Lock()
	defer in.rw.Unlock()

	if in.task.Stage > task.Stage {
		return false
	}

	if in.task.Stage == task.Stage && in.task.Status() > task.Status() {
		return false
	}

	cur := in.task
	in.overlap = &cur
	in.task = task

	if in.timer != nil {
		in.timer.Stop()
		in.timer = nil
	}
	return true
}

func (in *inner) UpdateTimer(timer *time.Timer) {
	in.rw.Lock()
	defer in.rw.Unlock()

	if in.timer != nil {
		in.timer.Stop()
	}
	in.timer = timer
}

func (in *inner) GetAwaken() Awaken {
	in.rw.RLock()
	defer in.rw.RUnlock()

	awk := Awaken {
		TaskKey: in.task.Key,
	}
	return awk
}

func (in *inner) GetTask() Task {
	in.rw.RLock()
	defer in.rw.RUnlock()
	return in.task
}

type MemoScheduler struct {
	rw                      sync.RWMutex
	tasks                   map[string]*inner
	taskChan                chan string
	defaultKeepLiveDuration time.Duration
}

func NewMemoScheduler(defaultKeepLiveDuration time.Duration) *MemoScheduler {
	return &MemoScheduler{
		tasks:                   map[string]*inner{},
		taskChan:                make(chan string, 64),
		defaultKeepLiveDuration: defaultKeepLiveDuration,
	}
}

func (scheduler *MemoScheduler) Close(ctx context.Context) error {
	scheduler.rw.RLock()
	defer scheduler.rw.RUnlock()
	for _, t := range scheduler.tasks {
		t.UpdateTimer(nil)
	}
	return nil
}

func (scheduler *MemoScheduler) ScheduleTask(ctx context.Context, task Task, cover bool) error {
	scheduler.rw.Lock()
	in, ext := scheduler.tasks[task.Key]
	var awakeTime = task.Schedule.AwakenTime

	if !ext {
		in = &inner {
			task: task,
		}
		scheduler.tasks[task.Key] = in
	} else {
		if in.task.Status() != StatusExecuteFinished && !cover {
			return nil
		}

		if !in.UpdateTask(task) {
			scheduler.rw.Unlock()
			return errors.New("can not update stack")
		}
		awakeTime = in.GetTask().Schedule.AwakenTime
	}
	scheduler.schedule(in, awakeTime, in.GetTask().Schedule.CompensateDuration)
	scheduler.rw.Unlock()
	return nil
}

func (scheduler *MemoScheduler) schedule(in *inner, awakeTime time.Time, keepLive time.Duration) {
	taskKey := in.GetAwaken().TaskKey
	if keepLive == 0 {
		keepLive = scheduler.defaultKeepLiveDuration
	}

	laterAwakeTime := awakeTime.Add(keepLive)
	if keepLive == 0 {
		laterAwakeTime = awakeTime.Add(scheduler.defaultKeepLiveDuration)
	}

	delta := awakeTime.Sub(time.Now())

	timer := time.AfterFunc(delta, func() {
		scheduler.rw.RLock()
		in, ext := scheduler.tasks[taskKey]
		if ext {
			scheduler.schedule(in, laterAwakeTime, keepLive)
			scheduler.taskChan <- in.task.Key
		}
		scheduler.rw.RUnlock()
	})

	in.UpdateTimer(timer)
}

func (scheduler *MemoScheduler) RemoveTaskSchedule(ctx context.Context, task Task) error {
	scheduler.rw.Lock()
	defer scheduler.rw.Unlock()

	defer delete(scheduler.tasks, task.Key)

	in, ok := scheduler.tasks[task.Key]
	if !ok {
		return nil
	}

	if !in.UpdateTask(task) {
		return nil
	}
	in.UpdateTimer(nil)
	return nil
}

func (scheduler *MemoScheduler) WaitTaskAwaken(ctx context.Context) (awaken Awaken, err error) {
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case taskKey, _ := <-scheduler.taskChan:
		scheduler.rw.RLock()
		in, ok := scheduler.tasks[taskKey]
		scheduler.rw.RUnlock()
		if ok {
			awaken = in.GetAwaken()
		}
	}
	return
}

func (scheduler *MemoScheduler) ReadTask(ctx context.Context, taskKey string) (*Task, error) {
	scheduler.rw.RLock()
	in, ok := scheduler.tasks[taskKey]
	scheduler.rw.RUnlock()
	if !ok {
		return nil, nil
	} else {
		task := in.GetTask()
		return &task, nil
	}
}

func (scheduler *MemoScheduler) TaskSummery(ctx context.Context) (*Summery, error) {
	var count = 0
	scheduler.rw.RLock()
	count = len(scheduler.tasks)
	scheduler.rw.RUnlock()
	return &Summery{
		StatusCount: map[StatusCode] int64{
			StatusExecuting: int64(count),
		},
	}, nil
}

func (scheduler *MemoScheduler) NewStageID(ctx context.Context, taskKey string) (seq int64, err error) {
	scheduler.rw.RLock()
	in, ext := scheduler.tasks[taskKey]
	scheduler.rw.RUnlock()
	if !ext {
		return 1, nil
	}
	t := in.GetTask()
	return t.Stage + 1, nil
}