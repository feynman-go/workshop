package pagesche

import (
	"context"
	"errors"
	"fmt"
	"github.com/feynman-go/workshop/schedule"
	"github.com/feynman-go/workshop/schedule/timewheel"
	"github.com/feynman-go/workshop/syncrun/routine"
	"log"
	"sort"
	"sync"
	"time"
)

var _ schedule.Scheduler = (*Scheduler)(nil)
var _ schedule.Receiver = (*Scheduler)(nil)

type Scheduler struct {
	store ScheduleStore
	timeWheel *timewheel.TimeWheel
	currentPage []ScheduleData // timestamp desc order list
	lastTimeMs Timestamp
	pageInterval int64
	rw sync.RWMutex
	rt *routine.Routine
}

func NewScheduler(timeWheel *timewheel.TimeWheel, store ScheduleStore, pageInterval int64) *Scheduler {
	scheduler := &Scheduler{
		timeWheel: timeWheel,
		pageInterval: pageInterval,
		store: store,
	}
	scheduler.rt = routine.New(scheduler.run)
	scheduler.rt.Start()
	return scheduler
}

// not call in parallel
func (s *Scheduler) Commit(ctx context.Context, err error) {
	l := len(s.currentPage)
	if l == 0 {
		return
	}
	d := s.currentPage[l - 1]
	markErr := s.store.MarkMillionSeconds(ctx, d.Timestamp)
	if err != nil {
		log.Println("mark million seconds err:", markErr)
	}
}

// not call in parallel
func (s *Scheduler) WaitAwaken(ctx context.Context) (schedule.Awaken, error) {
	for ctx.Err() == nil {
		if l := len(s.currentPage); l > 0 {
			d := s.currentPage[l - 1]
			s.currentPage = s.currentPage[:l - 1]
			awaken := ScheduleAwaken{
				data: d,
			}
			return awaken, nil
		}

		tasks, err := s.timeWheel.WaitAvailableTask(ctx)
		if err != nil {
			return nil, err
		}

		sort.Slice(tasks, func(i, j int) bool{
			task1 := tasks[i]
			task2 := tasks[j]
			return !task1.Payload.(ScheduleData).Timestamp.Less(task2.Payload.(ScheduleData).Timestamp)
		})

		for _, t := range tasks {
			s.currentPage = append(s.currentPage, t.Payload.(ScheduleData))
		}
	}
	return nil, nil
}

// if schedule over max time wheel. Return ErrOverMaxDuration
func (s *Scheduler) PostSchedule(ctx context.Context, schedule schedule.Schedule) error {
	if schedule.ID == "" {
		return fmt.Errorf("empty schedule id")
	}

	now := time.Now()
	if schedule.ExpectTime.Before(now) {
		schedule.MaxDelays = now.Sub(schedule.ExpectTime)
	}

	return s.addSchedule(ctx, schedule)
}

func (s *Scheduler) addSchedule(ctx context.Context, sch schedule.Schedule) error {
	data, err := s.store.SaveSchedule(ctx, sch)
	if err != nil {
		return fmt.Errorf("save schedule: %w", err)
	}

	tmMs := sch.ExpectTime.UnixNano() / int64(time.Millisecond)
	err = s.timeWheel.AddTask(timewheel.TimerTask{
		ExpectUnixMs: tmMs,
		Key:          sch.ID,
		Payload:      data,
	})
	s.rw.Unlock()
	if err == nil {
		s.rw.Lock()
		if s.lastTimeMs.Less(data.Timestamp) {
			s.lastTimeMs = data.Timestamp
		}
		s.rw.Unlock()
		return nil
	}

	if !errors.Is(err, timewheel.ErrOverMaxDuration) {
		return fmt.Errorf("add task %w", err)
	}
	return nil
}

func (s *Scheduler) run(ctx context.Context) {
	tm := time.NewTimer(0)
	for {
		ms, err := s.store.GetMarkMillionSeconds(ctx)
		if err == nil {
			s.lastTimeMs = ms
			break
		}
		log.Println("mark million seconds")
		select {
		case <- ctx.Done():
			return
		case <- time.NewTimer(5 * time.Second).C:
		}
	}

	for ctx.Err() ==  nil {
		select {
		case <- ctx.Done():
		case <- tm.C:
		}

		s.rw.RLock()
		lastTimeMs :=  s.lastTimeMs
		s.rw.RUnlock()
		endMs := Timestamp{
			MillionSeconds: lastTimeMs.MillionSeconds + s.pageInterval,
			Index:          0,
		}
		datas, err := s.store.GetPageSchedule(ctx, lastTimeMs, endMs)
		if err != nil {
			log.Println("get data service", err)
			continue
		}

		for _, d := range datas {
			err = s.addSchedule(ctx, d.Spec)
			if err != nil {
				log.Println("add schedule:", err)
				panic(err)
			}
		}
		tm.Reset(time.Duration(s.pageInterval / 2) * time.Millisecond)
	}
}

func (s *Scheduler) CloseWithContext(ctx context.Context) error {
	return s.rt.StopAndWait(ctx)
}

type ScheduleAwaken struct {
	data ScheduleData
}

func (awaken ScheduleAwaken) Spec() schedule.Schedule {
	return awaken.data.Spec
}

type ScheduleStore interface {
	GetPageSchedule(ctx context.Context, fromTimeMs Timestamp, toTimeMs Timestamp) ([]ScheduleData, error)
	SaveSchedule(ctx context.Context, s schedule.Schedule) (ScheduleData, error)
	MarkMillionSeconds(ctx context.Context, timestamp Timestamp) error
	GetMarkMillionSeconds(ctx context.Context) (timestamp Timestamp, err error)
}

type Timestamp struct {
	MillionSeconds int64
	Index int32
}

func (ts Timestamp) Less(timestamp Timestamp) bool {
	if ts.MillionSeconds < timestamp.MillionSeconds {
		return true
	}
	if ts.MillionSeconds == timestamp.MillionSeconds && ts.Index < timestamp.Index {
		return true
	}
	return false
}

type ScheduleData struct {
	Timestamp Timestamp `bson:"timestamp"`
	Spec schedule.Schedule `bson:"spec"`
}

