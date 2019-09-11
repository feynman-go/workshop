package mongo

import (
	"context"
	"github.com/feynman-go/workshop/cap/leader"
	"github.com/feynman-go/workshop/parallel"
	"github.com/feynman-go/workshop/parallel/prob"
	"github.com/feynman-go/workshop/task"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
	"time"
)

type taskData struct {
	Key string        `bson:"_id"`
	Stage  int64         `bson:"stage"`
	Schedule task.Schedule `bson:"schedule"`
	Info task.Info `bson:"info"`
	Execution task.Execution `bson:"execution"`
	SortKey sortKey `bson:"sortKey"`
	Status task.StatusCode `bson:"status"`
}


type TaskScheduler struct {
	leaders *leader.PartitionLeaders
	col string
	database *mongo.Database
	resumeToken bson.Raw
	rw sync.RWMutex
	timers map[string]*timerInfo
	ch chan taskData
	pb *prob.Prob
	timeOut time.Duration
}

func (ts *TaskScheduler) ScheduleTask(ctx context.Context, tk task.Task, overlap bool) (task.Task, error) {
	status := tk.Status(time.Now())

	sr := ts.database.Collection(ts.col).FindOneAndUpdate(
		ctx,
		bson.M{
			"_id": tk.Key,
			"stage": bson.M {
				"$lte": tk.Stage,
			},
			"status": bson.M {
				"$lte": status,
			},
		},
		bson.M{
			"$set": bson.M{
				"status": tk.Status(status),
			},
		},
		options.FindOneAndUpdate().
			SetUpsert(true).
			SetMaxTime(ts.timeOut).
			SetReturnDocument(options.After),
	)

	if sr.Err() != nil {
		return task.Task{}, sr.Err()
	}

	sr.Decode()
}

func (ts *TaskScheduler) CloseTaskSchedule(ctx context.Context, taskKey string) error {
	ts.rw.Lock()
	ti := ts.timers[taskKey]
	ts.rw.Unlock()
	if ti != nil {
		ti.stop()
	}
	ts.database.
}

func(ts *TaskScheduler) WaitTaskAwaken(ctx context.Context) (awaken task.Awaken, err error) {
	select {
	case <- ctx.Done():
		err = ctx.Err()
	case td := <- ts.ch:
		return task.Awaken {
			Task: task.Task{
				Key:       td.Key,
				Stage:     td.Seq,
				Schedule:  td.Schedule,
				Info:      td.Info,
				Execution: td.Execution,
			},
		}, nil
	}
}

func (ts *TaskScheduler) start() {
	ts.pb = prob.New()
	prob.SyncRunWithRestart(ts.pb, func(ctx context.Context) bool {
		ts.leaders.SyncLeader(context.Background(), func(ctx context.Context, pid leader.PartitionID) {
			ts.runPartition(ctx, pid)
		})
		return true
	}, prob.RandRestart(time.Second, 3 * time.Second))

	ts.pb.Start()
}

func (ts *TaskScheduler) Close(ctx context.Context) error {
	ts.pb.Close()
	select {
	case <- ctx.Done():
		return ctx.Err()
	case <- ts.pb.Closed():
		return nil
	}
}

func (ts *TaskScheduler) ReadTask(ctx context.Context, taskKey string) (*task.Task, error) {

}

func (ts *TaskScheduler) NewStageID(ctx context.Context, taskKey string) (seq int64, err error) {

}

func(ts *TaskScheduler) initTasks(ctx context.Context, startKey, endKey sortKey) (lastTime uint32, err error) {
	lastTime = uint32(time.Now().Unix())
	c, err := ts.database.Collection(ts.col).Find(ctx, bson.M{
		"sortKey": bson.M{
			"$gte": startKey,
			"lt": endKey,
		},
	})
	if err != nil {
		return 0, err
	}
	var doc taskData
	for c.Next(ctx) {
		err = c.Decode(&doc)
		if err != nil {
			c.Close(ctx)
			return 0, err
		}
		ts.updateTask(doc)
	}
	return
}

func(ts *TaskScheduler) runPartition(ctx context.Context, pid leader.PartitionID) {
	var errChan = make(chan error, 1)

	parallel.RunParallel(ctx, func(ctx context.Context) {
		var (
			first bool
			startKey = sortKey(0).setPartition(int16(pid))
			endKey = sortKey(0).setPartition(int16(pid + 1))
		)

		lt, err := ts.initTasks(ctx, startKey, endKey)
		if err != nil {
			errChan <- err
			return
		}

		for ctx.Err() == nil {
			runCtx, _ := context.WithTimeout(ctx, 10 * time.Second)
			pipeline := bson.A{bson.M{
				"$match": bson.M{
					"sortKey": bson.M{
						"$gte": startKey,
						"lt": endKey,
					},
				},
			}}

			opt := options.ChangeStream().
				SetFullDocument(options.UpdateLookup).
				SetMaxAwaitTime(5 * time.Second)
			if first {
				opt = opt.SetStartAtOperationTime(&primitive.Timestamp{
					T: lt - 30,
				})
				first = false
			} else {
				opt = opt.SetResumeAfter(ts.getResumeToken())
			}


			cs, err := ts.database.Collection(ts.col).Watch(runCtx, pipeline, opt)
			if err != nil {
				errChan <- err
				return
			}
			var doc = taskData{}
			for cs.Next(runCtx) {
				err = cs.Decode(&doc)
				if err != nil {
					errChan <- err
					cs.Close(ctx)
					return
				}
				ts.updateTask(doc)
			}
			cs.Close(runCtx)
		}
	})
	return
}

func (ts *TaskScheduler) setResumeToken(resumeToken bson.Raw) {
	ts.rw.Lock()
	defer ts.rw.Unlock()
	ts.resumeToken = resumeToken
}

func (ts *TaskScheduler) getResumeToken() bson.Raw{
	ts.rw.RLock()
	defer ts.rw.RUnlock()
	return ts.resumeToken
}

func (ts *TaskScheduler) updateTask(data taskData) {
	ts.rw.Lock()
	defer ts.rw.Unlock()

	if data.Schedule.AwakenTime.Sub(time.Now()) > 10 * time.Minute {
		delete(ts.timers, data.Key)
		return
	}

	info, ext := ts.timers[data.Key]
	if !ext {
		info = &timerInfo{
			taskKey: data.Key,
		}
		ts.timers[data.Key] = info
	}
	info.setTaskAwaken(data, ts.ch)
}


type sortKey uint64

func (sk sortKey) setPartition(partition int16) sortKey {
	return sk | sortKey(partition << 48)
}

func (sk sortKey) getPartition() int16 {
	return int16((sk & sortKey(0xffff000000000000)) >> 48)
}

func (sk sortKey) setTime(partition int32) sortKey {
	return sk | sortKey(partition << 16)
}

func (sk sortKey) getTime() int32 {
	return int32((sk & sortKey(0xffffffff0000)) >> 16)
}


type timerInfo struct {
	rw sync.RWMutex
	expectTime time.Time
	timer *time.Timer
	taskKey string
}

func (info *timerInfo) setTaskAwaken(data taskData, cn chan taskData) {
	info.rw.Lock()
	defer info.rw.Unlock()

	if info.timer != nil {
		info.timer.Stop()
	}
	info.expectTime = data.Schedule.AwakenTime
	info.timer = time.AfterFunc(data.Schedule.AwakenTime.Sub(time.Now()), func() {
		cn <- data
	})
}

func (info *timerInfo) stop() {
	info.rw.Lock()
	defer info.rw.Unlock()

	if info.timer != nil {
		info.timer.Stop()
	}
}
