package mongo

import (
	"context"
	"github.com/feynman-go/workshop/cap/leader"
	"github.com/feynman-go/workshop/syncrun"
	"github.com/feynman-go/workshop/syncrun/prob"
	"github.com/feynman-go/workshop/task"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
	"time"
)

type taskDoc struct {
	Key string        `bson:"_id"`
	Stage  int64         `bson:"stage"`
	Schedule task.Schedule `bson:"schedule"`
	Info task.Info `bson:"info"`
	Execution task.Execution `bson:"execution"`
	SortKey sortKey `bson:"sortKey"`
	Status task.StatusCode `bson:"status"`
}

func (td taskDoc) ToTask() task.Task {
	return task.Task{
		td.Key,
		td.Stage,
		td.Schedule,
		td.Info,
		td.Execution,
	}
}

var _ task.Scheduler = (*TaskScheduler)(nil)

type TaskScheduler struct {
	leaders *leader.PartitionLeaders
	col string
	database *mongo.Database
	resumeToken bson.Raw
	rw sync.RWMutex
	timers map[string]*timerInfo
	ch chan taskDoc
	pb *prob.Prob
	hashPartition func(taskKey string) int16
}

func NewTaskScheduler(database *mongo.Database, col string, hashPartition func(taskKey string) int16) *TaskScheduler {
	return &TaskScheduler{
		col: col,
		timers: map[string]*timerInfo{},
		ch: make(chan taskDoc),
		database: database,
		hashPartition: hashPartition,
	}
}

func (ts *TaskScheduler) ScheduleTask(ctx context.Context, tk task.Task, overlap bool) (task.Task, error) {
	status := tk.Status()
	sr := ts.database.Collection(ts.col).FindOneAndUpdate(
		ctx,
		bson.M{
			"_id": tk.Key,
			"$or": bson.A{
				bson.M {
					"stage": bson.M {
						"$lte": tk.Stage,
					},
				},
				bson.M {
					"stage": tk.Stage,
					"status": bson.M {
						"$lte": tk.Status(),
					},
				},
			},
		},
		bson.M{
			"$setOnInsert": bson.M{
				"_id": tk.Key,
				"create_time": time.Now(),
			},
			"$set": bson.M {
				"status": status,
				"stage": tk.Stage,
				"schedule": tk.Schedule,
				"info": tk.Info,
				"execution": tk.Execution,
				"sortKey": sortKey(0).
					setPartition(ts.hashPartition(tk.Key)).
					setUnixMillionSeconds(
						int64(tk.Schedule.AwakenTime.UnixNano() / int64(time.Millisecond,
						))),
			},
		},
		options.FindOneAndUpdate().
			SetUpsert(true).
			SetReturnDocument(options.After),
	)

	if sr.Err() != nil {
		return task.Task{}, sr.Err()
	}

	var td taskDoc

	err := sr.Decode(&td)
	if err != nil {
		return task.Task{}, err
	}

	return td.ToTask(), nil
}

func (ts *TaskScheduler) CloseTaskSchedule(ctx context.Context, tk task.Task) error {
	ts.rw.Lock()
	ti := ts.timers[tk.Key]
	if ti != nil {
		ti.stop()
	}
	ts.rw.Unlock()

	_, err := ts.database.Collection(ts.col).DeleteOne(ctx, bson.M{
		"_id": tk.Key,
		"$or": bson.A{
			bson.M {
				"stage": bson.M {
					"$lte": tk.Stage,
				},
			},
			bson.M {
				"stage": tk.Stage,
				"status": bson.M {
					"$lte": tk.Status(),
				},
			},
		},
	})
	if err != nil {
		return err
	}
	return nil
}

func(ts *TaskScheduler) WaitTaskAwaken(ctx context.Context) (awaken task.Awaken, err error) {
	select {
	case <- ctx.Done():
		err = ctx.Err()
		return task.Awaken{}, err
	case td := <- ts.ch:
		return task.Awaken {
			Task: td.ToTask(),
		}, nil
	}
}

func (ts *TaskScheduler) start() {
	f := syncrun.FuncWithRandomStart(func(ctx context.Context) bool {
		ts.leaders.SyncLeader(context.Background(), func(ctx context.Context, pid leader.PartitionID) {
			ts.runPartition(ctx, pid)
		})
		return true
	}, syncrun.RandRestart(time.Second, 3 * time.Second))

	ts.pb = prob.New(f)
	ts.pb.Start()
}

func (ts *TaskScheduler) Close(ctx context.Context) error {
	ts.pb.Stop()
	select {
	case <- ctx.Done():
		return ctx.Err()
	case <- ts.pb.Stopped():
		return nil
	}
}

func (ts *TaskScheduler) ReadTask(ctx context.Context, taskKey string) (*task.Task, error) {
	rs := ts.database.Collection(ts.col).FindOne(ctx, bson.M{
		"_id": taskKey,
	})

	if rs.Err() ==  mongo.ErrNoDocuments {
		return nil, nil
	}

	if rs.Err() != nil {
		return nil, rs.Err()
	}
	var doc taskDoc
	err := rs.Decode(&doc)
	if err != nil {
		return nil, err
	}

	t := doc.ToTask()
	return &t, nil
}

func (ts *TaskScheduler) NewStageID(ctx context.Context, taskKey string) (stageID int64, err error) {
	r, err := ts.ReadTask(ctx, taskKey)
	if err != nil {
		return 0, err
	}
	if r == nil {
		return 1, nil
	}
	return r.Stage + 1, nil
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
	var doc taskDoc
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

	syncrun.Run(ctx, func(ctx context.Context) {
		// TODO create index
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
			var doc = taskDoc{}
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

func (ts *TaskScheduler) updateTask(data taskDoc) {
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

func (sk sortKey) setUnixMillionSeconds(millionSeconds int64) sortKey {
	return sk | sortKey(millionSeconds << 4)
}

func (sk sortKey) getUnixMillionSeconds() uint64 {
	return uint64((sk & ^sortKey(0xff)) >> 4)
}


type timerInfo struct {
	rw sync.RWMutex
	expectTime time.Time
	timer *time.Timer
	taskKey string
}

func (info *timerInfo) setTaskAwaken(data taskDoc, cn chan taskDoc) {
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
