package mongo

import (
	"context"
	"github.com/feynman-go/workshop/client/mgo"
	"github.com/feynman-go/workshop/syncrun/prob"
	"github.com/feynman-go/workshop/task"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"sync"
	"time"
)

const (
	PartVarPartitionIDField = "taskPartId"
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
	partID int16
	col string
	db *mgo.DbClient
	resumeToken bson.Raw
	rw sync.RWMutex
	timers map[string]*timerInfo
	ch chan taskDoc
	pb *prob.Prob
	hashPartition func(taskKey string) int16
	lastTimePoint time.Time
}

func NewTaskScheduler(database *mgo.DbClient, col string, partID int16, hashPartition func(taskKey string) int16) *TaskScheduler {
	scheduler := &TaskScheduler{
		col: col,
		timers: map[string]*timerInfo{},
		ch: make(chan taskDoc),
		db: database,
		hashPartition: hashPartition,
		partID: partID,
	}
	scheduler.pb = prob.New(scheduler.run)
	return scheduler
}

func (ts *TaskScheduler) ScheduleTask(ctx context.Context, tk task.Task, overlap bool) (task.Task, error) {
	status := tk.Status()
	var sr *mongo.SingleResult

	err := ts.db.Do(ctx, func(ctx context.Context, db *mongo.Database) error {
		sr = db.Collection(ts.col).FindOneAndUpdate(
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
			return sr.Err()
		}

		return nil
	})

	if err != nil {
		return task.Task{}, err
	}

	if sr == nil {
		return task.Task{}, errors.New("bad result")
	}

	var td taskDoc
	err = sr.Decode(&td)
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

	err := ts.db.Do(ctx, func(ctx context.Context, db *mongo.Database) error {
		_, err := db.Collection(ts.col).DeleteOne(ctx, bson.M{
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
		return err
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
	var rs *mongo.SingleResult

	err := ts.db.Do(ctx, func(ctx context.Context, db *mongo.Database) error {
		rs = db.Collection(ts.col).FindOne(ctx, bson.M{
			"_id": taskKey,
		})
		return nil
	})

	if err != nil {
		return nil, err
	}

	if rs.Err() == mongo.ErrNoDocuments {
		return nil, nil
	}

	if rs.Err() != nil {
		return nil, rs.Err()
	}
	var doc taskDoc
	err = rs.Decode(&doc)
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

func(ts *TaskScheduler) setUpTasks(ctx context.Context, startKey, endKey sortKey, endTime time.Time, db *mongo.Database) (err error) {
	cursor, err := db.Collection(ts.col).Find(ctx, bson.M{
		"sortKey": bson.M{
			"$gte": startKey,
			"$lt": endKey,
		},
		"schedule.awaken": bson.M{
			"$lte": endTime,
		},
	})

	if err != nil {
		return err
	}

	defer cursor.Close(ctx)

	var doc taskDoc
	var lastTime time.Time
	for cursor.Next(ctx) {
		err = cursor.Decode(&doc)
		if err != nil {
			return err
		}
		ts.updateTask(doc)
		if doc.Schedule.AwakenTime.After(lastTime) {
			lastTime = doc.Schedule.AwakenTime
		}
	}
	ts.rw.Lock()
	defer ts.rw.Unlock()

	if lastTime.After(ts.lastTimePoint) {
		ts.lastTimePoint = lastTime
	}
	return cursor.Err()
}

func(ts *TaskScheduler) run(ctx context.Context) {
	var pid = ts.partID
	startKey := sortKey(0).setPartition(pid)
	endKey := sortKey(0).setPartition(pid + 1)

	ts.runWatcher(ctx, startKey, endKey)
	return
}

func(ts *TaskScheduler) runWatcher(ctx context.Context, startKey, endKey sortKey) error {
	pipeline := bson.A{bson.M{
		"$match": bson.M{
			"sortKey": bson.M{
				"$gte": startKey,
				"lt": endKey,
			},
		},
	}}

	err := ts.db.LongTimeDo(ctx, func(ctx context.Context, db *mongo.Database) error {
		for ctx.Err() == nil {
			opt := options.ChangeStream().
				SetFullDocument(options.UpdateLookup).
				SetMaxAwaitTime(5 * time.Second)

			ts.rw.RLock()
			if ts.resumeToken == nil {
				opt = opt.SetStartAtOperationTime(&primitive.Timestamp{
					T: uint32(time.Now().Unix() + 30),
				})
			} else {
				opt = opt.SetResumeAfter(ts.getResumeToken())
			}
			if ts.lastTimePoint.IsZero() {
				ts.lastTimePoint = time.Now()
			}
			ts.rw.RUnlock()

			lastTime := ts.lastTimePoint.Add(10 * time.Second)
			err := ts.setUpTasks(ctx, startKey, endKey, lastTime, db)
			if err != nil {
				return err
			}

			runCtx, _ := context.WithTimeout(ctx, lastTime.Sub(time.Now()))

			cs, err := db.Collection(ts.col).Watch(runCtx, pipeline, opt)
			if err != nil {
				return err
			}

			var doc = struct {
				ID bson.Raw `bson:"_id"`
				OperationType string `bson:"operationType"`
				FullDoc taskDoc `bson:"fullDocument"`
			}{}

			for cs.Next(runCtx) {
				err = cs.Decode(&doc)
				if err != nil {
					break
				}

				switch doc.OperationType {
				case "insert", "update", "replace":
					awaken := doc.FullDoc.Schedule.AwakenTime
					if awaken.Before(lastTime) || awaken.Equal(lastTime) {
						ts.updateTask(doc.FullDoc)
					}
				case "delete":
					ts.removeTask(doc.FullDoc)
				}
				ts.setResumeToken(doc.ID)
			}
			if err == nil {
				err = cs.Err()
			}
			if err != nil {
				log.Println("watcher cur err:", err)
			}
			cs.Close(ctx)
		}
		return ctx.Err()
	})

	return err
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

	info, ext := ts.timers[data.Key]
	if !ext {
		info = &timerInfo{
			taskKey: data.Key,
		}
		ts.timers[data.Key] = info
	}
	info.setTaskAwaken(data, ts.ch)
}

func (ts *TaskScheduler) removeTask(data taskDoc) {
	ts.rw.Lock()
	defer ts.rw.Unlock()

	info, ext := ts.timers[data.Key]
	if ext {
		info.stop()
	}
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
		d := data
		d.Schedule.AwakenTime = time.Now().Add(d.Schedule.CompensateDuration)
		info.setTaskAwaken(data, cn)
	})
}

func (info *timerInfo) stop() {
	info.rw.Lock()
	defer info.rw.Unlock()

	if info.timer != nil {
		info.timer.Stop()
	}
}
