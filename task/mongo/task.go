package mongo

import (
	"context"
	"github.com/feynman-go/workshop/database/mgo"
	"github.com/feynman-go/workshop/syncrun"
	"github.com/feynman-go/workshop/syncrun/routine"
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
	Key string               `bson:"_id"`
	Stage  int64             `bson:"stage"`
	Schedule task.Schedule   `bson:"schedule"`
	Meta task.Meta           `bson:"meta"`
	Execution task.Execution `bson:"execution"`
	SortKey sortKey          `bson:"sortKey"`
	Status task.StatusCode   `bson:"status"`
}

func (td taskDoc) ToTask() task.Task {
	return task.Task{
		td.Key,
		td.Stage,
		td.Schedule,
		td.Meta,
		td.Execution,
	}
}

var _ task.Scheduler = (*TaskScheduler)(nil)

type TaskScheduler struct {
	partID        int16
	col           string
	db            *mgo.DbClient
	resumeToken   bson.Raw
	rw            sync.RWMutex
	timers        map[string]*timerInfo
	ch            chan taskDoc
	pb            *routine.Routine
	hashPartition func(taskKey string) int16
	lastWatchTime primitive.Timestamp
	inited        bool
}

func NewTaskScheduler(database *mgo.DbClient, col string, partID int16, hashPartition func(taskKey string) int16) *TaskScheduler {
	scheduler := &TaskScheduler{
		col: col,
		timers: map[string]*timerInfo{},
		ch: make(chan taskDoc, 1),
		db: database,
		hashPartition: hashPartition,
		partID: partID,
	}
	scheduler.pb = routine.New(scheduler.run)
	scheduler.pb.Start()
	return scheduler
}

func (ts *TaskScheduler) ScheduleTask(ctx context.Context, tk task.Task, cover bool) error {
	_, err := ts.update(ctx, tk, cover)
	return err
}

func (ts *TaskScheduler) RemoveTaskSchedule(ctx context.Context, tk task.Task) error {
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
			TaskKey: td.ToTask().Key,
		}, nil
	}
}

func (ts *TaskScheduler) CloseWithContext(ctx context.Context) error {
	ts.pb.Stop()
	select {
	case <- ctx.Done():
		return ctx.Err()
	case <- ts.pb.Stopped():
		return nil
	}
}

func (ts *TaskScheduler) start() {
	ts.pb.Start()
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
			break
		}
		ts.updateTask(doc)
		if doc.Schedule.AwakenTime.After(lastTime) {
			lastTime = doc.Schedule.AwakenTime
		}
	}

	if err != nil {
		return err
	}

	return cursor.Err()
}

func(ts *TaskScheduler) run(ctx context.Context) {
	var pid = ts.partID
	startKey := sortKey(0).setPartition(pid)
	endKey := sortKey(0).setPartition(pid + 1)

	syncrun.FuncWithReStart(func(ctx context.Context) bool {
		err := ts.runWatcher(ctx, startKey, endKey)
		if err != nil {
			log.Println("run watcher err:", err)
		}
		return true
	}, syncrun.RandRestart(2 * time.Second, 5 *time.Second))(ctx)
	return
}

func(ts *TaskScheduler) prepareWatchOption(interval time.Duration) (*options.ChangeStreamOptions, time.Time) {
	now := time.Now()
	if interval == 0 {
		interval = 5 * time.Second
	}

	opt := options.ChangeStream().
		SetFullDocument(options.UpdateLookup).
		SetMaxAwaitTime(interval)

	ts.rw.RLock()
	resumeToken := ts.resumeToken
	lastWatchTime := ts.lastWatchTime
	ts.rw.RUnlock()

	if resumeToken == nil {
		if lastWatchTime.T == 0 {
			lastWatchTime.T = uint32(now.Unix() - 10)
		} else {
			lastWatchTime.I ++
		}
		opt = opt.SetStartAtOperationTime(&lastWatchTime)
	} else {
		opt = opt.SetResumeAfter(ts.resumeToken)
	}


	return opt, now.Add(interval * 2)
}

func (ts *TaskScheduler) runWatcher(ctx context.Context, startKey, endKey sortKey) error {
	pipeline := mongo.Pipeline{bson.D{
		{"$match", bson.M{
			"$or": bson.A{
				bson.M{
					"fullDocument.sortKey": bson.M{"$lt": endKey, "$gte": startKey},
					"operationType": bson.M{"$in": bson.A{"insert", "replace", "delete", "update"}},
				},
				bson.M{
					"operationType": bson.M{"$in": bson.A{"invalidate"}},
				},
			},
		}},
	}}

	err := ts.db.LongTimeDo(ctx, func(ctx context.Context, db *mongo.Database) error {
		for ctx.Err() == nil {
			opt, lastTime := ts.prepareWatchOption(5 * time.Second)

			err := ts.setUpTasks(ctx, startKey, endKey, lastTime, db)
			if err != nil {
				log.Println("setup task err:", err)
				return err
			}

			runCtx, _ := context.WithDeadline(ctx, lastTime)
			cs, err := db.Collection(ts.col).Watch(runCtx, pipeline, opt)
			if err != nil {
				return err
			}

			var doc = changeDoc{}
			var invalidate = false
			for !invalidate && cs.Next(runCtx)  {
				err = cs.Decode(&doc)
				if err != nil {
					log.Println("decode change stream doc err:", err)
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
				case "invalidate":
					invalidate = true
				}
				ts.setLastWatchTime(doc.ClusterTime)
				ts.setResumeToken(doc.ID)
			}

			if invalidate {
				ts.setResumeToken(nil)
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

type changeDoc struct {
	ID bson.Raw `bson:"_id"`
	OperationType string `bson:"operationType"`
	FullDoc taskDoc `bson:"fullDocument"`
	Ns struct {
		DB string `bson:"ns"`
		Coll string `bson:"coll"`
	} `bson:"ns"`
	To struct {
		DB string `bson:"ns"`
		Coll string `bson:"coll"`
	} `bson:"to"`
	ClusterTime primitive.Timestamp `bson:"clusterTime"`
}

func (ts *TaskScheduler) update(ctx context.Context, tk task.Task, cover bool) (last *taskDoc, err error) {
	status := tk.Status()

	query := bson.M{
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
	}

	update := bson.M{
		"$setOnInsert": bson.M{
			"_id": tk.Key,
			"create_time": time.Now(),
		},
		"$set": bson.M {
			"status": status,
			"stage": tk.Stage,
			"schedule": tk.Schedule,
			"meta": tk.Meta,
			"execution": tk.Execution,
			"sortKey": sortKey(0).
				setPartition(ts.hashPartition(tk.Key)).
				setUnixMillionSeconds(
					int64(tk.Schedule.AwakenTime.UnixNano() / int64(time.Millisecond,
					))),
		},
	}

	opt := options.FindOneAndUpdate().
		SetUpsert(true).SetReturnDocument(options.Before)

	if !cover {
		opt.SetUpsert(false)
	}

	var sr *mongo.SingleResult
	err = ts.db.Do(ctx, func(ctx context.Context, db *mongo.Database) error {
		sr = db.Collection(ts.col).FindOneAndUpdate(
			ctx,
			query,
			update,
			opt,
		)
		return nil
	})

	if err != nil {
		return nil, err
	}

	if sr == nil {
		return nil, errors.New("bad result")
	}

	if sr.Err() == mongo.ErrNoDocuments {
		return nil, nil
	}

	var td taskDoc
	err = sr.Decode(&td)
	if err != nil {
		return nil, err
	}
	return &td, nil
}


func (ts *TaskScheduler) setResumeToken(resumeToken bson.Raw) {
	ts.rw.Lock()
	defer ts.rw.Unlock()
	ts.resumeToken = resumeToken
}

func (ts *TaskScheduler) setLastWatchTime(tsp primitive.Timestamp) {
	ts.rw.Lock()
	defer ts.rw.Unlock()
	ts.lastWatchTime = tsp
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

func (ts *TaskScheduler) init(ctx context.Context) error {
	if ts.inited {
		return nil
	}
	err := ts.db.Do(ctx, func(ctx context.Context, db *mongo.Database) error {
		_, err := db.Collection(ts.col).Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: bson.M{
				"sortKey": -1,
			},
		})
		return err
	})

	if err != nil {
		log.Println("init db index err:", err)
		return err
	}
	ts.inited = true
	return nil
}


type sortKey uint64

func (sk sortKey) setPartition(partition int16) sortKey {
	flag := sortKey(0xffff)
	flag = flag << 48
	flag = ^flag
	return flag & sk | (sortKey(partition) << 48)
}

func (sk sortKey) getPartition() int16 {
	return int16((sk & sortKey(0xffff000000000000)) >> 48)
}

func (sk sortKey) setUnixMillionSeconds(millionSeconds int64) sortKey {
	flag := sortKey(0xffffffff)
	flag = flag << 4
	flag = ^flag
	return flag & sk | (sortKey(millionSeconds) << 4)
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
	delta := data.Schedule.AwakenTime.Sub(time.Now())
	info.timer = time.AfterFunc(delta, func() {
		cn <- data
		d := data
		d.Schedule.AwakenTime = time.Now().Add(d.Schedule.CompensateDuration)
		info.setTaskAwaken(d, cn)
	})
}

func (info *timerInfo) stop() {
	info.rw.Lock()
	defer info.rw.Unlock()

	if info.timer != nil {
		info.timer.Stop()
	}
}