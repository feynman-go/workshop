package mongo

import (
	"context"
	"fmt"
	"github.com/feynman-go/workshop/cap/leader"
	"github.com/feynman-go/workshop/database/mgo"
	"github.com/feynman-go/workshop/syncrun"
	"github.com/feynman-go/workshop/syncrun/prob"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"sync"
	"time"
)

type Elector struct {
	rw                  sync.RWMutex
	key                 interface{}
	col                 string
	database            *mgo.DbClient
	docTooLaterDuration time.Duration
	cn                  chan electorDoc
	resumeTimestamp     primitive.Timestamp
	pb 					*prob.Prob
}

func NewElector(key interface{}, col string, database *mgo.DbClient, docTooLaterDuration time.Duration) *Elector {
	elector := &Elector {
		key:                 key,
		col:                 col,
		database:            database,
		docTooLaterDuration: docTooLaterDuration,
		cn:                  make(chan electorDoc, 32),
	}
	elector.pb = prob.New(elector.run)
	elector.pb.Start()
	return elector
}

func (elector *Elector) PostElection(ctx context.Context, election leader.Election) error {
	return elector.database.Do(ctx, func(ctx context.Context, db *mongo.Database) error {
		col := db.Collection(elector.col)
		res, err := col.UpdateOne(ctx, bson.M{
			"_id": elector.key,
			"seq": bson.M {
				"$lt": election.Sequence,
			},
		}, bson.M{
			"$set": bson.M{
				"_id": elector.key,
				"lastTime": primitive.NewDateTimeFromTime(time.Now()),
				"seq": election.Sequence,
				"leader": election.ElectID,
			},
		}, (&options.UpdateOptions{}).SetUpsert(true))
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil
			}
			return err
		}
		if res.UpsertedCount == 0 && res.ModifiedCount == 0 {
			go func() {
				ctx, _ := context.WithTimeout(context.Background(), 10 * time.Second)
				err := elector.fetchAndNotify(ctx, col)
				if err != nil {
					log.Println("fetchAndNotify err:", err)
				}
			}()
		}
		return nil
	})
}

func (elector *Elector) KeepLive(ctx context.Context, keepLive leader.KeepLive) error {
	return elector.database.Do(ctx, func(ctx context.Context, db *mongo.Database) error {
		col := db.Collection(elector.col)
		res, err := col.UpdateOne(ctx, bson.M{
			"_id": elector.key,
			"seq": bson.M {
				"$lte": keepLive.Sequence,
			},
		}, bson.M{
			"lastTime": time.Now(),
			"seq": keepLive.Sequence,
			"leader": keepLive.ElectID,
		}, (&options.UpdateOptions{}).SetUpsert(true))

		if err != nil {
			return err
		}
		if res.UpsertedCount == 0 && res.ModifiedCount == 0 {
			go func() {
				ctx, _ := context.WithTimeout(context.Background(), 10 * time.Second)
				err := elector.fetchAndNotify(ctx, col)
				if err != nil {
					log.Println("fetchAndNotify err:", err)
				}
			}()
		}
		return nil
	})

}

func (elector *Elector) fetchAndNotify(ctx context.Context, col *mongo.Collection) error {
	sr := col.FindOne(ctx, bson.M{
		"_id": elector.key,
	})
	if sr.Err() == nil {
		log.Println("find current election err:", sr.Err())
	}

	var d electorDoc
	err := sr.Decode(&d)
	if err != nil {
		return err
	}

	select {
	case elector.cn <- d:
	default:
		log.Println("push document chan block")
	}
	return err
}

func (elector *Elector) setResumeTimestamp(ts primitive.Timestamp) {
	elector.rw.Lock()
	defer elector.rw.Unlock()
	elector.resumeTimestamp = ts
}

func (elector *Elector) getResumeTimestamp() primitive.Timestamp {
	elector.rw.RLock()
	defer elector.rw.RUnlock()
	return elector.resumeTimestamp
}

func (elector *Elector) run(ctx context.Context) {

	pipeline := mongo.Pipeline{bson.D{
		{"$match", bson.M{
			"$or": bson.A{
				bson.M{
					//"fullDocument._id": elector.key,
					"operationType": bson.M{"$in": bson.A{"insert", "replace", "delete", "update"}},
				},
				bson.M{
					"operationType": bson.M{"$in": bson.A{"invalidate"}},
				},
			},
		}},
	}}

	syncrun.FuncWithRandomStart(func(ctx context.Context) bool {
		err := elector.database.LongTimeDo(ctx, func(ctx context.Context, db *mongo.Database) error{
			for ctx.Err() == nil {
				ts := elector.getResumeTimestamp()
				if ts.T == 0 {
					ts.T = uint32(time.Now().Unix() - 10)
				} else {
					ts.I ++
				}

				runCtx, _ := context.WithTimeout(ctx, 10 * time.Second)
				cs, err := db.Collection(elector.col).Watch(runCtx, pipeline,
					options.ChangeStream().SetFullDocument(options.UpdateLookup),
					options.ChangeStream().SetMaxAwaitTime(10 * time.Second),
					options.ChangeStream().SetStartAtOperationTime(&ts),
				)

				if err != nil {
					return fmt.Errorf("watch err: %v", err)
				}

				var doc = changeDoc{}
				for cs.Next(ctx) {
					err = cs.Decode(&doc)
					if err != nil {
						break
					}

					elector.setResumeTimestamp(doc.ClusterTime)
					if doc.OperationType == "invalidate" {
						break
					}

					if doc.FullDoc != nil {
						lastTime := doc.FullDoc.LastTime
						if lastTime.Add(elector.docTooLaterDuration).Before(time.Now()) {
							continue
						}
						select {
						case elector.cn <- *doc.FullDoc:
						default:
							log.Println("elector push doc block")
						}
					}
				}

				err = cs.Err()
				cs.Close(runCtx)
				if err == nil || err == mongo.ErrNilCursor || err == context.Canceled || err == context.DeadlineExceeded {
					continue
				} else {
					return err
				}
			}
			return ctx.Err()
		})
		if err != nil {
			log.Println("elector run end with err:", err)
		}
		return true
	}, syncrun.RandRestart(time.Second, 3 * time.Second)) (ctx)
}

func (elector *Elector) WaitElectionNotify(ctx context.Context) (leader.Election, error) {
	for {
		select {
		case <- ctx.Done():
			return leader.Election{}, ctx.Err()
		case doc := <- elector.cn:
			election := leader.Election{
				Sequence: doc.Sequence,
				ElectID:  doc.LeaderID,
			}
			return election, nil
		}
	}
}

type electorDoc struct {
	ID interface{} `bson:"_id"`
	LeaderID string `bson:"leader"`
	Sequence int64 `bson:"seq"`
	LastTime time.Time `bson:"lastTime,omitempty"`
}

type changeDoc struct {
	ID bson.Raw          `bson:"_id"`
	OperationType string `bson:"operationType"`
	FullDoc *electorDoc  `bson:"fullDocument"`
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

