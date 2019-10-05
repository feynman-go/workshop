package mongo

import (
	"context"
	"github.com/feynman-go/workshop/cap/leader"
	"github.com/feynman-go/workshop/client/mgo"
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
	cn                  chan document

	resumeToken 		bson.Raw
	pb *prob.Prob
}

func NewElector(key interface{}, col string, database *mgo.DbClient, docTooLaterDuration time.Duration) *Elector {
	elector := &Elector {
		key:                 key,
		col:                 col,
		database:            database,
		docTooLaterDuration: docTooLaterDuration,
		cn:                  make(chan document, 32),
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
				"lastTime": primitive.NewDateTimeFromTime(time.Now()),
				"seq": election.Sequence,
				"leader": election.ElectID,
			},
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

	var d document
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

func (elector *Elector) setResumeToken(resumeToken bson.Raw) {
	elector.rw.Lock()
	defer elector.rw.Unlock()
	elector.resumeToken = resumeToken
}

func (elector *Elector) getResumeToken() bson.Raw{
	elector.rw.RLock()
	defer elector.rw.RUnlock()
	return elector.resumeToken
}

func (elector *Elector) run(ctx context.Context) {
	syncrun.FuncWithRandomStart(func(ctx context.Context) bool {
		err := elector.database.Do(ctx, func(ctx context.Context, db *mongo.Database) error{
			for ctx.Err() == nil {
				runCtx, _ := context.WithTimeout(ctx, 10 * time.Second)
				pipeline := mongo.Pipeline{bson.D{{"$match", bson.M{"_id": elector.key}}}}

				cs, err := db.Collection(elector.col).Watch(runCtx, pipeline,
					options.ChangeStream().SetFullDocument(options.UpdateLookup),
					options.ChangeStream().SetMaxAwaitTime(5 * time.Second),
					options.ChangeStream().SetResumeAfter(elector.getResumeToken()),
				)

				if err != nil {
					timer := time.NewTimer(2 * time.Second)
					select {
					case <- timer.C:
					case <- ctx.Done():
						return ctx.Err()
					}
					continue
				}

				var doc = document{}
				for cs.Next(runCtx) {
					err = cs.Decode(&doc)
					if err != nil {
						cs.Close(ctx)
						return err
					}
					lastTime := doc.FullDocument.LastTime
					if lastTime.Add(elector.docTooLaterDuration).Before(time.Now()) {
						continue
					}

					select {
					case elector.cn <- doc:
					default:
						log.Println("elector push doc block")
					}
					elector.setResumeToken(cs.ResumeToken())
				}

				err = cs.Err()
				if err == nil || err == mongo.ErrNilCursor || err == context.Canceled || err == context.DeadlineExceeded {
					cs.Close(runCtx)
					continue
				} else {
					cs.Close(runCtx)
					return err
				}
			}
			return ctx.Err()
		})
		if err != nil {
			log.Println("elector run end with err:", err)
		}
		return false
	}, syncrun.RandRestart(time.Second, 3 * time.Second))
}

func (elector *Elector) WaitElectionNotify(ctx context.Context) (leader.Election, error) {
	var election leader.Election
	select {
	case <- ctx.Done():
		return leader.Election{}, ctx.Err()
	case doc := <- elector.cn:
		election = leader.Election{
			Sequence: doc.FullDocument.Sequence,
			ElectID:  doc.FullDocument.LeaderID,
		}
	}
	return election, nil
}

type document struct {
	FullDocument struct {
		ID interface{} `bson:"_id"`
		LeaderID string `bson:"leader"`
		Sequence int64 `bson:"seq"`
		LastTime time.Time `bson:"lastTime,omitempty"`
	} `bson:"fullDocument"`
}