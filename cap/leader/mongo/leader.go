package mongo

import (
	"context"
	"github.com/feynman-go/workshop/cap/leader"
	"github.com/feynman-go/workshop/parallel"
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
	database            *mongo.Database
	docTooLaterDuration time.Duration
	cn                  chan document

	resumeToken bson.Raw
}

func NewElector(key interface{}, col string, database *mongo.Database, docTooLaterDuration time.Duration) *Elector {
	return &Elector{
		key:                 key,
		col:                 col,
		database:            database,
		docTooLaterDuration: docTooLaterDuration,
		cn:                  make(chan document, 32),
	}
}

func (mgo *Elector) PostElection(ctx context.Context, election leader.Election) error {
	col := mgo.database.Collection(mgo.col)
	res, err := col.UpdateOne(ctx, bson.M{
		"_id": mgo.key,
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
			err := mgo.fetchAndNotify(ctx, col)
			if err != nil {
				log.Println("fetchAndNotify err:", err)
			}
		}()
	}
	return nil
}

func (mgo *Elector) KeepLive(ctx context.Context, keepLive leader.KeepLive) error {
	col := mgo.database.Collection(mgo.col)
	res, err := col.UpdateOne(ctx, bson.M{
		"_id": mgo.key,
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
			err := mgo.fetchAndNotify(ctx, col)
			if err != nil {
				log.Println("fetchAndNotify err:", err)
			}
		}()
	}
	return nil
}

func (mgo *Elector) fetchAndNotify(ctx context.Context, col *mongo.Collection) error {
	sr := col.FindOne(ctx, bson.M{
		"_id": mgo.key,
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
	case mgo.cn <- d:
	default:
		log.Println("push document chan block")
	}
	return err
}

func (mgo *Elector) setResumeToken(resumeToken bson.Raw) {
	mgo.rw.Lock()
	defer mgo.rw.Unlock()
	mgo.resumeToken = resumeToken
}

func (mgo *Elector) getResumeToken() bson.Raw{
	mgo.rw.RLock()
	defer mgo.rw.RUnlock()
	return mgo.resumeToken
}


func (mgo *Elector) WaitElectionNotify(ctx context.Context) (leader.Election, error) {

	var election leader.Election
	var streamErr error

	parallel.RunParallel(ctx, func(ctx context.Context) {
		for ctx.Err() == nil {
			log.Println("1111111111")
			runCtx, _ := context.WithTimeout(ctx, 10 * time.Second)
			//pipeline := mongo.Pipeline{bson.D{{"$match", bson.M{"_id": mgo.key}}}}

			log.Println("2222222222")
			cs, err := mgo.database.Collection(mgo.col).Watch(runCtx, mongo.Pipeline{},
				options.ChangeStream().SetFullDocument(options.UpdateLookup),
				//options.ChangeStream().SetMaxAwaitTime(5 * time.Second),
				//options.ChangeStream().SetResumeAfter(mgo.getResumeToken()),
			)

			log.Println("3333333333")
			if err != nil {
				log.Println("watch connection err:", err)
				timer := time.NewTimer(2 * time.Second)
				select {
				case <- timer.C:
				case <- ctx.Done():
					return
				}
				continue
			}

			var doc = document{}
			for cs.Next(runCtx) {
				err = cs.Decode(&doc)
				log.Println("555555555555", doc)
				if err != nil {
					cs.Close(ctx)
					streamErr = err
					return
				}
				lastTime := doc.FullDocument.LastTime
				if lastTime.Add(mgo.docTooLaterDuration).Before(time.Now()) {
					continue
				}

				mgo.cn <- doc
				mgo.setResumeToken(cs.ResumeToken())
			}


			err = cs.Err()
			if err == nil || err == mongo.ErrNilCursor || err == context.Canceled || err == context.DeadlineExceeded {
				cs.Close(runCtx)
				continue
			} else {
				cs.Close(runCtx)
				streamErr = err
				return
			}
		}
	}, func(ctx context.Context) {
		select {
		case <- ctx.Done():
			return
		case doc := <- mgo.cn:
			election = leader.Election{
				Sequence: doc.FullDocument.Sequence,
				ElectID:  doc.FullDocument.LeaderID,
			}
		}
	})

	if streamErr != nil {
		return leader.Election{}, streamErr
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