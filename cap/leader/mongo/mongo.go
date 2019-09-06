package mongo

import (
	"context"
	"github.com/feynman-go/workshop/cap/leader"
	"github.com/feynman-go/workshop/parallel"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Elector struct {
	rw sync.RWMutex
	key interface{}
	col string
	database *mongo.Database
	docInvalidDuration time.Duration
	cn chan document

	resumeToken bson.Raw
}

func NewElector(key interface{}, col string, database *mongo.Database, docInvalidDuration time.Duration) *Elector {
	return &Elector{
		key: key,
		col: col,
		database: database,
		docInvalidDuration: docInvalidDuration,
		cn: make(chan document, 32),
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
		"lastTime": time.Now(),
		"seq": election.Sequence,
		"leader": election.ElectID,
	}, (&options.UpdateOptions{}).SetUpsert(true))

	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
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
	if res.MatchedCount == 0 {
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


func (mgo *Elector) WaitElectionNotify(ctx context.Context) (*leader.Election, error) {

	var electValue = &atomic.Value{}
	var errValue = &atomic.Value{}
	var errChan = make(chan error)

	parallel.RunParallel(ctx, func(ctx context.Context) {
		for ctx.Err() == nil {
			runCtx, _ := context.WithTimeout(ctx, 10 * time.Second)
			pipeline := bson.A{bson.M{"$match": bson.M{"_id": mgo.key}}}

			cs, err := mgo.database.Collection(mgo.col).Watch(runCtx, pipeline,
				options.ChangeStream().SetFullDocument(options.UpdateLookup),
				options.ChangeStream().SetMaxAwaitTime(5 * time.Second),
				options.ChangeStream().SetResumeAfter(mgo.getResumeToken()),
			)
			if err != nil {
				continue
			}

			var doc = document{}
			for cs.Next(runCtx) {
				err = cs.Decode(&doc)
				if err != nil {
					cs.Close(ctx)
					errChan <- err
					return
				}

				if doc.LastTime.Add(mgo.docInvalidDuration).Before(time.Now()) {
					continue
				}

				mgo.cn <- doc
				mgo.setResumeToken(cs.ResumeToken())
			}
			cs.Close(runCtx)
		}
	}, func(ctx context.Context) {
		select {
		case <- ctx.Done():
			return
		case err := <- errChan:
			errValue.Store(err)
			return
		case doc := <- mgo.cn:
			electValue.Store(leader.Election{
				Sequence: doc.Sequence,
				ElectID:  doc.LeaderID,
			})
		}
	})

	vElec := electValue.Load()
	vErr := errValue.Load()
	if vElec != nil {
		return nil, vErr.(error)
	}

	elect := vElec.(*leader.Election)
	return elect, nil
}

type document struct {
	ID string `bson:"_id"`
	LeaderID string `bson:"leader"`
	Sequence int64 `bson:"seq"`
	LastTime time.Time `bson:"lastTime,omitempty"`
}