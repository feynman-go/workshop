package middle

import (
	"errors"
	"fmt"
	"golang.org/x/time/rate"
	"sync"
	"time"
)

const (
	BreakerStatusClosed = iota
	BreakerStatusOpen
	BreakerStatusTransition
)

type Breaker struct {
	limiter      *rate.Limiter
	rw           sync.RWMutex
	status       int
	threshold    float64 // threshold value of failed / total
	openTime     time.Time
	openDuration time.Duration
}

// failedLimit is the frequence, burst is the max bucket size.
func NewBreaker(failedLimit float64, bucketSize int, openDuration time.Duration) *Breaker {
	return &Breaker{
		limiter: rate.NewLimiter(rate.Limit(failedLimit), bucketSize),
		openDuration: openDuration,
	}
}

// return status
func (b *Breaker) AddResult(success bool) {
	b.rw.Lock()
	b.addResult(success)
	b.rw.Unlock()
}

func (b *Breaker) addResult(success bool) {
	switch b.status {
	case BreakerStatusOpen:
		if time.Now().After(b.openTime) {
			b.status = BreakerStatusTransition
			b.addResult(success)
		}
	case BreakerStatusClosed:
		if !success {
			if b.checkThreshold() {
				b.open()
			}
		}
	case BreakerStatusTransition:
		if success {
			b.close()
		} else {
			b.open()
		}
	default:
		panic(fmt.Errorf("bad status %v", b.status))
	}
}

func (b *Breaker) checkThreshold() bool {
	return !b.limiter.Allow()
}

func (b *Breaker) open() {
	b.status = BreakerStatusOpen
	b.openTime = time.Now().Add(b.openDuration)
}

func (b *Breaker) close() {
	b.status = BreakerStatusClosed
	b.limiter.SetLimit(b.limiter.Limit())
}


// get current status
func (b *Breaker) Status() int {
	var res int
	b.rw.RLock()
	res = b.status
	if res == BreakerStatusOpen {
		if time.Now().After(b.openTime) {
			res = BreakerStatusTransition
		}
	}
	b.rw.RUnlock()
	return res
}

var BreakerOpenErr = errors.New("breaker is opened")

type BreakerMiddle struct {
	m *sync.Map
	breakerPool *sync.Pool
}

func NewBreakerMiddle(openDuration time.Duration, limit float64, burst int) *BreakerMiddle {
	return &BreakerMiddle{
		m: &sync.Map{},
		breakerPool: &sync.Pool{
			New: func() interface{}{
				return NewBreaker(limit, burst, openDuration)
			},
		},
	}
}

func (bm *BreakerMiddle) Before(eventKey int64) (func(error), error) {
	bk := bm.breakerPool.Get().(*Breaker)
	v, loaded := bm.m.LoadOrStore(eventKey, bk)
	if loaded {
		bm.breakerPool.Put(bk)
		bk = v.(*Breaker)
	}

	switch s := bk.Status(); s {
	case BreakerStatusClosed, BreakerStatusTransition:
		return func(err error) {
			bm.After(eventKey, err)
		}, nil
	case BreakerStatusOpen:
		return nil, BreakerOpenErr
	default:
		panic("unknown status: " + fmt.Sprint(s))
	}
}

func (bm *BreakerMiddle) After(eventKey int64, err error) {
	v, ok := bm.m.Load(eventKey)
	if !ok {
		return
	}
	bk := v.(*Breaker)
	bk.addResult(err == nil)
	return
}
