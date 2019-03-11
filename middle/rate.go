package middle

import (
	"errors"
	"golang.org/x/time/rate"
	"sync"
)

var RateLimitErr = errors.New("reach rate limit")

type RateMiddle struct {
	m           *sync.Map
	limiterPool *sync.Pool
}

func NewRateMiddle(burst int, limit float64) *RateMiddle {
	return &RateMiddle{
		m: &sync.Map{},
		limiterPool: &sync.Pool{
			New: func() interface{}{
				return rate.NewLimiter(rate.Limit(limit), burst)
			},
		},
	}
}


func (rm *RateMiddle) Before(eventKey int64) (func(error), error) {
	limiter := rm.limiterPool.Get().(*rate.Limiter)
	v, loaded := rm.m.LoadOrStore(eventKey, limiter)
	if loaded {
		rm.limiterPool.Put(limiter)
		limiter = v.(*rate.Limiter)
	}

	if !limiter.Allow() {
		return nil, RateLimitErr
	}
	return nil, nil
}
