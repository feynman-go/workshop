package breaker

import (
	"context"
	"sync"
)

type Breaker struct {
	rw       sync.RWMutex
	onChan chan struct{}
	offChan chan struct{}
	reason string
}

func New(on bool) *Breaker {
	bk := &Breaker{
		onChan:  make(chan struct{}),
		offChan: make(chan struct{}),
		reason:  "",
	}
	if on {
		close(bk.onChan)
	} else {
		close(bk.offChan)
	}
	return bk
}

func (breaker *Breaker) IsOn() bool {
	select {
	case <- breaker.onChan:
		return true
	default:
		return false
	}
}

func (breaker *Breaker) On(reason string) {
	breaker.rw.Lock()
	defer breaker.rw.Unlock()

	select {
	case <- breaker.onChan:
	case <- breaker.offChan:
		close(breaker.onChan)
		breaker.offChan = make(chan struct{})
	}
	breaker.reason = reason
}

func (breaker *Breaker) Off(reason string) {
	breaker.rw.Lock()
	defer breaker.rw.Unlock()

	select {
	case <- breaker.onChan:
		close(breaker.offChan)
		breaker.onChan = make(chan struct{})
	case <- breaker.offChan:
	}
	breaker.reason = reason
}


func (breaker *Breaker) OnChan() <- chan struct{} {
	return breaker.onChan
}

func (breaker *Breaker) OffChan() <- chan struct{} {
	return breaker.offChan
}

// ctn is continue
func (breaker *Breaker) WaitBreakerOn(ctx context.Context, d func(ctx context.Context) (doAgain bool)) int {
	var (
		ctn = true
		count int
		)
	for ctn {
		breaker.DoIfBreakerOn(ctx, func(ctx context.Context) {
			ctn = d(ctx)
			count ++
		})
	}
	return count
}

func (breaker *Breaker) DoIfBreakerOn(ctx context.Context, d func(ctx context.Context)) bool {
	select {
	case <- ctx.Done():
		return false
	case <- breaker.OnChan():
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		go func() {
			select {
			case <- runCtx.Done():
			case <- breaker.OffChan():
				cancel()
			}
		}()
		d(runCtx)
		return true
	}
}