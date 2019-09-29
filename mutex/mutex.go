package mutex

import (
	"context"
	"sync"
)

type Mutex struct {
	rwm      sync.RWMutex
	writerCn chan struct{}
	readerCn chan struct{}
	readerCount uint32
}

func (mx *Mutex) Wait(ctx context.Context) bool {
	var cn chan struct{}
	mx.rwm.RLock()
	cn = mx.writerCn
	mx.rwm.RUnlock()
	if cn != nil {
		select {
		case <- cn:
			return true
		case <- ctx.Done():
			return false
		}
	}
	return true
}

func (mx *Mutex) Hold(ctx context.Context) bool {
	for ctx.Err() == nil {
		if mx.wait(ctx) && mx.occupy() {
			return true
		}
	}
	return false
}


func (mx *Mutex) TryHold() bool {
	if mx.occupy() {
		return true
	}
	return false
}

func (mx *Mutex) HoldForRead(ctx context.Context) bool {
	for ctx.Err() == nil {
		if mx.waitForRead(ctx) && mx.occupyRead() {
			return true
		}
	}
	return false
}

func (mx *Mutex) TryHoldForRead() bool {
	if mx.occupyRead() {
		return true
	}
	return false
}

func (mx *Mutex) wait(ctx context.Context) bool {
	mx.rwm.RLock()
	writerCn := mx.writerCn
	readerCn := mx.readerCn
	mx.rwm.RUnlock()

	for {
		switch  {
		case writerCn != nil && readerCn != nil:
			select {
			case <- writerCn:
			case <- readerCn:
			case <- ctx.Done():
				return false
			}
		case writerCn != nil:
			select {
			case <- writerCn:
			case <- ctx.Done():
				return false
			}
		default:
			return ctx.Err() == nil
		}
	}
}

func (mx *Mutex) waitForRead(ctx context.Context) bool {
	mx.rwm.RLock()
	writerCn := mx.writerCn
	mx.rwm.RUnlock()

	for {
		switch  {
		case writerCn != nil :
			select {
			case <- writerCn:
			case <- ctx.Done():
				return false
			}
		default:
			return ctx.Err() == nil
		}
	}
}


func (mx *Mutex) occupy() bool {
	mx.rwm.Lock()
	defer mx.rwm.Unlock()

	if mx.writerCn == nil && mx.readerCn == nil {
		mx.writerCn = make(chan struct{})
		return true
	}
	return false
}

func (mx *Mutex) occupyRead() bool {
	mx.rwm.Lock()
	defer mx.rwm.Unlock()

	if mx.writerCn != nil {
		return false
	}

	if mx.readerCn == nil {
		mx.readerCn = make(chan struct{})
	}
	mx.readerCount ++
	return true
}


func (mx *Mutex) Release() {
	mx.rwm.Lock()
	defer mx.rwm.Unlock()
	if mx.writerCn == nil {
		return
	}
	close(mx.writerCn)
	mx.writerCn = nil
}

func (mx *Mutex) ReleaseForRead() {
	mx.rwm.Lock()
	defer mx.rwm.Unlock()
	mx.readerCount --

	if mx.readerCount == 0 {
		close(mx.readerCn)
		mx.readerCn = nil
	}
}