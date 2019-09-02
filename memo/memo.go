package memo

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"
)

type MemoStore struct {
	m    *sync.Map
	pool *sync.Pool
}

func NewMemoStore(liveDuration time.Duration, liveDelta time.Duration) *MemoStore {
	return &MemoStore{
		m: &sync.Map{},
		pool: &sync.Pool{
			New: func() interface{} {
				return &memoData{
					liveDelta:    liveDelta,
					liveDuration: liveDuration,
				}
			},
		},
	}
}

func (store *MemoStore) Run(ctx context.Context) error {
	var err error
	for {
		err = store.checkDead(ctx)
		if err != nil {
			log.Println("check memory store err:", err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		tm := time.NewTimer(10 * time.Second)
		select {
		case <-tm.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (store *MemoStore) checkDead(ctx context.Context) error {
	var (
		retErr error
		list   []*memoData
		now    = time.Now()
	)
	store.m.Range(func(key, value interface{}) bool {
		if ctx.Err() != nil {
			retErr = ctx.Err()
			return false
		}
		list = append(list, value.(*memoData))
		return false
	})

	for _, l := range list {
		if !l.OkUntil(now) {
			store.m.Delete(l)
		}
	}
	return retErr
}

func (store *MemoStore) Delete(id interface{}) {
	store.m.Delete(id)
}

func (store *MemoStore) getData(key interface{}) *memoData {
	v, ext := store.m.Load(key)
	if !ext {
		return nil
	}
	return v.(*memoData)
}

func (store *MemoStore) loadData(id interface{}, deft interface{}) (interface{}, *memoData, bool) {
	data := store.pool.Get().(*memoData)
	data.Set(deft)

	v, loaded := store.m.LoadOrStore(id, data)
	if loaded {
		data.value = nil
		store.pool.Put(data)
	}
	data = v.(*memoData)
	if loaded {
		return deft, data, loaded
	}
	return data.GetValue(), data, loaded
}

func (store *MemoStore) KeepLive(id interface{}) {
	d := store.getData(id)
	if d == nil {
		return
	}
	d.KeepLive()
}

func (store *MemoStore) Set(id, value interface{}) error {
	_, d, loaded := store.loadData(id, value)
	if loaded {
		d.Set(value)
	}
	return nil
}

func (store *MemoStore) Get(id interface{}) (interface{}, bool) {
	d := store.getData(id)
	if d == nil {
		return nil, false
	}
	ret := d.GetValue()
	return ret, !IsEmpty(ret)
}

func (store *MemoStore) Load(id interface{}, deft interface{}) (interface{}, bool) {
	v, _, loaded := store.loadData(id, deft)
	return v, loaded
}

func (store *MemoStore) Live(id interface{}) time.Time {
	md := store.getData(id)
	if md == nil {
		return time.Time{}
	}
	return md.liveUntil
}

func (store *MemoStore) Range(r func(id, v interface{}) bool) {
	store.m.Range(func(key, v interface{}) bool {
		v, ok := store.Get(key)
		if ok {
			return r(key, v)
		}
		return true
	})
}

type memoData struct {
	liveDuration time.Duration
	liveDelta    time.Duration
	value        interface{}
	rw           sync.RWMutex
	lastUpdate   time.Time
	liveUntil    time.Time
}

func (data *memoData) OkUntil(t time.Time) bool {
	data.rw.RLock()
	defer data.rw.RUnlock()
	return data.liveUntil.After(t) && !data.isEmpty()
}

func (data *memoData) GetValue() interface{} {
	var (
		ag interface{}
	)
	if !data.OkUntil(time.Now()) {
		return empty
	}
	data.rw.RLock()
	ag = data.value
	data.rw.RUnlock()
	return ag
}

func (data *memoData) KeepLive() {
	data.rw.Lock()
	data.keepLive()
	data.rw.Unlock()
	return
}

func (data *memoData) keepLive() {
	if data.isEmpty() {
		return
	}

	delta := time.Duration(rand.Int63n(int64(data.liveDelta)))
	n := time.Now()
	data.liveUntil = n.Add(data.liveDuration - delta)
	return
}

func (data *memoData) Set(value interface{}) {
	var n = time.Now()
	data.rw.Lock()
	defer data.rw.Unlock()
	data.value = value
	data.lastUpdate = n
	data.keepLive()
}

func (data *memoData) IsEmpty() bool {
	data.rw.Lock()
	defer data.rw.Unlock()

	return data.isEmpty()
}

func (data *memoData) isEmpty() bool {
	return data.value == empty
}

func IsEmpty(v interface{}) bool {
	return v == empty
}


var empty = &struct{}{}