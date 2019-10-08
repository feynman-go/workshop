package cache

import (
	"context"
	"errors"
	"fmt"
	"github.com/feynman-go/workshop/mutex"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"math/rand"
	"sync"
	"time"
)

var (
	ErrNotSupportCategory error = errors.New("not support category")
	ErrNotFound           error = errors.New("not found")
	ErrStoreLimited       error = errors.New("store limited")
)

type Request struct {
	ID       interface{}
	Params   map[string]string
}

type Resource struct {
	Meta map[string]string
	Data interface{}
}

type Repository struct {
	st             SourceDataStore
	ch             CacheStore
	handles        *sync.Map // 句柄
	logger         *zap.Logger
	errHandler     ErrHandler
	throughLimiter *rate.Limiter
}

func NewRepository(handler ResourceHandler, logger *zap.Logger, middle ...Middle) *Repository {
	mids := chainMiddle(middle)
	return &Repository{
		st:             mids.WrapStore(handler),
		ch:             mids.WrapCache(handler),
		handles:        new(sync.Map),
		logger:         logger,
		throughLimiter: handler.ThroughLimit,
		errHandler:     mids.WrapErrHandler(handler),
	}
}

// find resource in cache first, if not exists than read from store and update to cache
func (rep *Repository) Find(ctx context.Context, request Request) (*Resource, error) {
	var (
		err       error
		res       *Resource
	)

	v, ok := rep.handles.Load(request.ID)
	if ok {
		hd := v.(*mutex.Mutex)
		if !hd.Wait(ctx) {
			err = fmt.Errorf("wait context err")
			return nil, err
		}
	}

	res, err = rep.findFromCache(ctx, request)
	if err != nil { // reset resource to nil if is err from cache
		if rep.errHandler == nil || !rep.errHandler.ThroughOnCacheErr(ctx, request, err) {
			return nil, err
		}
		res = nil
	}

	if res == nil {
		res, err = rep.throughToStore(ctx, request)
		if err != nil {
			res, err = rep.downgrade(ctx, request, err) // try downgrade data
		} else if res == nil {
			res, err = rep.downgrade(ctx, request, nil) // try downgrade data if data not exists
		}
	}
	return res, err
}

func (rep *Repository) throughToStore(ctx context.Context, id Request) (*Resource, error) {
	var err error
	var res *Resource

	res, err = rep.tryUpdate(ctx, id)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Fetch resource from source data store. Update cache if syncCache is true. This method can be used to update cache!
func (rep *Repository) Fetch(ctx context.Context, id Request, syncCache bool) (*Resource, error) {
	if !syncCache {
		return rep.fetchFromStore(ctx, id)
	} else {
		return rep.SyncCache(ctx, id)
	}
}

// Disable disable the cache.
func (rep *Repository) Disable(ctx context.Context, request Request, force bool) error {
	var err error
	for {
		newHandler := handlerPool.Get().(*mutex.Mutex)
		v, loaded := rep.handles.LoadOrStore(request.ID, newHandler)
		if loaded {
			handlerPool.Put(newHandler)
		}
		hd := v.(*mutex.Mutex)
		ok := hd.TryHold()
		if !force {
			if !ok {
				return errors.New("resource is occupied")
			}
		} else {
			ok = hd.Hold(ctx)
			if !ok && ctx.Err() != nil {
				return ctx.Err()
			}
			if !ok {
				continue
			}
		}
		err = rep.ch.DisableCache(ctx, request)
		rep.logger.Error("Repository finish disable cache", zap.Any("id", request.ID), zap.Error(err))
		rep.handles.Delete(request.ID)
		hd.Release()
		return err
	}
}

func (rep *Repository) downgrade(ctx context.Context, id Request, err error) (*Resource, error) {
	if rep.errHandler == nil {
		return nil, err
	}
	return rep.errHandler.Downgrading(ctx, id, err)
}

// block in this method until resource prepares or return err if prepared failed. Return true if occupy the updating privilege.
func (rep *Repository) tryUpdate(ctx context.Context, request Request) (*Resource, error) {
	var err error
	newHandler := handlerPool.Get().(*mutex.Mutex)
	v, loaded := rep.handles.LoadOrStore(request.ID, newHandler)
	if loaded {
		handlerPool.Put(newHandler)
	}
	hd := v.(*mutex.Mutex)
	ok := hd.Hold(ctx)
	if ctx.Err() != nil {
		err = ctx.Err()
		return nil, err
	}

	var (
		res *Resource
	)

	if ok {
		// handle exec
		defer func() {
			rep.handles.Delete(request.ID)
			hd.Release()
		}()
		if !rep.tryThrough(ctx) {
			return nil, ErrStoreLimited
		}
		res, err = rep.SyncCache(ctx, request)
	}

	return res, err
}

// enforce sync cache from store
func (rep *Repository) SyncCache(ctx context.Context, request Request) (*Resource, error) {
	logger := ctxzap.Extract(ctx)
	res, err := rep.fetchFromStore(ctx, request)
	if err != nil {
		return res, err
	}
	if res == nil {
		return nil, nil
	}

	res, err = rep.ch.UpdateCache(ctx, request, *res)
	if err != nil {
		logger.Error(fmt.Sprint("update cache err:", err))
		return res, err
	}

	return res, err
}

func (rep *Repository) fetchFromStore(ctx context.Context, id Request) (*Resource, error) {
	var (
		res *Resource
		err error
	)
	res, err = rep.st.FetchFromStore(ctx, id)
	return res, err
}

func (rep *Repository) findFromCache(ctx context.Context, id Request) (*Resource, error) {
	return rep.ch.FindFromCache(ctx, id)
}

func (rep *Repository) tryThrough(ctx context.Context) bool {
	if rep.throughLimiter == nil {
		return true
	}
	wait := rand.Int31n(250)
	c, _ := context.WithTimeout(ctx, time.Duration(wait)*time.Millisecond)
	err := rep.throughLimiter.Wait(c)
	if err != nil {
		return false
	}
	return true
}


type Handler interface {
	CacheStore
	SourceDataStore
	ErrHandler
}

// cache repository used for cache store
type CacheStore interface {
	// update cached resource
	UpdateCache(ctx context.Context, request Request, data Resource) (*Resource, error)

	// find resource in cache, return nil if not in cache
	FindFromCache(ctx context.Context, request Request) (*Resource, error)

	// disable cache
	DisableCache(ctx context.Context, request Request) error
}

// source data store
type SourceDataStore interface {
	// return nil, if not exists
	FetchFromStore(ctx context.Context, id Request) (*Resource, error)
}

type ErrHandler interface {
	// go through on cache err, store limiter will also work
	ThroughOnCacheErr(ctx context.Context, resource Request, err error) (goThrough bool)

	//data downgrading if find data has err
	Downgrading(ctx context.Context, resource Request, err error) (*Resource, error)
}

var handlerPool = &sync.Pool{New: func() interface{} { return &mutex.Mutex{} }}

type ResourceHandler struct {
	ThroughLimit 			*rate.Limiter
	FetchFromStoreFunc      func(ctx context.Context, request Request) (*Resource, error)
	UpdateCacheFunc         func(ctx context.Context, request Request, data Resource) (*Resource, error)
	FindFromCacheFunc       func(ctx context.Context, request Request) (*Resource, error)
	DisableCacheFunc        func(ctx context.Context, request Request) error
	ThroughOnCacheErrFunc   func(ctx context.Context, resource Request, err error) (goThrough bool)   // go through on cache err, store limiter will also work
	Downgrade               func(ctx context.Context, resource Request, err error) (*Resource, error) //data downgrading
	BeforeResourceCacheFunc func(key Request)
}

func (h ResourceHandler) FetchFromStore(ctx context.Context, id Request) (*Resource, error) {
	if h.FetchFromStoreFunc != nil {
		return h.FetchFromStoreFunc(ctx, id)
	}
	return nil, ErrNotSupportCategory
}

func (h ResourceHandler) UpdateCache(ctx context.Context, request Request, data Resource) (*Resource, error) {
	if h.UpdateCacheFunc != nil {
		return h.UpdateCacheFunc(ctx, request, data)
	}
	return nil, ErrNotSupportCategory
}

func (h ResourceHandler) FindFromCache(ctx context.Context, id Request) (*Resource, error) {
	if h.FindFromCacheFunc != nil {
		return h.FindFromCacheFunc(ctx, id)
	}
	return nil, ErrNotSupportCategory
}

func (h ResourceHandler) DisableCache(ctx context.Context, id Request) error {
	if h.DisableCacheFunc != nil {
		return h.DisableCacheFunc(ctx, id)
	}
	return ErrNotSupportCategory
}

func (h *ResourceHandler) BeforeResourceCache(key Request) {
	if h.BeforeResourceCacheFunc != nil {
		h.BeforeResourceCacheFunc(key)
		return
	}
	return
}

// go through on cache err, store limiter will also work
func (h ResourceHandler) ThroughOnCacheErr(ctx context.Context, key Request, err error) (goThrough bool) {
	if h.ThroughOnCacheErrFunc != nil {
		return h.ThroughOnCacheErr(ctx, key, err)
	}
	return false
}

//data downgrading if find data has err
func (h ResourceHandler) Downgrading(ctx context.Context, key Request, err error) (*Resource, error) {
	if h.Downgrade != nil {
		res, err := h.Downgrade(ctx, key, err)
		return res, err
	}
	return nil, err
}