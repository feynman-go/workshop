package cache

import (
	"context"
	"github.com/feynman-go/workshop/record"
)

type Middle interface {
	WrapCache(store CacheStore) CacheStore
	WrapStore(store SourceDataStore) SourceDataStore
	WrapErrHandler(handler ErrHandler) ErrHandler
}

type chainMiddle []Middle

func (middle chainMiddle) WrapCache(cache CacheStore) CacheStore {
	for _, mid := range middle {
		cache = mid.WrapCache(cache)
	}
	return cache
}

func (middle chainMiddle) WrapStore (store SourceDataStore) SourceDataStore {
	for _, mid := range middle {
		store = mid.WrapStore(store)
	}
	return store
}

func (middle chainMiddle) WrapErrHandler(handler ErrHandler) ErrHandler {
	for _, mid := range middle {
		handler = mid.WrapErrHandler(handler)
	}
	return handler
}

type recorderMid struct {
	factory record.Factory
}

func NewRecorderMid(factory record.Factory) Middle {
	return &recorderMid{
		factory: factory,
	}
}

func (mid *recorderMid) WrapCache(cache CacheStore) CacheStore {
	return recorderWrap{
		factory: mid.factory,
		cache: cache,
	}
}

func (mid *recorderMid) WrapErrHandler(handler ErrHandler) ErrHandler {
	return recorderWrap{
		factory: mid.factory,
		errHandler: handler,
	}
}

func (mid *recorderMid) WrapStore(source SourceDataStore) SourceDataStore {
	return recorderWrap{
		factory: mid.factory,
		source:  source,
	}
}

type recorderWrap struct {
	factory record.Factory
	cache CacheStore
	source SourceDataStore
	errHandler ErrHandler
}

func (rw recorderWrap) UpdateCache(ctx context.Context, request Request, data Resource) (res *Resource, err error) {
	rd, ctx := rw.factory.ActionRecorder(ctx, "UpdateCache")
	defer rd.Commit(err)
	res, err = rw.cache.UpdateCache(ctx, request, data)
	return
}

// find resource in cache, return nil if not in cache
func (rw recorderWrap) FindFromCache(ctx context.Context, request Request) (res *Resource, err error) {
	rd, ctx := rw.factory.ActionRecorder(ctx, "FindFromCache")
	defer rd.Commit(err)
	res, err = rw.cache.FindFromCache(ctx, request)
	return
}

// disable cache
func (rw recorderWrap) DisableCache(ctx context.Context, request Request) (err error) {
	rd, ctx := rw.factory.ActionRecorder(ctx, "DisableCache")
	defer rd.Commit(err)
	err = rw.cache.DisableCache(ctx, request)
	return
}

func (rw recorderWrap) FetchFromStore(ctx context.Context, req Request) (res *Resource, err error) {
	rd, ctx := rw.factory.ActionRecorder(ctx, "FetchFromStore")
	defer rd.Commit(err)
	res, err = rw.cache.FindFromCache(ctx, req)
	return
}

// go through on cache err, store limiter will also work
func (rw recorderWrap) ThroughOnCacheErr(ctx context.Context, resource Request, err error) (goThrough bool) {
	return rw.errHandler.ThroughOnCacheErr(ctx, resource, err)
}

//data downgrading if find data has err
func (rw recorderWrap) Downgrading(ctx context.Context, resource Request, err error) (*Resource, error) {
	return rw.errHandler.Downgrading(ctx, resource, err)
}