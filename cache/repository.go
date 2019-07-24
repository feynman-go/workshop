package cache

import (
	"context"
	"errors"
	"fmt"
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

type ResourceKey struct {
	Category string // used for category, helper field
	ID       string
}

type Resource struct {
	Key  ResourceKey
	Data interface{}
}

type Repository struct {
	throughLimiter *rate.Limiter
	st             SourceDataStore
	ch             CacheStore
	monitor        RepositoryMonitor
	handles        *sync.Map // 句柄
	hook           RepositoryHook
	logger         *zap.Logger
}

func RepositoryFromResourcesHandler(handler *ResourceHandlers, monitor RepositoryMonitor, logger *zap.Logger, throughLimiter *rate.Limiter) *Repository {
	return &Repository{
		throughLimiter: throughLimiter,
		st:             handler,
		ch:             handler,
		handles:        new(sync.Map),
		monitor:        monitor,
		logger:         logger,
		hook:           handler,
	}
}

// find resource in cache first, if not exists than read from store and update to cache
func (rep *Repository) Find(ctx context.Context, id ResourceKey) (*Resource, error) {
	var (
		hit = true
		t = time.Now()
		err error
	)

	v, ok := rep.handles.Load(id)
	if ok {
		hd := v.(*Handler)
		err = hd.onlyWait(ctx)
		if err != nil {
			return nil, err
		}
	}

	res, err := rep.findFromCache(ctx, id)
	if err != nil {
		return nil, err
	}

	if res == nil {
		hit = false

		if !rep.tryThrough(ctx) {
			return nil, ErrStoreLimited
		}

		_, err := rep.tryUpdate(ctx, id)
		if err != nil {
			return nil, err
		}

		res, err = rep.findFromCache(ctx, id)
		if err != nil {
			return nil, err
		}

		if res == nil {
			return nil, ErrNotFound
		}
	}

	if rep.monitor != nil {
		rep.monitor.AddFindRecord(id, hit, time.Now().Sub(t))
	}
	return res, nil
}

// Fetch resource from source data store. Update cache if syncCache is true. This method can be used to update cache!
func (rep *Repository) Fetch(ctx context.Context, id ResourceKey, syncCache bool) (*Resource, error) {
	if !syncCache {
		return rep.fetchFromStore(ctx, id)
	} else {
		return rep.SyncCache(ctx, id)
	}
}

// Disable disable the cache.
func (rep *Repository) Disable(ctx context.Context, id ResourceKey, force bool) error {
	var err error
	for {
		newHandler := handlerPool.Get().(*Handler)
		newHandler.Key = id
		v, loaded := rep.handles.LoadOrStore(id, newHandler)
		if loaded {
			handlerPool.Put(newHandler)
		}
		hd := v.(*Handler)
		ok := hd.occupyOrNotWait()
		if !force {
			if !ok {
				return errors.New("resource is occupied")
			}
		} else {
			ok, err = hd.occupyOrWait(ctx)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
		}
		err = rep.ch.DisableCache(ctx, id)
		rep.logger.Error("Repository finish disable cache", zap.Any("id", id), zap.Error(err))
		rep.handles.Delete(id)
		hd.release()
		return err
	}
}

// block in this method until resource prepares or return err if prepared failed. Return true if occupy the updating privilege.
func (rep *Repository) tryUpdate(ctx context.Context, id ResourceKey) (bool, error) {
	var err error
	newHandler := handlerPool.Get().(*Handler)
	newHandler.Key = id
	v, loaded := rep.handles.LoadOrStore(id, newHandler)
	if loaded {
		handlerPool.Put(newHandler)
	}
	hd := v.(*Handler)
	ok, err := hd.occupyOrWait(ctx)
	if err != nil {
		return false, err
	}

	if ok {
		// handle exec
		defer func() {
			rep.handles.Delete(id)
			hd.release()
		}()
		_, err = rep.SyncCache(ctx, id)
	}

	return true, err
}

// enforce sync cache from store
func (rep *Repository) SyncCache(ctx context.Context, id ResourceKey) (*Resource, error) {
	logger := ctxzap.Extract(ctx)
	if rep.hook != nil {
		rep.hook.BeforeResourceCache(id)
	}

	res, err := rep.fetchFromStore(ctx, id)
	if err != nil {
		return res, err
	}

	if res == nil {
		return nil, errors.New("fetch from store is empty")
	}

	err = rep.ch.UpdateCache(ctx, *res)
	if err != nil {
		logger.Error(fmt.Sprint("update cache err:", err))
	}
	return res, nil
}

func (rep *Repository) fetchFromStore(ctx context.Context, id ResourceKey) (*Resource, error) {
	var (
		res *Resource
		err error
	)
	res, err = rep.st.FetchFromStore(ctx, id)
	return res, err
}

func (rep *Repository) findFromCache(ctx context.Context, id ResourceKey) (*Resource, error) {
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

// monitor interface
type RepositoryMonitor interface {
	AddFindRecord(key ResourceKey, hit bool, duration time.Duration)
}

// monitor interface
type RepositoryHook interface {
	BeforeResourceCache(key ResourceKey)
}

// cache repository used for cache store
type CacheStore interface {
	// update cached resource
	UpdateCache(ctx context.Context, data Resource) error

	// find resource in cache, return nil if not in cache
	FindFromCache(ctx context.Context, id ResourceKey) (*Resource, error)

	// disable cache
	DisableCache(ctx context.Context, id ResourceKey) error
}

// source data store
type SourceDataStore interface {
	// return nil, if not exists
	FetchFromStore(ctx context.Context, id ResourceKey) (*Resource, error)
}

// gather business logic into handler
type RepositoryHandler interface {
	SourceDataStore
	CacheStore
	RepositoryMonitor
	RepositoryHook
}

type Handler struct {
	Key ResourceKey
	mx  sync.RWMutex
	cd  chan struct{}
}

func (tk *Handler) occupy() bool {
	if tk.cd == nil {
		tk.cd = make(chan struct{})
		return true
	}
	return false
}


// wait context end or resource handler released, only called by Handler!
func (tk *Handler) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		logger := ctxzap.Extract(ctx)
		dead, _ := ctx.Deadline()
		logger.Debug(fmt.Sprintf(
			"wait task '%v' context down err: %v, deadlin %v now:%v",
			tk.Key, ctx.Err(), dead, time.Now(),
		))
		return ctx.Err()
	case <-tk.cd:
		return nil
	}
}

// not do any occupy
func (tk *Handler) onlyWait(ctx context.Context) error {
	for {
		var occupied bool
		tk.mx.RLock()
		occupied = tk.cd != nil
		tk.mx.RUnlock()

		if occupied {
			break
		} else {
			select {
			case <- ctx.Done():
				return ctx.Err()
			case <- time.NewTimer(100 * time.Millisecond).C:
			}
		}
	}
	return tk.wait(ctx)
}



// occupy this Resource id. Return true means occupy resource success (response for handle resource update), or wait until other handler finish.
func (tk *Handler) occupyOrWait(ctx context.Context) (bool, error) {
	tk.mx.Lock()
	if tk.occupy() {
		tk.mx.Unlock()
		return true, nil
	} else {
		tk.mx.Unlock()
		return false, tk.wait(ctx)
	}
}

func (tk *Handler) occupyOrNotWait() bool {
	tk.mx.Lock()
	if tk.occupy() {
		tk.mx.Unlock()
		return true
	} else {
		tk.mx.Unlock()
		return false
	}
}

// release the resource	handler, other task can handle this resource
func (tk *Handler) release() {
	tk.mx.Lock()
	if tk.cd == nil {
		tk.mx.Unlock()
		return
	}
	select {
	case <-tk.cd:
	default:
		close(tk.cd)
	}
	tk.mx.Unlock()
	return
}

var handlerPool = &sync.Pool{New: func() interface{} { return &Handler{} }}

type ResourceHandlers struct {
	m *sync.Map
}

func NewResourceHandlers() *ResourceHandlers {
	return &ResourceHandlers{
		m: &sync.Map{},
	}
}

func (handlers *ResourceHandlers) SetHandler(handler ResourceHandler) {
	handlers.m.Store(handler.Category, handler)
}

func (handlers *ResourceHandlers) FetchFromStore(ctx context.Context, id ResourceKey) (*Resource, error) {
	h, ok := handlers.getHandler(id.Category)
	if !ok {
		return nil, ErrNotSupportCategory
	}
	if h.FetchFromStoreFunc != nil {
		return h.FetchFromStoreFunc(ctx, id)
	}
	return nil, ErrNotSupportCategory
}

func (handlers *ResourceHandlers) UpdateCache(ctx context.Context, data Resource) error {
	h, ok := handlers.getHandler(data.Key.Category)
	if !ok {
		return ErrNotSupportCategory
	}
	if h.UpdateCacheFunc != nil {
		return h.UpdateCacheFunc(ctx, data)
	}
	return ErrNotSupportCategory
}

func (handlers *ResourceHandlers) FindFromCache(ctx context.Context, id ResourceKey) (*Resource, error) {
	h, ok := handlers.getHandler(id.Category)
	if !ok {
		return nil, ErrNotSupportCategory
	}
	if h.FindFromCacheFunc != nil {
		return h.FindFromCacheFunc(ctx, id)
	}
	return nil, ErrNotSupportCategory
}

func (handlers *ResourceHandlers) DisableCache(ctx context.Context, id ResourceKey) error {
	h, ok := handlers.getHandler(id.Category)
	if !ok {
		return ErrNotSupportCategory
	}
	if h.DisableCache != nil {
		return h.DisableCache(ctx, id)
	}
	return ErrNotSupportCategory
}

func (handlers *ResourceHandlers) BeforeResourceCache(key ResourceKey) {
	h, ok := handlers.getHandler(key.Category)
	if !ok {
		return
	}
	if h.BeforeResourceCacheFunc != nil {
		h.BeforeResourceCacheFunc(key)
		return
	}
	return
}

func (handlers *ResourceHandlers) getHandler(category string) (ResourceHandler, bool) {
	v, ok := handlers.m.Load(category)
	if !ok {
		if category != "" {
			return ResourceHandler{}, false

		}
		v, ok = handlers.m.Load("")
		if !ok {
			return ResourceHandler{}, false
		}
	}
	hd, ok := v.(ResourceHandler)
	return hd, ok
}

type ResourceHandler struct {
	Category                string
	FetchFromStoreFunc      func(ctx context.Context, id ResourceKey) (*Resource, error)
	UpdateCacheFunc         func(ctx context.Context, data Resource) error
	FindFromCacheFunc       func(ctx context.Context, id ResourceKey) (*Resource, error)
	DisableCache            func(ctx context.Context, id ResourceKey) error
	BeforeResourceCacheFunc func(key ResourceKey)
}
