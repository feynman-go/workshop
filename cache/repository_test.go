package cache

import (
	"context"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"sync"
	"testing"
	"time"
)

func TestRepositoryStoreAndGet(t *testing.T) {
	var store = &sync.Map{}
	store.Store(2, "2")
	store.Store(1, "1")

	var cache = &sync.Map{}
	cache.Store(1, "1")

	handler := ResourceHandler{
		ThroughLimit: rate.Every(time.Second / 250),
		FetchFromStoreFunc: func(ctx context.Context, request Request) (*Resource, error) {
			v, ok := store.Load(request.ID)
			if !ok {
				return nil, nil
			}
			return &Resource {
				Data: v,
			}, nil
		},
		UpdateCacheFunc: func(ctx context.Context, request Request, data Resource) (*Resource, error) {
			if data.Data == nil {
				return nil, nil
			}
			cache.Store(request.ID, data.Data)
			return &data, nil
		},
		FindFromCacheFunc: func(ctx context.Context, request Request) (*Resource, error){
			v, ok := cache.Load(request.ID)
			if !ok {
				return nil, nil
			}
			return &Resource{
				Data: v,
			}, nil
		},
	}

	rep := NewRepository(handler, zap.L())
	res, err := rep.Find(context.Background(), Request{
		ID: 1,
	})

	if err != nil {
		t.Fatal(err)
	}

	if res.Data != "1" {
		t.Fatal("valid data")
	}

	res, err = rep.Find(context.Background(), Request{
		ID: 2,
	})

	if err != nil {
		t.Fatal(err)
	}

	if res.Data != "2" {
		t.Fatal("valid data")
	}
}