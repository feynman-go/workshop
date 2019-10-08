package cache

import (
	"context"
	"github.com/feynman-go/workshop/record"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"log"
	"sync"
	"time"
)

func ExampleBasicUse() {
	var m = &sync.Map{}

	handler := ResourceHandler{
		ThroughLimit: rate.NewLimiter(rate.Every(100 *time.Millisecond), 10), // 全局的缓存击穿限流
		FindFromCacheFunc: func(ctx context.Context, request Request) (*Resource, error) {
			// 从缓存中获取数据
			v, ok := m.Load(request.ID)
			if !ok { // 如果数据不存在则返回空
				return nil, nil
			}
			return &Resource{
				Data: v,
			}, nil
		},
		UpdateCacheFunc: func(ctx context.Context, request Request, res Resource) (*Resource, error) {
			// 跟新缓存数据
			m.Store(request.ID, res.Data)
			return &res, nil
		},
		FetchFromStoreFunc: func(ctx context.Context, request Request) (*Resource, error) {
			// 从数据库（上游获取数据）
			// data = database.query()
			var data interface{}
			return &Resource{
				Data: data,
			}, nil
		},
		ThroughOnCacheErrFunc: func(ctx context.Context, resource Request, err error) (goThrough bool) {
			// 缓存失效的时候是否需要击穿，全局的击穿限流依然保持
			if err == errors.New("good err") {
				return true
			}
			return false
		},
		Downgrade: func(ctx context.Context, resource Request, err error) (*Resource, error) {
			// 缓存失效的时候是否需要击穿，全局的击穿限流依然保持
			if err == errors.New("good err") {
				return &Resource{
					Data: "default value",
				}, nil
			}
			return nil, err
		},
	}

	factory := record.EasyRecorders("test-cache-record")

	mid := NewRecorderMid(factory)

	rep := NewRepository(handler, zap.L(), mid)
	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	res, err := rep.Find(ctx, Request{
		ID: "1",
	})

	if err != nil {
		log.Println("find res is err")
		return
	}

	if res == nil {
		log.Println("can not found resource")
		return
	}

	log.Println("did get data:", res.Data)
}
