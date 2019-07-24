package client

import (
	"context"
	"fmt"
	"github.com/feynman-go/workshop/breaker"
	"github.com/feynman-go/workshop/promise"
	"golang.org/x/time/rate"
	"log"
	"testing"
	"time"
)

func TestClientBasic(t *testing.T) {
	type mockClient struct {
		RequestCount *int
	}

	resPool := NewMemoryPool(1, Refresher(func(last interface{}) (new interface{}, err error) {
		var reqCount = 0
		return mockClient{
			RequestCount: &reqCount,
		}, nil
	}), rate.Every(time.Second), breaker.StatusConfig{
		AbnormalLimit: rate.Every(time.Second),
		ResetLimit: rate.Every(time.Second),
		AbnormalDuration: time.Second,
		RestoreCount: 10,
	})

	client := New(promise.NewPool(1), resPool)
	ctx := context.Background()

	action := Action{
		Partition: true,
		PartitionKey: 1,
		Do: func(ctx context.Context, resource *Resource) error {
			mc := resource.Get().(mockClient)
			*mc.RequestCount += 1
			log.Println("request count:", *mc.RequestCount)
			if *mc.RequestCount > 3 {
				return fmt.Errorf("two many request count '%v'", *mc.RequestCount)
			}
			return nil
		},
		Recover: func(ctx context.Context, resource *Resource, err error) (time.Duration, bool) {
			mc := resource.Get().(mockClient)
			t.Log("recover resource count:", *mc.RequestCount)
			resource.PutBack(true)
			return time.Second, true
		},
		MaxTimeOut: time.Second,
		MinTimeOut: 1 * time.Second,
	}

	for i := 0; i < 3; i++ {
		result := client.Do(ctx, action)
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	}

	result := client.Do(ctx, action)
	if result.Err != nil {
		t.Fatal(result.Err)
	}
}

func TestClientRecover(t *testing.T) {
	type mockClient struct {
		RequestCount *int
	}

	var reqCount = 0
	resPool := NewMemoryPool(1, Refresher(func(last interface{}) (new interface{}, err error) {
		log.Println("try refresh data")
		return mockClient {
			RequestCount: &reqCount,
		}, nil
	}), rate.Every(3 * time.Second), breaker.StatusConfig{
		AbnormalLimit: rate.Every(3 * time.Second),
		ResetLimit: rate.Every(time.Second),
		AbnormalDuration: time.Second,
		RestoreCount: 2,
	})

	client := New(promise.NewPool(1), resPool)
	ctx := context.Background()

	action := Action{
		Partition: true,
		PartitionKey: 1,
		Do: func(ctx context.Context, resource *Resource) error {
			mc := resource.Get().(mockClient)
			*mc.RequestCount += 1
			c := *mc.RequestCount
			log.Println("client request count is:", c)
			if c >= 5 {
				return fmt.Errorf("two many request count '%v'", *mc.RequestCount)
			}
			return nil
		},
		Recover: func(ctx context.Context, resource *Resource, err error) (time.Duration, bool) {
			mc := resource.Get().(mockClient)
			log.Println("recover resource count:", *mc.RequestCount)
			resource.PutBack(true)
			return time.Second, true
		},
		MaxTimeOut: time.Second,
		MinTimeOut: 1 * time.Second,
	}

	for i := 0; i < 100; i++ {
		time.Sleep(time.Second)
		result := client.Do(ctx, action)
		log.Println("get err", i , result.Err)
	}
}
