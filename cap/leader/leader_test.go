package leader

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestSingleLeader(t *testing.T) {
	elector := NewMemoElector()
	member := NewMember(elector, ElectionFactoryFunc(func (ctx context.Context, sequence int64) (Election, error) {
		return Election{
			Sequence: sequence + 1,
			ElectID: "electID",
		}, nil
	}), Option{
		MaxElectDuration: 1 * time.Second,
		MinElectDuration: time.Second / 2,
		MaxKeepLiveDuration: 1 * time.Second,
		MinKeepLiveDuration: time.Second / 2,
		MaxExecDuration: 3 * time.Second,
		MinExecDuration: 1 * time.Second,
	})

	member.Start()

	var flag int32

	go member.SyncLeader(context.Background(), func(ctx context.Context) {
		atomic.AddInt32(&flag, 1)
	})

	time.Sleep(time.Second / 2 + time.Second)

	v := atomic.LoadInt32(&flag)
	if v != 1 {
		t.Fatal("expect atomic")
	}
}

func TestLeaderToFollower(t *testing.T) {
	elector := NewMemoElector()
	member1 := NewMember(elector, ElectionFactoryFunc(func (ctx context.Context, sequence int64) (Election, error) {
		return Election{
			Sequence: sequence + 1,
			ElectID: "electID1",
		}, nil
	}), Option{
		MaxElectDuration: 3 * time.Second,
		MinElectDuration: 3 * time.Second,
		MaxKeepLiveDuration: 5 * time.Second,
		MinKeepLiveDuration: 5 * time.Second,
		MaxExecDuration: 5 * time.Second,
		MinExecDuration: 5 * time.Second,
	})

	member2 := NewMember(elector, ElectionFactoryFunc(func (ctx context.Context, sequence int64) (Election, error) {
		return Election{
			Sequence: sequence + 1,
			ElectID: "electID2",
		}, nil
	}), Option{
		MaxElectDuration: time.Second,
		MinElectDuration: time.Second,
		MaxKeepLiveDuration: 5 * time.Second,
		MinKeepLiveDuration: 5 * time.Second,
		MaxExecDuration: 5 * time.Second,
		MinExecDuration: 5 * time.Second,
	})

	member1.Start()

	var start = make(chan struct{})
	var end = make(chan struct{})
	go member1.SyncLeader(context.Background(), func(ctx context.Context) {
		close(start)
		select {
		case <- ctx.Done():
		}
		close(end)
	})

	select {
	case <- start:
	}

	member2.Start()

	time.Sleep(time.Second * 3 + time.Second / 2)

	select {
	case <- end:
	default:
		t.Fatal("should closed")
	}
}