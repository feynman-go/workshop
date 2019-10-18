package notify

import (
	"context"
	"testing"
	"time"
)

func TestNotifySend(t *testing.T) {
	stream := NewMemoMessageStream()
	iterator := NewIterator(stream, Option{})
	defer func() {
		iterator.CloseWithContext(context.Background())
	}()

	ctx, _ := context.WithTimeout(context.Background(), 300 * time.Millisecond)
	stream.Push(1)

	time.Sleep(100 * time.Millisecond)

	nf, err := iterator.Peek(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if nf.Data != 1 {
		t.Fatal("nf should not be zero")
	}

	stream.Push(2)
	stream.Push(3)
	time.Sleep(100 * time.Millisecond)

	nf, err = iterator.Peek(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if nf.Data != 2 {
		t.Fatal("nf should not be zero")
	}

	nf, err = iterator.Peek(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if nf.Data != 3 {
		t.Fatal("nf should not be zero")
	}

	ctx, _ = context.WithTimeout(context.Background(), 100 * time.Millisecond)
	start := time.Now()
	nf, err = iterator.Peek(ctx)
	if nf != nil {
		t.Fatal("peek should return empty")
	}

	if delta := time.Now().Sub(start); delta > 200 * time.Millisecond || delta < 100 * time.Millisecond {
		t.Fatal("bad delta")
	}

}

func TestNotifySendMultiAndReset(t *testing.T) {
	stream := NewMemoMessageStream()
	iterator := NewIterator(stream, Option{})
	defer func() {
		iterator.CloseWithContext(context.Background())
	}()

	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	stream.Push(1)

	time.Sleep(100 * time.Millisecond)
	nf, err := iterator.Peek(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if nf.Data != 1 {
		t.Fatal("nf should not be zero")
	}

	stream.Push(2)
	stream.Push(3)

	time.Sleep(100 * time.Millisecond)
	nf, err = iterator.Peek(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if nf.Data != 2 {
		t.Fatal("nf should not be zero")
	}

	nf, err = iterator.Peek(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if nf.Data != 3 {
		t.Fatal("nf should not be zero")
	}

	iterator.ResetPeeked()

	nf, err = iterator.Peek(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if nf.Data != 1 {
		t.Fatal("nf should not be zero")
	}

	nf, err = iterator.Peek(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if nf.Data != 2 {
		t.Fatal("nf should not be zero")
	}

	nf, err = iterator.Peek(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if nf.Data != 3 {
		t.Fatal("nf should not be zero")
	}


	err = iterator.PullPeeked(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ctx, _ = context.WithTimeout(context.Background(), 100 * time.Millisecond)
	start := time.Now()
	nf, err = iterator.Peek(ctx)
	if nf != nil {
		t.Fatal("peek should return empty")
	}

	if delta := time.Now().Sub(start); delta > 200 * time.Millisecond || delta < 100 * time.Millisecond {
		t.Fatal("bad delta")
	}
}