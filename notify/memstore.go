package notify

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/MauriceGit/skiplist"
	"github.com/feynman-go/workshop/syncrun/routine"
	"github.com/pkg/errors"
	"sync"
	"time"
)

var _ OutputCursor = (*MemoCursor)(nil)

type MemoCursor struct {
	rw sync.RWMutex
	stream *MemoMessageStream
	newItemCursor chan struct{}
	e *skiplist.SkipListElement
	closed chan struct{}
	err error
	startIndex uint64
}

func (cursor *MemoCursor) Next(ctx context.Context) *Notification {
	tk := time.NewTicker(time.Second)
	defer tk.Stop()
	for ctx.Err() == nil {
		var next *skiplist.SkipListElement
		if cursor.e == nil {
			next = cursor.stream.firstElement(cursor.startIndex)
		} else {
			next = cursor.stream.nextElement(cursor.e)
		}

		if next == nil {
			select {
			case <- cursor.newItemCursor:
			case <- cursor.stream.Closed():
				cursor.rw.Lock()
				if cursor.err != nil {
					cursor.err = errors.New("stream closed")
				}
				cursor.rw.Unlock()
				return nil
			case <- cursor.closed:
				return nil
			case <- tk.C:
				continue
			case <- ctx.Done():
				return nil
			}
		} else {
			cursor.e = next
			return next.GetValue().(msgContainer).Ntf
		}
	}
	return nil
}

func (cursor *MemoCursor) CloseWithContext(ctx context.Context) error {
	cursor.rw.Lock()
	defer cursor.rw.Unlock()
	select {
	case <- cursor.closed:
		return errors.New("closed")
	default:
		close(cursor.closed)
		cursor.stream.ReleaseCursor(cursor)
		cursor.err = errors.New("closed")
	}
	return nil
}

func (cursor *MemoCursor) Err() error {
	return cursor.err
}

var _ OutputStream = (*MemoMessageStream)(nil)

type MemoMessageStream struct {
	rw sync.RWMutex
	ll *skiplist.SkipList
	index uint64
	pb *routine.Routine
	table map[*MemoCursor]bool
}

func NewMemoMessageStream() *MemoMessageStream {
	l := skiplist.New()
	stream := &MemoMessageStream {
		ll: &l,
		index: 0,
		table: map[*MemoCursor]bool{},
	}
	stream.pb = routine.New(stream.run)
	stream.pb.Start()
	stream.table = map[*MemoCursor]bool{}
	return stream
}

func (stream *MemoMessageStream) Push(data interface{}) uint64 {
	stream.rw.Lock()

	var idx uint64
	last := stream.ll.GetLargestNode()
	if last != nil {
		ctn := last.GetValue().(msgContainer)
		idx = ctn.Index + 1
	} else {
		idx = 1
	}

	output := &Notification{
		OffsetToken: stream.formatIndex(idx),
		Data: data,
		CreateTime: time.Now(),
	}

	stream.ll.Insert(msgContainer{
		Index: idx,
		Ntf:   output,
	})

	stream.rw.Unlock()

	go func() {
		stream.rw.RLock()
		defer stream.rw.RUnlock()

		for cursor := range stream.table {
			select {
			case cursor.newItemCursor <- struct{}{}:
			default:
			}
		}
	}()
	return idx
}

func (stream *MemoMessageStream) FetchOutputCursor(ctx context.Context) (OutputCursor, error) {
	stream.rw.RLock()
	index := stream.index
	stream.rw.RUnlock()
	cursor := &MemoCursor{
		stream: stream,
		err: nil,
		closed: make(chan struct{}),
		newItemCursor: make(chan struct{}, 2),
		startIndex: index,
	}

	stream.rw.Lock()
	defer stream.rw.Unlock()

	stream.table[cursor] = true
	return cursor, nil
}

func (stream *MemoMessageStream) Closed() <- chan struct{}{
	return stream.pb.Stopped()
}

func (stream *MemoMessageStream) Close() error {
	stream.rw.Lock()
	defer stream.rw.Unlock()

	stream.table = nil
	stream.pb.Stop()
	return nil
}

func (stream *MemoMessageStream) formatIndex(index uint64) string {
	bs := [8]byte{}
	binary.BigEndian.PutUint64(bs[:], index)
	return base64.RawStdEncoding.EncodeToString(bs[:])
}

func (stream *MemoMessageStream) parseToken(resumeToken string) (uint64, error) {
	bs, err := base64.RawStdEncoding.DecodeString(resumeToken)
	if err != nil {
		return 0, err
	}
	index := binary.BigEndian.Uint64(bs[:])
	return index, nil
}

func (stream *MemoMessageStream) CommitOutput(ctx context.Context, msgs []Notification) error {
	var maxIdx uint64 = 0
	for _, m := range msgs {
		idx, err := stream.parseToken(m.OffsetToken)
		if err != nil {
			return err
		}
		if idx != 0 && idx > maxIdx {
			maxIdx = idx
		}
	}

	stream.rw.Lock()
	stream.index = maxIdx
	stream.rw.Unlock()
	return nil
}

func (stream *MemoMessageStream) run(ctx context.Context) {
	tk := time.NewTicker(time.Second)
	defer tk.Stop()

	for {
		stream.rw.RLock()
		idx := stream.index
		e := stream.ll.GetSmallestNode()
		stream.rw.RUnlock()

		select {
		case <- ctx.Done():
			return
		case <- tk.C:
			for ; e != nil; e = stream.nextElement(e) {
				ctn := e.GetValue().(msgContainer)
				if ctn.Index < idx {
					stream.rw.Lock()
					stream.ll.Delete(ctn)
					stream.rw.Unlock()
				}
			}
		}
	}
}

func (stream *MemoMessageStream) firstElement(index uint64) *skiplist.SkipListElement {
	stream.rw.RLock()
	defer stream.rw.RUnlock()

	if index == 0 {
		return stream.ll.GetSmallestNode()
	}

	e, _ := stream.ll.FindGreaterOrEqual(msgContainer{
		Index: index,
	})

	return e
}

func (stream *MemoMessageStream) nextElement(from *skiplist.SkipListElement) *skiplist.SkipListElement {
	stream.rw.RLock()
	defer stream.rw.RUnlock()
	if from == nil {
		return stream.ll.GetSmallestNode()
	}

	next := stream.ll.Next(from)
	if next == stream.ll.GetSmallestNode() {
		next = nil
	}
	return next
}

func (stream *MemoMessageStream) GetResumeToken(ctx context.Context) (token string, err error) {
	stream.rw.RLock()
	index := stream.index
	stream.rw.RUnlock()
	return stream.formatIndex(index), nil
}

func (stream *MemoMessageStream) ReleaseCursor(cursor *MemoCursor) {
	stream.rw.Lock()
	defer stream.rw.Unlock()
	delete(stream.table, cursor)
}

func (stream *MemoMessageStream) GetLastIndex() uint64 {
	stream.rw.RLock()
	defer stream.rw.RUnlock()

	b := stream.ll.GetLargestNode()
	if b != nil {
		ctn := b.GetValue().(msgContainer)
		return ctn.Index
	}
	return 0
}


type msgContainer struct {
	Index uint64
	Ntf   *Notification
}

func (m msgContainer) ExtractKey() float64 {
	return float64(m.Index)
}

func (m msgContainer) String() string {
	return fmt.Sprint(m.Index)
}