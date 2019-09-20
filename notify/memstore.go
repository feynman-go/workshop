package notify

import (
	"container/list"
	"context"
	"encoding/base64"
	"encoding/binary"
	"sync"
)

type MemoCursor struct {
	stream *MemoMessageStream
	e *list.Element
	index uint64
	err error
}

func (cursor *MemoCursor) Next(ctx context.Context) *Message {

}

func (cursor *MemoCursor) Close(ctx context.Context) error {
	return nil
}

func (cursor *MemoCursor) Err() error {
	return cursor.err
}

func (cursor *MemoCursor) ResumeToken() string {
	return cursor.stream.formatIndex(cursor.index)
}

type MemoMessageStream struct {
	rw sync.RWMutex
	ll *list.List
	index uint64
}

func NewMemoMessageStream() *MemoMessageStream {
	return &MemoMessageStream {
		ll: list.New(),
	}
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

func (stream *MemoMessageStream) ResumeFromToken(ctx context.Context, resumeToken string) (Cursor, error) {
	bs, err := base64.RawStdEncoding.DecodeString(resumeToken)
	if err != nil {
		return nil, err
	}
	index := binary.BigEndian.Uint64(bs[:])


	return &MemoCursor{
		e:
		index: index,
		stream: stream,
		err: nil,
	}, nil
}

func (stream *MemoMessageStream) StoreResumeToken(ctx context.Context, token string) error {
	idx, err := stream.parseToken(token)
	if err != nil {
		return err
	}
	stream.rw.Lock()
	stream.index = idx
	stream.rw.Unlock()
	return nil
}

func (stream *MemoMessageStream) GetResumeToken(ctx context.Context) (token string, err error) {
	stream.rw.RLock()
	index := stream.index
	stream.rw.RUnlock()
	return stream.formatIndex(index), nil
}

func (stream *MemoMessageStream) GetLastIndex() uint64 {
	stream.rw.Lock()
	defer stream.rw.Unlock()

	b := stream.ll.Back()
	if b != nil {
		ctn := b.Value.(msgContainer)
		return ctn.Index
	}
	return 0
}

type msgContainer struct {
	Index uint64
}
