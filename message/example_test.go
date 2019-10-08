package message

import (
	"context"
	"log"
	"time"
)

func ExampleNotifierBasicUse() {
	// publisher 是一个推送的抽象接口
	var publisher Publisher = MockPublisher{
		PublishFunc: func(ctx context.Context, message []OutputMessage) error {
			log.Println("did push message")
			return nil
		},
	}

	var stream OutputStream = NewMemoMessageStream() // stream 可以中断并且重续，并从一个流节点开始

	notifier := New(stream, publisher, Option {
		MaxBlockCount: 1, // 可以合并请求的发送数量
		MaxBlockDuration: time.Second, // 可以合并请求的发送时间
	})

	notifier.Start()
	// wait or do other things
	notifier.Close()
}