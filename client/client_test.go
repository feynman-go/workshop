package client

import (
	"context"
	"github.com/feynman-go/workshop/record"
	"golang.org/x/time/rate"
	"log"
	"time"
)

func ExampleBasicUse() {
	var agent RecoverableAgent // = .....

	recordMid := NewRecorderMiddle(record.EasyRecorders("client-test"))

	// 基础的限流插件
	breakerMid := NewBasicBreakerMiddle(
		rate.NewLimiter(rate.Every(time.Second), 10),
		agent,
		time.Second, // 错误限流后等待的最少时间
		3 * time.Second, // 错误限流后最长的等待时间
	)

	opt := Option{}.
		SetParallelCount(10). // 并发数量
		AddMiddle(breakerMid, recordMid)

	client := New(agent, opt)

	err := client.Do(context.Background(), func(ctx context.Context, agent Agent) error {
		var err error
		// dbClient := agent.(*DbClient)
		// dbClient.query() ...
		// dbClient.Update() ...
		// return err
		return err
	}, ActionOption{}.
		SetPartition(1)) // 设置是否分区, 已经分区id, 分区能保证的是相同的分区id 同时只有一个在执行。

	if err != nil {
		log.Println("client do err:", err)
	}
}