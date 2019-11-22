package task

import (
	"context"
	"github.com/feynman-go/workshop/record"
	"log"
	"time"
)

func ExampleBasicUse() {
	scheduler := NewMemoScheduler(time.Minute) // 任务调度器，负责触发任务，任务存储
	executor := ExecutorFunc(func(ctx Context, res *Result) {
		// 执行业务逻辑的代码
		var err error
		// doSomething async
		if err == nil {
			res.SetFinish() //标记为结束
		} else {
			res.SetWaitAndReDo(3 * time.Minute)
		}
		return
	})

	mid := NewRecorderMiddle(record.EasyRecorders("task-record"))

	taskManager := NewManager(scheduler, executor, ManagerOption{
		MaxBusterTask: 10, // 同时并发执行的任务
		DefaultExecMaxDuration: time.Minute, //最大执行的任务的时间
		DefaultRecoverCount: 10, // 任务可以从失败中恢复的次数
		Middle: []Middle{mid}, // record mid
	})

	opt := Option{}. // 一个任务具体的选项
		SetOverLap(true). // 如果任务已经存在则可以覆盖这个任务. 会先尽量停止已有的任务，再开始新的任务, 否则会延续上个相同key的任务的状态
		SetExpectStartTime(time.Now().Add(time.Minute)) // 定时执行，这个任务可以被延后执行

	// 声明一个新的任务
	err := taskManager.ApplyNewTask(context.Background(), "task-key-1", opt)
	if err != nil {
		log.Println("apply new task err:", err)
	}
}