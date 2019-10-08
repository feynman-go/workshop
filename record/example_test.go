package record

import (
	"context"
	"github.com/pkg/errors"
	"time"
)

func ExampleBasicUse() {
	factory := EasyRecorders("test-record")

	func(ctx context.Context) {
		var err error
		recorder, ctx := factory.ActionRecorder(ctx, "test-function") // 生成一个记录
		defer func() {
			recorder.Commit(err, BoolField("remote err", true)) // 提交这个记录
		}()
		// err = doSomething(ctx)
	}(context.Background())
}

func ExampleWrap() {
	factory := EasyRecorders("test-record")

	type operation func (ctx context.Context) error

	wrap := func(oper operation) operation {
		return func (ctx context.Context) error {
			var err error
			recorder, ctx := factory.ActionRecorder(ctx, "test-function") // 生成一个记录
			defer func() {
				recorder.Commit(err) // 提交这个记录
			}()
			err = oper(ctx)
			return err
		}
	}

	wrapped := wrap(func(ctx context.Context) error {
		// 真正的业务逻辑
		return errors.New("err occur")
	})

	wrapped(context.Background())
	return
}

func ExampleRecordTimeOut() {
	factory := EasyRecorders("test-record")

	// factory wrapper, 包装一个正常的Factory对象。
	factory = FactoryWrapper{
		Wrapper: &MaxDurationWrapper{time.Second}, // 这个拦截器可以保证在超过time.Second 的时候自动提交记录，不用等真正结束。
		Factory: factory,
	}

	func(ctx context.Context) {
		var err error
		recorder, ctx := factory.ActionRecorder(ctx, "test-function") // 生成一个记录
		defer func() {
			recorder.Commit(err, BoolField("remote err", true)) // 提交这个记录
		}()
		// err = doSomethingCostTooMuchTime(ctx)
	}(context.Background())
}