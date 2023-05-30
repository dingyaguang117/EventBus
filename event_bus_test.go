package EventBus

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	bus := New()
	if bus == nil {
		t.Log("New EventBus not created!")
		t.Fail()
	}
}

func TestHasCallback(t *testing.T) {
	bus := New()
	bus.Subscribe("topic", func() {})
	if bus.HasCallback("topic_topic") {
		t.Fail()
	}
	if !bus.HasCallback("topic") {
		t.Fail()
	}
}

func TestSubscribe(t *testing.T) {
	bus := New()
	if bus.Subscribe("topic", func() {}) != nil {
		t.Fail()
	}
	if bus.Subscribe("topic", "String") == nil {
		t.Fail()
	}
}

func TestSubscribeWithOptionOnError(t *testing.T) {
	var (
		handler1Called bool
		handler2Called bool
	)

	bus := New()

	errHandler1 := func(err *HandlingError) {
		t.Logf("handler1 called with error: %s", err)
		handler1Called = true
	}

	errHandler2 := func(err *HandlingError) {
		handler2Called = true
	}

	if bus.Subscribe("topic", func() error {
		return errors.New("test error")
	}, OptionOnError(errHandler1), OptionOnError(errHandler2)) != nil {
		t.Fail()
	}

	bus.Publish("topic")

	if !handler1Called {
		t.Logf("handler1 not called")
		t.Fail()
	}

	if !handler2Called {
		t.Logf("handler2 not called")
		t.Fail()
	}
}

func TestSubscribeWithRetry(t *testing.T) {
	var (
		retryCount       int = 1
		expectedTryCount int = retryCount + 1
		actualTryCount   int
		handlerCalled    bool
	)

	bus := New()

	errHandler := func(err *HandlingError) {
		handlerCalled = true
	}

	err := bus.Subscribe("topic", func() error {
		actualTryCount += 1
		return errors.New("test error")
	},
		OptionOnError(errHandler),
		OptionRetry(retryCount, time.Second),
	)

	if err != nil {
		t.Fail()
	}

	bus.Publish("topic")

	if expectedTryCount != actualTryCount {
		t.Logf("expected %d tries, got %d", expectedTryCount, actualTryCount)
		t.Fail()
	}

	if !handlerCalled {
		t.Logf("handler not called")
		t.Fail()
	}
}

func TestSubscribeWithRetryButNoErrors(t *testing.T) {
	var (
		retryCount       int = 10
		expectedTryCount int = 1
		actualTryCount   int
	)

	bus := New()

	err := bus.Subscribe("topic", func() error {
		actualTryCount += 1
		return nil
	},
		OptionRetry(retryCount, time.Second),
	)

	if err != nil {
		t.Fail()
	}

	bus.Publish("topic")

	if expectedTryCount != actualTryCount {
		t.Logf("expected %d tries, got %d", expectedTryCount, actualTryCount)
		t.Fail()
	}
}

func TestSubscribeOnce(t *testing.T) {
	bus := New()
	if bus.SubscribeOnce("topic", func() {}) != nil {
		t.Fail()
	}
	if bus.SubscribeOnce("topic", "String") == nil {
		t.Fail()
	}
}

func TestSubscribeOnceAndManySubscribe(t *testing.T) {
	bus := New()
	event := "topic"
	flag := 0
	fn := func() { flag += 1 }
	bus.SubscribeOnce(event, fn)
	bus.Subscribe(event, fn)
	bus.Subscribe(event, fn)
	bus.Publish(event)

	if flag != 3 {
		t.Fail()
	}
}

func TestUnsubscribe(t *testing.T) {
	bus := New()
	handler := func() {}
	bus.Subscribe("topic", handler)
	if bus.Unsubscribe("topic", handler) != nil {
		t.Fail()
	}
	if bus.Unsubscribe("topic", handler) == nil {
		t.Fail()
	}
}

type handler struct {
	val int
}

func (h *handler) Handle() {
	h.val++
}

func TestUnsubscribeMethod(t *testing.T) {
	bus := New()
	h := &handler{val: 0}

	bus.Subscribe("topic", h.Handle)
	bus.Publish("topic")
	if bus.Unsubscribe("topic", h.Handle) != nil {
		t.Fail()
	}
	if bus.Unsubscribe("topic", h.Handle) == nil {
		t.Fail()
	}
	bus.Publish("topic")
	bus.WaitAsync()

	if h.val != 1 {
		t.Fail()
	}
}

func TestPublish(t *testing.T) {
	bus := New()
	bus.Subscribe("topic", func(a int, err error) {
		if a != 10 {
			t.Fail()
		}

		if err != nil {
			t.Fail()
		}
	})
	bus.Publish("topic", 10, nil)
}

var (
	handler1Called bool
	handler2Called bool
	handler3Called bool
)

func handler1() {
	handler1Called = true
}
func handler2() {
	handler2Called = true
}
func handler3() {
	handler3Called = true
}

func TestPublishWithCallbackName(t *testing.T) {

	bus := New()
	bus.Subscribe("topic", handler1)
	bus.Subscribe("topic", handler2)
	bus.Subscribe("topic", handler3)
	bus.PublishWithCallbackName("topic", "github.com/dingyaguang117/EventBus.handler1")

	if !handler1Called {
		t.Logf("handler1 not called")
		t.Fail()
	}
	if handler2Called {
		t.Logf("handler2 called")
		t.Fail()
	}
	if handler3Called {
		t.Logf("handler3 called")
		t.Fail()
	}
}

func TestSubcribeOnceAsync(t *testing.T) {
	results := make([]int, 0)

	bus := New()
	bus.SubscribeOnceAsync("topic", func(a int, out *[]int) {
		*out = append(*out, a)
	})

	bus.Publish("topic", 10, &results)
	bus.Publish("topic", 10, &results)

	bus.WaitAsync()

	if len(results) != 1 {
		t.Fail()
	}

	if bus.HasCallback("topic") {
		t.Fail()
	}
}

func TestSubscribeAsyncTransactional(t *testing.T) {
	results := make([]int, 0)

	bus := New()
	bus.SubscribeAsync("topic", func(a int, out *[]int, dur string) {
		sleep, _ := time.ParseDuration(dur)
		time.Sleep(sleep)
		*out = append(*out, a)
	}, true)

	bus.Publish("topic", 1, &results, "1s")
	bus.Publish("topic", 2, &results, "0s")

	bus.WaitAsync()

	if len(results) != 2 {
		t.Fail()
	}

	if results[0] != 1 || results[1] != 2 {
		t.Fail()
	}
}

func TestSubscribeAsync(t *testing.T) {
	results := make(chan int)

	bus := New()
	bus.SubscribeAsync("topic", func(a int, out chan<- int) {
		out <- a
	}, false)

	bus.Publish("topic", 1, results)
	bus.Publish("topic", 2, results)

	numResults := 0

	go func() {
		for _ = range results {
			numResults++
		}
	}()

	bus.WaitAsync()
	println(2)

	time.Sleep(10 * time.Millisecond)

	// todo race detected during execution of test
	//if numResults != 2 {
	//	t.Fail()
	//}
}

var (
	beforeExecuteHookCalled bool
	afterExecuteHookCalled  bool
)

func BeforeExecuteHook(topic string, callbackName string, args []interface{}) {
	beforeExecuteHookCalled = true
	fmt.Printf("BeforeExecuteHook, callbackName: %v, topic: %s, args: %v\n", callbackName, topic, args)
}

func AfterExecuteHook(topic string, callbackName string, args []interface{}, result error) {
	afterExecuteHookCalled = true
	fmt.Printf("BeforeExecuteHook, callbackName: %v, topic: %s, args: %v, result: %v\n", callbackName, topic, args, result)
}

func TestEventBus_AddBeforeExecuteHook(t *testing.T) {
	bus := New()
	bus.Subscribe("topic", handler1)
	bus.Subscribe("topic", handler2)
	bus.AddBeforeExecuteHook(BeforeExecuteHook)
	bus.AddAfterExecuteHook(AfterExecuteHook)
	bus.Publish("topic")

	if !beforeExecuteHookCalled {
		t.Logf("BeforeExecuteHook not called")
		t.Fail()
	}

	if !afterExecuteHookCalled {
		t.Logf("AfterExecuteHook not called")
		t.Fail()
	}
}
