package EventBus

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"time"
)

// BusSubscriber defines subscription-related bus behavior
type BusSubscriber interface {
	Subscribe(topic string, fn interface{}, options ...Option) error
	SubscribeAsync(topic string, fn interface{}, transactional bool, options ...Option) error
	SubscribeOnce(topic string, fn interface{}, options ...Option) error
	SubscribeOnceAsync(topic string, fn interface{}, options ...Option) error
	Unsubscribe(topic string, handler interface{}) error
}

// BusPublisher defines publishing-related bus behavior
type BusPublisher interface {
	Publish(topic string, args ...interface{})
	PublishWithCallbackName(topic string, callbackName string, args ...interface{})
}

// BusController defines bus control behavior (checking handler's presence, synchronization)
type BusController interface {
	HasCallback(topic string) bool
	WaitAsync()
}

type BusHooker interface {
	AddBeforeExecuteHook(fn BeforeExecuteHookHandler)
	AddAfterExecuteHook(fn AfterExecuteHookHandler)
}

// Bus englobes global (subscribe, publish, control) bus behavior
type Bus interface {
	BusController
	BusSubscriber
	BusPublisher
	BusHooker
}

// EventBus - box for handlers and callbacks.
type EventBus struct {
	handlers           map[string][]*eventHandler
	beforeExecuteHooks []BeforeExecuteHookHandler
	afterExecuteHooks  []AfterExecuteHookHandler
	lock               sync.Mutex // a lock for the map
	wg                 sync.WaitGroup
}

type eventHandler struct {
	callBack      reflect.Value
	once          *sync.Once
	async         bool
	transactional bool
	sync.Mutex                   // lock for an event handler - useful for running async callbacks serially
	errorHandlers []ErrorHandler // error handlers
	retryCount    int
	retryDelay    time.Duration
}

func (handler *eventHandler) GetCallbackName() string {
	return runtime.FuncForPC(handler.callBack.Pointer()).Name()
}

type BeforeExecuteHookHandler func(handler *eventHandler, topic string, args []interface{})

type AfterExecuteHookHandler func(handler *eventHandler, topic string, args []interface{}, result error)

// Option is a function to set eventHandler's properties that can effect the behavior of Publish
type Option func(*eventHandler)

// OptionOnError adds an error handler to the event handler
func OptionOnError(errorHandler ErrorHandler) Option {
	return func(handler *eventHandler) {
		handler.errorHandlers = append(handler.errorHandlers, errorHandler)
	}
}

type HandlingError struct {
	Topic        string
	CallBack     reflect.Value
	CallBackName string
	Err          error
	Args         []interface{}
}

type ErrorHandler func(err *HandlingError)

// OptionRetry adds retry logic to the event handler
func OptionRetry(retryCount int, retryDelay time.Duration) Option {
	return func(handler *eventHandler) {
		handler.retryCount = retryCount
		handler.retryDelay = retryDelay
	}
}

// New returns new EventBus with empty handlers.
func New() Bus {
	b := &EventBus{
		make(map[string][]*eventHandler),
		nil,
		nil,
		sync.Mutex{},
		sync.WaitGroup{},
	}
	return Bus(b)
}

func (bus *EventBus) AddBeforeExecuteHook(fn BeforeExecuteHookHandler) {
	bus.beforeExecuteHooks = append(bus.beforeExecuteHooks, fn)
}

func (bus *EventBus) AddAfterExecuteHook(fn AfterExecuteHookHandler) {
	bus.afterExecuteHooks = append(bus.afterExecuteHooks, fn)
}

// doSubscribe handles the subscription logic and is utilized by the public Subscribe functions
func (bus *EventBus) doSubscribe(topic string, fn interface{}, handler *eventHandler) error {
	bus.lock.Lock()
	defer bus.lock.Unlock()
	if !(reflect.TypeOf(fn).Kind() == reflect.Func) {
		return fmt.Errorf("%s is not of type reflect.Func", reflect.TypeOf(fn).Kind())
	}
	bus.handlers[topic] = append(bus.handlers[topic], handler)
	return nil
}

// Subscribe subscribes to a topic.
// Returns error if `fn` is not a function.
func (bus *EventBus) Subscribe(topic string, fn interface{}, options ...Option) error {
	h := &eventHandler{
		callBack: reflect.ValueOf(fn),
	}
	for _, option := range options {
		option(h)
	}
	return bus.doSubscribe(topic, fn, h)
}

// SubscribeAsync subscribes to a topic with an asynchronous callback
// Transactional determines whether subsequent callbacks for a topic are
// run serially (true) or concurrently (false)
// Returns error if `fn` is not a function.
func (bus *EventBus) SubscribeAsync(topic string, fn interface{}, transactional bool, options ...Option) error {
	h := &eventHandler{
		callBack:      reflect.ValueOf(fn),
		async:         true,
		transactional: transactional,
	}
	for _, option := range options {
		option(h)
	}
	return bus.doSubscribe(topic, fn, h)
}

// SubscribeOnce subscribes to a topic once. Handler will be removed after executing.
// Returns error if `fn` is not a function.
func (bus *EventBus) SubscribeOnce(topic string, fn interface{}, options ...Option) error {
	h := &eventHandler{
		callBack: reflect.ValueOf(fn), once: new(sync.Once),
	}
	for _, option := range options {
		option(h)
	}
	return bus.doSubscribe(topic, fn, h)
}

// SubscribeOnceAsync subscribes to a topic once with an asynchronous callback
// Handler will be removed after executing.
// Returns error if `fn` is not a function.
func (bus *EventBus) SubscribeOnceAsync(topic string, fn interface{}, options ...Option) error {
	h := &eventHandler{
		callBack: reflect.ValueOf(fn), once: new(sync.Once), async: true,
	}
	for _, option := range options {
		option(h)
	}
	return bus.doSubscribe(topic, fn, h)
}

// HasCallback returns true if exists any callback subscribed to the topic.
func (bus *EventBus) HasCallback(topic string) bool {
	bus.lock.Lock()
	defer bus.lock.Unlock()
	_, ok := bus.handlers[topic]
	if ok {
		return len(bus.handlers[topic]) > 0
	}
	return false
}

// Unsubscribe removes callback defined for a topic.
// Returns error if there are no callbacks subscribed to the topic.
func (bus *EventBus) Unsubscribe(topic string, handler interface{}) error {
	bus.lock.Lock()
	defer bus.lock.Unlock()
	if _, ok := bus.handlers[topic]; ok && len(bus.handlers[topic]) > 0 {
		bus.removeHandler(topic, bus.findHandlerIdx(topic, reflect.ValueOf(handler)))
		return nil
	}
	return fmt.Errorf("topic %s doesn't exist", topic)
}

// Publish executes callback defined for a topic. Any additional argument will be transferred to the callback.
func (bus *EventBus) Publish(topic string, args ...interface{}) {
	bus.PublishWithCallbackName(topic, "", args...)
}

// PublishWithCallbackName executes specified callback defined for a topic.
// Any additional argument will be transferred to the callback.
func (bus *EventBus) PublishWithCallbackName(topic string, callbackName string, args ...interface{}) {
	// Handlers slice may be changed by removeHandler and Unsubscribe during iteration,
	// so make a copy and iterate the copied slice.
	bus.lock.Lock()
	handlers := bus.handlers[topic]
	copyHandlers := make([]*eventHandler, len(handlers))
	copy(copyHandlers, handlers)
	bus.lock.Unlock()
	for _, handler := range copyHandlers {
		// if callbackName is specified, only execute the callback with the same name
		if callbackName != "" && handler.GetCallbackName() != callbackName {
			continue
		}
		if !handler.async {
			bus.doPublish(handler, topic, args...)
		} else {
			bus.wg.Add(1)
			if handler.transactional {
				handler.Lock()
			}
			go bus.doPublishAsync(handler, topic, args...)
		}
	}
}

func (bus *EventBus) doPublish(handler *eventHandler, topic string, args ...interface{}) {
	if handler.once == nil {
		bus.doExecCallback(handler, topic, args...)
	} else {
		handler.once.Do(func() {
			bus.lock.Lock()
			for idx, h := range bus.handlers[topic] {
				// compare pointers since pointers are unique for all members of slice
				if h.once == handler.once {
					bus.removeHandler(topic, idx)
					break
				}
			}
			bus.lock.Unlock()
			bus.doExecCallback(handler, topic, args...)
		})
	}
}

func (bus *EventBus) doExecCallback(handler *eventHandler, topic string, args ...interface{}) {
	var resultError error
	passedArguments := bus.setUpPublish(handler, args...)

	for _, hook := range bus.beforeExecuteHooks {
		hook(handler, topic, args)
	}

	defer func() {
		for _, hook := range bus.afterExecuteHooks {
			hook(handler, topic, args, resultError)
		}
	}()

	// retry logic
	for i := 0; i < handler.retryCount+1; i++ {
		result := handler.callBack.Call(passedArguments)

		// when the first return value is nil, then do not call error handlers
		if len(result) == 0 || result[0].IsNil() {
			return
		}

		// when the first return value is error, set resultError
		if len(result) > 0 {
			if err, ok := result[0].Interface().(error); ok {
				resultError = err
			}
		}

		time.Sleep(handler.retryDelay)
	}
	if resultError != nil {
		bus.handleError(resultError, handler, topic, args...)
	}
}

func (bus *EventBus) doPublishAsync(handler *eventHandler, topic string, args ...interface{}) {
	defer bus.wg.Done()
	if handler.transactional {
		defer handler.Unlock()
	}
	bus.doPublish(handler, topic, args...)
}

func (bus *EventBus) handleError(err error, eventHandler *eventHandler, topic string, args ...interface{}) {
	for _, handler := range eventHandler.errorHandlers {
		handler(&HandlingError{
			Topic:        topic,
			CallBack:     eventHandler.callBack,
			CallBackName: eventHandler.GetCallbackName(),
			Err:          err,
			Args:         args,
		})
	}
}

func (bus *EventBus) removeHandler(topic string, idx int) {
	if _, ok := bus.handlers[topic]; !ok {
		return
	}
	l := len(bus.handlers[topic])

	if !(0 <= idx && idx < l) {
		return
	}

	copy(bus.handlers[topic][idx:], bus.handlers[topic][idx+1:])
	bus.handlers[topic][l-1] = nil // or the zero value of T
	bus.handlers[topic] = bus.handlers[topic][:l-1]
}

func (bus *EventBus) findHandlerIdx(topic string, callback reflect.Value) int {
	if _, ok := bus.handlers[topic]; ok {
		for idx, handler := range bus.handlers[topic] {
			if handler.callBack.Type() == callback.Type() &&
				handler.callBack.Pointer() == callback.Pointer() {
				return idx
			}
		}
	}
	return -1
}

func (bus *EventBus) setUpPublish(callback *eventHandler, args ...interface{}) []reflect.Value {
	funcType := callback.callBack.Type()
	passedArguments := make([]reflect.Value, len(args))
	for i, v := range args {
		if v == nil {
			passedArguments[i] = reflect.New(funcType.In(i)).Elem()
		} else {
			passedArguments[i] = reflect.ValueOf(v)
		}
	}

	return passedArguments
}

// WaitAsync waits for all async callbacks to complete
func (bus *EventBus) WaitAsync() {
	bus.wg.Wait()
}
