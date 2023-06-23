package EventBus

import (
	"fmt"
	"github.com/dingyaguang117/go-hooker/hooker"
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
	AddHook(hook BusHook)
}

type BusExecuteFunc func(handler *EventHandler, topic string, args ...interface{}) (resultError error)

type BusHook hooker.Hook[BusExecuteFunc]

// Bus englobes global (subscribe, publish, control) bus behavior
type Bus interface {
	BusController
	BusSubscriber
	BusPublisher
	BusHooker
}

// EventBus - box for handlers and callbacks.
type EventBus struct {
	handlers map[string][]*EventHandler
	hooker   *hooker.Hooker[BusExecuteFunc]
	lock     sync.Mutex // a lock for the map
	wg       sync.WaitGroup
}

type EventHandler struct {
	callBack      reflect.Value
	once          *sync.Once
	async         bool
	transactional bool
	sync.Mutex                   // lock for an event handler - useful for running async callbacks serially
	errorHandlers []ErrorHandler // error handlers
	retryCount    int
	retryDelay    time.Duration
}

func (handler *EventHandler) GetCallbackName() string {
	return runtime.FuncForPC(handler.callBack.Pointer()).Name()
}

// Option is a function to set EventHandler's properties that can effect the behavior of Publish
type Option func(*EventHandler)

// OptionOnError adds an error handler to the event handler
func OptionOnError(errorHandler ErrorHandler) Option {
	return func(handler *EventHandler) {
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
	return func(handler *EventHandler) {
		handler.retryCount = retryCount
		handler.retryDelay = retryDelay
	}
}

// New returns new EventBus with empty handlers.
func New() Bus {
	b := &EventBus{
		make(map[string][]*EventHandler),
		nil,
		sync.Mutex{},
		sync.WaitGroup{},
	}
	h := hooker.NewHooker[BusExecuteFunc](b.doExecCallback)
	b.hooker = h
	return Bus(b)
}

func (bus *EventBus) AddHook(hook BusHook) {
	bus.hooker.AddHook(hooker.Hook[BusExecuteFunc](hook))
}

// doSubscribe handles the subscription logic and is utilized by the public Subscribe functions
func (bus *EventBus) doSubscribe(topic string, fn interface{}, handler *EventHandler) error {
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
	h := &EventHandler{
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
	h := &EventHandler{
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
	h := &EventHandler{
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
	h := &EventHandler{
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
	copyHandlers := make([]*EventHandler, len(handlers))
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

func (bus *EventBus) doPublish(handler *EventHandler, topic string, args ...interface{}) {
	if handler.once == nil {
		_ = bus.doExecCallbackWithHook(handler, topic, args...)
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
			_ = bus.doExecCallbackWithHook(handler, topic, args...)
		})
	}
}

func (bus *EventBus) doExecCallbackWithHook(handler *EventHandler, topic string, args ...interface{}) (resultError error) {
	return bus.hooker.GetWrapped()(handler, topic, args...)
}

func (bus *EventBus) doExecCallback(handler *EventHandler, topic string, args ...interface{}) (resultError error) {
	passedArguments := bus.setUpPublish(handler, args...)
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
	return resultError
}

func (bus *EventBus) doPublishAsync(handler *EventHandler, topic string, args ...interface{}) {
	defer bus.wg.Done()
	if handler.transactional {
		defer handler.Unlock()
	}
	bus.doPublish(handler, topic, args...)
}

func (bus *EventBus) handleError(err error, eventHandler *EventHandler, topic string, args ...interface{}) {
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

func (bus *EventBus) setUpPublish(callback *EventHandler, args ...interface{}) []reflect.Value {
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
