package streamz

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStreams(t *testing.T) {

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	s := NewSource(ctx)

	s.ErrorPresenter(func(f Frame, err error) {
		fmt.Println(err)
	})

	a := s.Map(func(in Frame) (out Frame, err error) {
		fmt.Println("map")
		return out, nil
	}).Map(func(in Frame) (Frame, error) {
		fmt.Println("hallo")
		return in, nil
	}).Filter(func(in Frame) bool {
		return true
	})

	a.Sink(func(in Frame) error {
		fmt.Println("a")
		return nil
	})

	a.Map(func(in Frame) (Frame, error) {
		fmt.Println("ok")
		return in, nil
	}).Sink(func(in Frame) error {
		fmt.Println("b")
		return nil
	})

	cancel()

	for i := 0; i < 10; i++ {
		f := NewFrame()
		s.Emit(f)
	}

}

func TestMapCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	s := NewSource(ctx)

	s.Map(func(in Frame) (Frame, error) {
		return in, nil
	}).Sink(func(in Frame) error {
		time.Sleep(time.Second * 2)
		return nil
	})

	s.Emit(NewFrame())
	s.Emit(NewFrame())
	s.Emit(NewFrame())
	time.Sleep(time.Millisecond * 500)
	cancel()
}

func TestReliefValve(t *testing.T) {
	assert := assert.New(t)

	s := NewSource(context.TODO())

	s.RateLimit(time.Millisecond * 10).ReliefValve(time.Millisecond * 10).Map(func(in Frame) (Frame, error) {
		return in, nil
	}).Sink(func(in Frame) error {
		time.Sleep(time.Second * 1)
		return nil
	})

	for i := 0; i < 10; i++ {
		f := NewFrame()
		assert.NoError(s.Emit(f))
	}

	time.Sleep(time.Second * 5)

}

func TestTimedWindow(t *testing.T) {
	assert := assert.New(t)
	s := NewSource(context.TODO())

	s.RateLimit(time.Millisecond*200).TimedWindow(time.Second, func(in []Frame) (Frame, error) {
		fmt.Println("Got Frames:", len(in))
		out := NewFrame()
		return out, nil
	}).Sink(func(in Frame) error {
		return nil
	})

	for i := 0; i < 10; i++ {
		f := NewFrame()
		assert.NoError(s.Emit(f))
	}

	time.Sleep(time.Second * 5)
}

func loadInt(sm *sync.Map, key string) int {
	v, _ := sm.LoadOrStore(key, 0)
	return v.(int)
}
func TestAccumulate(t *testing.T) {
	assert := assert.New(t)
	s := NewSource(context.TODO())

	s.RateLimit(time.Millisecond * 200).Accumulate(func(in Frame, state *sync.Map) (Frame, error) {
		counter := loadInt(state, "counter")
		counter++
		state.Store("counter", counter)
		fmt.Println(counter)
		return nil, nil
	}).Sink(func(in Frame) error {
		return nil
	})

	for i := 0; i < 10; i++ {
		f := NewFrame()
		assert.NoError(s.Emit(f))
	}

	time.Sleep(time.Second * 5)
}
