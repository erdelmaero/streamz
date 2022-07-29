package streamz

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSinkClose(t *testing.T) {
	assert := assert.New(t)

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	testOkCh := make(chan bool)

	s := NewSource(ctx)

	pipeline := s.Map(func(in Frame) (Frame, error) {
		return in, nil
	})

	si1 := pipeline.Sink(func(in Frame) error {
		testOkCh <- true
		return nil
	})
	si1.Close()

	pipeline.Sink(func(in Frame) error {
		testOkCh <- true
		return nil
	})

	si3 := pipeline.Sink(func(in Frame) error {
		testOkCh <- true
		return nil
	})
	si3.Close()

	s.Emit(NewFrame())

	_, hasValue := <-testOkCh
	assert.True(hasValue)

	assert.Never(func() bool {
		return <-testOkCh
	}, time.Second, time.Millisecond)

	si4, outCh := pipeline.SinkCh()
	defer si4.Close()

	go s.Emit(NewFrame())
	assert.Eventually(func() bool {
		<-outCh
		return true
	}, time.Second, time.Millisecond*10)

	cancel()
}
