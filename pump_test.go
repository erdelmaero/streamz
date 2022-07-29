package streamz

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPump(t *testing.T) {
	assert := assert.New(t)
	s := NewPump(context.Background(), func() (Frame, error) {
		f := NewFrame()
		return f, nil
	})

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
