package streamz

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestFlatten(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	s := NewSource(ctx)

	s.Flatten(func(in Frame, out Emitter) error {
		var vals []string
		in.GetPayload(&vals)

		for _, val := range vals {
			f := NewFrame()
			f.SetPayload(val)
			out.Emit(f)
		}

		return nil
	}).Sink(func(in Frame) error {
		var out string
		in.GetPayload(&out)

		fmt.Println(out)
		return nil
	})

	f := NewFrame()
	f.SetPayload([]string{"1", "2", "3", "4"})
	s.Emit(f)

	time.Sleep(time.Second)

	cancel()
}
