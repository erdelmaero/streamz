package streamz

import (
	"context"
	"fmt"
)

type Source interface {
	Pipe
	ErrorPresenter(ErrFn)
	Emit(Frame) error
}

func NewSource(ctx context.Context) Source {
	return &pipe{
		ctx:     ctx,
		out:     newBroadcaster(ctx, 0),
		scatter: 1,
		errFn: func(f Frame, err error) {
			panic(err)
		},
	}
}

func (p *pipe) Emit(f Frame) error {
	select {
	case <-p.ctx.Done():
		return fmt.Errorf("send on closed pipeline")
	case p.out.input <- f:
		return nil
	}
}
