package streamz

import (
	"context"
)

type SpinFn func() (Frame, error)

func NewPump(ctx context.Context, sfn SpinFn) Source {
	p := &pipe{
		ctx:     ctx,
		out:     newBroadcaster(ctx, 0),
		scatter: 1,
		errFn: func(f Frame, err error) {
			panic(err)
		},
	}

	go func() {

		for {
			f, err := sfn()
			if err != nil {
				p.errFn(f, err)
			}

			select {
			case p.out.input <- f:
			case <-p.ctx.Done():
				return
			}
		}
	}()

	return p
}
