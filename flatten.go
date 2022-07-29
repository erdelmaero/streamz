package streamz

type Emitter interface {
	Emit(f Frame) error
}

func (p *pipe) Flatten(ffn FlattenFn) Pipe {
	next := &pipe{
		out:     newBroadcaster(p.ctx, 0),
		scatter: p.scatter,
		errFn:   p.errFn,
		ctx:     p.ctx,
	}
	outCh := make(chan Frame)
	p.out.Register(outCh)
	for i := 0; i < p.scatter; i++ {
		go func() {
			for frame := range outCh {
				ffn(frame, next)

				select {
				case <-p.ctx.Done():
					return
				default:
				}
			}
		}()
	}
	return next
}
