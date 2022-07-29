package streamz

func (p *pipe) Filter(ffn FilterFn) Pipe {
	next := &pipe{
		ctx:     p.ctx,
		out:     newBroadcaster(p.ctx, 0),
		scatter: p.scatter,
	}
	outCh := make(chan Frame)
	p.out.Register(outCh)
	for i := 0; i < p.scatter; i++ {
		go func() {
			for frame := range outCh {
				if ffn(frame) {
					select {
					case next.out.input <- frame:
					case <-p.ctx.Done():
						return
					}
				}
			}
		}()
	}
	return next
}
