package streamz

func (p *pipe) Buffer(len int) Pipe {
	next := &pipe{
		ctx: p.ctx,
		out: newBroadcaster(p.ctx, len),
	}
	outCh := make(chan Frame)
	p.out.Register(outCh)
	go func() {
		for frame := range outCh {
			select {
			case next.out.input <- frame:
			case <-p.ctx.Done():
				return
			}
		}
	}()
	return next
}

func (p *pipe) Scatter(count int) Pipe {
	p.scatter = count
	return p
}

func (p *pipe) Gather() Pipe {
	p.scatter = 1
	return p
}
