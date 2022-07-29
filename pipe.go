package streamz

import (
	"context"
	"sync"
	"time"
)

type (
	MapFn     func(in Frame) (Frame, error)
	WindowFn  func(in []Frame) (Frame, error)
	AccFn     func(in Frame, state *sync.Map) (Frame, error)
	FlattenFn func(in Frame, out Emitter) error

	FilterFn func(in Frame) bool
	SinkFn   func(in Frame) error
	ErrFn    func(Frame, error)
)

type Pipe interface {
	Map(MapFn) Pipe
	TimedWindow(time.Duration, WindowFn) Pipe
	Accumulate(AccFn) Pipe
	Flatten(FlattenFn) Pipe
	Scatter(int) Pipe
	Gather() Pipe
	Filter(FilterFn) Pipe
	Buffer(int) Pipe
	ReliefValve(time.Duration) Pipe
	RateLimit(time.Duration) Pipe
	Sink(SinkFn) Sink
	SinkCh() (Sink, <-chan Frame)
}

type pipe struct {
	out     *broadcaster
	scatter int
	errFn   ErrFn
	ctx     context.Context
}

func (p *pipe) Map(mfn MapFn) Pipe {
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
				res, err := mfn(frame)
				if err != nil {
					p.errFn(frame, err)
					continue
				}

				select {
				case next.out.input <- res:
				case <-p.ctx.Done():
					return
				}
			}
		}()
	}
	return next
}

func (p *pipe) Accumulate(afn AccFn) Pipe {
	next := &pipe{
		out:     newBroadcaster(p.ctx, 0),
		scatter: p.scatter,
		errFn:   p.errFn,
		ctx:     p.ctx,
	}
	outCh := make(chan Frame)
	p.out.Register(outCh)
	state := &sync.Map{}
	for i := 0; i < p.scatter; i++ {
		go func() {
			for frame := range outCh {
				res, err := afn(frame, state)
				if err != nil {
					p.errFn(frame, err)
					continue
				}

				select {
				case next.out.input <- res:
				case <-p.ctx.Done():
					return
				}
			}
		}()
	}
	return next
}

func (p *pipe) TimedWindow(interval time.Duration, wfn WindowFn) Pipe {
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
			window := []Frame{}
			ticker := time.NewTicker(interval)
			for {
				select {
				case frame := <-outCh:
					window = append(window, frame)
				case <-ticker.C:
					res, err := wfn(window)
					if err != nil {
						p.errFn(res, err)
					}

					window = []Frame{}

					select {
					case next.out.input <- res:
					case <-p.ctx.Done():
						return
					}
				case <-p.ctx.Done():
					return
				}
			}
		}()
	}
	return next
}

func (p *pipe) RateLimit(interval time.Duration) Pipe {
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
			ticker := time.NewTicker(interval)
			for frame := range outCh {
				select {
				case next.out.input <- frame:
				case <-p.ctx.Done():
					ticker.Stop()
					return
				}
				<-ticker.C
			}
		}()
	}
	return next
}

func (p *pipe) ReliefValve(reliefTime time.Duration) Pipe {
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
				select {
				case next.out.input <- frame:
				case <-p.ctx.Done():
					return
				case <-time.After(reliefTime):
					// This reliefs the pressure and omits the frame
					// if the next pipe has backpressure because the
					// channel cannot receive.
				}
			}
		}()
	}
	return next
}

func (s *pipe) ErrorPresenter(errFn ErrFn) {
	s.errFn = errFn
}
