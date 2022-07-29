package streamz

import "context"

type broadcaster struct {
	input chan Frame
	reg   chan chan<- Frame
	unreg chan chan<- Frame

	outputs map[chan<- Frame]bool

	ctx context.Context
}

func (b *broadcaster) broadcast(f Frame) {
	// TODO: Evaluate weather the frame should be copied
	for ch := range b.outputs {
		select {
		case ch <- f:
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *broadcaster) run() {
	for {
		select {
		case m := <-b.input:
			b.broadcast(m)
		case ch, ok := <-b.reg:
			if ok {
				b.outputs[ch] = true
			} else {
				return
			}
		case ch := <-b.unreg:
			delete(b.outputs, ch)
		case <-b.ctx.Done():
			for ch := range b.outputs {
				close(ch)
			}
			close(b.reg)
			return
		}
	}
}

// NewBroadcaster creates a new broadcaster with the given input
// channel buffer length.
func newBroadcaster(ctx context.Context, buflen int) *broadcaster {
	b := &broadcaster{
		input:   make(chan Frame, buflen),
		reg:     make(chan chan<- Frame),
		unreg:   make(chan chan<- Frame),
		outputs: make(map[chan<- Frame]bool),
		ctx:     ctx,
	}

	go b.run()

	return b
}

func (b *broadcaster) Register(newch chan<- Frame) {
	b.reg <- newch
}

func (b *broadcaster) Unregister(newch chan<- Frame) {
	b.unreg <- newch
}
