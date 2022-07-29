package streamz

import "sync"

type Sink interface {
	Close()
}

type sink struct {
	outCh chan Frame
	bc    *broadcaster
	work  *sync.WaitGroup
}

func (si *sink) Close() {
	si.bc.Unregister(si.outCh)
	close(si.outCh)
	si.work.Wait()
}

func (p *pipe) Sink(sfn SinkFn) Sink {
	si := &sink{
		outCh: make(chan Frame),
		bc:    p.out,
		work:  &sync.WaitGroup{},
	}
	p.out.Register(si.outCh)

	si.work.Add(p.scatter)
	for i := 0; i < p.scatter; i++ {
		go func() {
			for frame := range si.outCh {
				err := sfn(frame)
				if err != nil {
					p.errFn(frame, err)
				}
			}
			si.work.Done()
		}()
	}
	return si
}

func (p *pipe) SinkCh() (Sink, <-chan Frame) {
	si := &sink{
		outCh: make(chan Frame),
		bc:    p.out,
		work:  &sync.WaitGroup{},
	}
	p.out.Register(si.outCh)
	return si, si.outCh
}
