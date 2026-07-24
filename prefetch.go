package fastsax

import "io"

// pchunk is one prefetched buffer: a reserved carry head of carryCap bytes
// followed by up to chunkSize fresh bytes.
type pchunk struct {
	buf []byte
	n   int // fresh bytes at buf[carryCap : carryCap+n]
	err error
}

// prefetcher reads ahead on a background goroutine, rotating buffers through
// free/filled channels. The consumer always owns exactly one chunk (cur); it
// is recycled on the following next call, which is what gives Batch its
// "valid until next Next" lifetime.
type prefetcher struct {
	cfg    config
	free   chan *pchunk
	filled chan *pchunk
	done   chan struct{}
	dead   chan struct{} // closed by run on exit; reset joins on it
	cur    *pchunk
	err    error
}

const prefetchDepth = 3

func newPrefetcher(r io.Reader, cfg config) *prefetcher {
	p := &prefetcher{
		cfg:    cfg,
		free:   make(chan *pchunk, prefetchDepth),
		filled: make(chan *pchunk, prefetchDepth),
		done:   make(chan struct{}),
		dead:   make(chan struct{}),
	}
	for range prefetchDepth {
		p.free <- &pchunk{buf: make([]byte, cfg.carryCap+cfg.chunkSize)}
	}
	go p.run(r)
	return p
}

func (p *prefetcher) run(r io.Reader) {
	defer close(p.dead)
	for {
		var ck *pchunk
		select {
		case ck = <-p.free:
		case <-p.done:
			return
		}
		cc := p.cfg.carryCap
		ck.n, ck.err = readChunk(r, ck.buf[cc:cc+p.cfg.chunkSize])
		select {
		case p.filled <- ck:
		case <-p.done:
			p.free <- ck // return the in-flight chunk so reset can reclaim it
			return
		}
		if ck.err != nil {
			return
		}
	}
}

// reset joins the read goroutine (waiting for any in-flight Read to return),
// reclaims every buffer into the free ring and restarts reading from r. The
// buffer ring is reused, so resetting allocates only the two control channels.
func (p *prefetcher) reset(r io.Reader) {
	p.stop()
	<-p.dead
	if p.cur != nil {
		p.free <- p.cur
		p.cur = nil
	}
	for {
		select {
		case ck := <-p.filled:
			p.free <- ck
		default:
			p.err = nil
			p.done = make(chan struct{})
			p.dead = make(chan struct{})
			go p.run(r)
			return
		}
	}
}

func (p *prefetcher) next(carry []byte) ([]byte, error) {
	cc := p.cfg.carryCap
	if p.err != nil {
		// The reader is exhausted; serve carry-only windows out of the last
		// chunk's head region.
		if p.cur == nil {
			return nil, p.err
		}
		copy(p.cur.buf[cc-len(carry):cc], carry)
		return p.cur.buf[cc-len(carry) : cc], p.err
	}
	var ck *pchunk
	select {
	case ck = <-p.filled:
	case <-p.done:
		return nil, io.ErrClosedPipe
	}
	copy(ck.buf[cc-len(carry):cc], carry)
	if p.cur != nil {
		p.free <- p.cur
	}
	p.cur = ck
	p.err = ck.err
	return ck.buf[cc-len(carry) : cc+ck.n], ck.err
}

func (p *prefetcher) stop() {
	select {
	case <-p.done:
	default:
		close(p.done)
	}
}
