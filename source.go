package fastsax

import "io"

// source produces contiguous input windows: the caller's carried tail is
// copied into a reserved head region and fresh data is read directly behind
// it, so every window is one contiguous slice and no event ever crosses a
// window boundary.
//
// The synchronous implementation reuses a single buffer; the prefetch
// implementation (see prefetch.go) rotates buffers from a pool while a
// background goroutine reads ahead.
type source struct {
	cfg config
	r   io.Reader

	buf []byte // len = carryCap + chunkSize (sync mode)
	err error

	pf *prefetcher // non-nil in prefetch mode
}

func newSource(r io.Reader, cfg config) *source {
	s := &source{cfg: cfg, r: r}
	if cfg.prefetch {
		s.pf = newPrefetcher(r, cfg)
	} else {
		s.buf = make([]byte, cfg.carryCap+cfg.chunkSize)
	}
	return s
}

// next returns the next window: carry immediately followed by up to chunkSize
// fresh bytes. err is the reader error observed while filling this window
// (io.EOF included); data already read is always returned alongside it.
// The window is valid until the following next call.
func (s *source) next(carry []byte) ([]byte, error) {
	if s.pf != nil {
		return s.pf.next(carry)
	}
	cc := s.cfg.carryCap
	copy(s.buf[cc-len(carry):cc], carry)
	if s.err != nil {
		return s.buf[cc-len(carry) : cc], s.err
	}
	n, err := readChunk(s.r, s.buf[cc:cc+s.cfg.chunkSize])
	s.err = err
	return s.buf[cc-len(carry) : cc+n], err
}

func (s *source) reset(r io.Reader) {
	s.r = r
	s.err = nil
	if s.pf != nil {
		s.pf.reset(r)
	}
}

func (s *source) stop() {
	if s.pf != nil {
		s.pf.stop()
	}
}

// readChunk fills buf as far as it can, tolerating short reads and guarding
// against readers that keep returning (0, nil).
func readChunk(r io.Reader, buf []byte) (int, error) {
	n := 0
	zeros := 0
	for n < len(buf) {
		m, err := r.Read(buf[n:])
		n += m
		if err != nil {
			return n, err
		}
		if m == 0 {
			zeros++
			if zeros > maxZeroReads {
				return n, io.ErrNoProgress
			}
		} else {
			zeros = 0
		}
	}
	return n, nil
}
