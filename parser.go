package fastsax

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

const (
	defaultChunkSize = 1 << 20
	defaultCarryCap  = 64 << 10
	maxZeroReads     = 100
)

type config struct {
	chunkSize int
	carryCap  int
	prefetch  bool
}

// Option configures a Parser or record-mode run.
type Option func(*config)

// WithChunkSize sets the size of each input window (default 1MB).
func WithChunkSize(n int) Option { return func(c *config) { c.chunkSize = n } }

// WithCarryCap sets the reserved head region used to carry incomplete markup
// between windows (default 64KB). Any single markup construct larger than
// this falls back to the slower jumbo path, so set it above the p99 size of
// your tags/comments/CDATA sections.
func WithCarryCap(n int) Option { return func(c *config) { c.carryCap = n } }

// WithPrefetch toggles the background read-ahead goroutine (default on).
// Disable it for fully synchronous, single-goroutine operation.
func WithPrefetch(v bool) Option { return func(c *config) { c.prefetch = v } }

func newConfig(opts []Option) (config, error) {
	c := config{chunkSize: defaultChunkSize, carryCap: defaultCarryCap, prefetch: true}
	for _, o := range opts {
		o(&c)
	}
	if c.chunkSize < 64 {
		c.chunkSize = 64
	}
	if c.carryCap < 16 {
		c.carryCap = 16
	}
	if int64(c.chunkSize)+int64(c.carryCap) >= 1<<32 {
		return c, errors.New("fastsax: chunkSize+carryCap must be below 4GB")
	}
	return c, nil
}

// Parser is a sequential batch parser. It is not safe for concurrent use.
type Parser struct {
	cfg config

	src *source

	// carrybuf holds the incomplete markup tail of the previous window; it is
	// copied into the reserved head of the next window so that no event ever
	// crosses a window boundary.
	carrybuf []byte

	// jumbo assembles a single markup construct larger than carryCap.
	jumbo     []byte
	jumboMode bool

	tape  []Event
	batch Batch

	off      int64 // absolute input offset of the current window's first byte
	batchOff int64 // absolute input offset of the current batch's first byte
	started  bool
	err      error
}

// NewParser returns a Parser reading from r.
func NewParser(r io.Reader, opts ...Option) (*Parser, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	p := &Parser{
		cfg:      cfg,
		carrybuf: make([]byte, 0, cfg.carryCap),
		tape:     make([]Event, 0, 1024),
	}
	p.src = newSource(r, cfg)
	return p, nil
}

// Reset makes the parser read from r, reusing all internal buffers. In
// prefetch mode it waits for the background goroutine's in-flight Read on the
// previous reader to return before restarting.
func (p *Parser) Reset(r io.Reader) {
	p.src.reset(r)
	p.carrybuf = p.carrybuf[:0]
	p.jumbo = p.jumbo[:0]
	p.jumboMode = false
	p.off = 0
	p.batchOff = 0
	p.started = false
	p.err = nil
	p.batch = Batch{}
}

// Close releases the prefetch goroutine, if any. The parser must not be used
// afterwards except via Reset.
func (p *Parser) Close() { p.src.stop() }

// InputOffset returns the absolute input offset of the first byte of the
// current batch's Data.
func (p *Parser) InputOffset() int64 { return p.batchOff }

// Next returns the next batch of events. The returned batch (Data and Events)
// is valid only until the following call to Next, Reset or Close.
// It returns io.EOF after the final batch.
func (p *Parser) Next() (*Batch, error) {
	if p.err != nil {
		return nil, p.err
	}
	for {
		if p.jumboMode {
			b, err := p.jumboNext()
			if err != nil {
				p.err = err
				return nil, err
			}
			if b != nil {
				return b, nil
			}
			continue // jumbo resolved without events; back to normal flow
		}

		win, srcErr := p.src.next(p.carrybuf)
		p.carrybuf = p.carrybuf[:0]
		if !p.started && len(win) > 0 {
			var skip int
			skip, p.err = checkBOM(win)
			if p.err != nil {
				return nil, p.err
			}
			win = win[skip:]
			p.off += int64(skip)
			p.started = true
		}
		if len(win) == 0 {
			if srcErr == nil {
				continue
			}
			p.err = eofOr(srcErr)
			return nil, p.err
		}

		p.tape = p.tape[:0]
		tape, cut, scanErr := scanChunk(win, p.tape)
		p.tape = tape
		if scanErr != nil {
			p.err = &SyntaxError{Offset: p.off + int64(cut), Msg: scanErr.Error()}
			return nil, p.err
		}
		tail := win[cut:]
		p.batchOff = p.off
		p.off += int64(cut)

		// Decide what happens to the incomplete tail. Errors discovered here
		// are stashed in p.err so the current batch of valid events is still
		// delivered; the error surfaces on the following call.
		switch {
		case len(tail) == 0:
			// nothing to carry
		case srcErr == nil && len(tail) <= p.cfg.carryCap:
			p.carrybuf = append(p.carrybuf[:0], tail...)
		case srcErr == nil:
			p.jumbo = append(p.jumbo[:0], tail...)
			p.jumboMode = true
		case srcErr == io.EOF:
			p.err = &SyntaxError{Offset: p.off, Msg: "unexpected EOF inside markup"}
		default:
			p.err = srcErr
		}

		if len(p.tape) == 0 {
			if p.err != nil {
				return nil, p.err
			}
			if srcErr != nil {
				p.err = eofOr(srcErr)
				return nil, p.err
			}
			continue
		}
		p.batch = Batch{Data: win[:cut], Events: p.tape}
		return &p.batch, nil
	}
}

// jumboNext extends the jumbo buffer with fresh input until at least one
// complete event exists, then emits it. Returns (nil, nil) when jumbo mode
// ended with no events to deliver (cannot normally happen).
func (p *Parser) jumboNext() (*Batch, error) {
	for {
		data, srcErr := p.src.next(nil)
		p.jumbo = append(p.jumbo, data...)

		p.tape = p.tape[:0]
		tape, cut, scanErr := scanChunk(p.jumbo, p.tape)
		p.tape = tape
		if scanErr != nil {
			return nil, &SyntaxError{Offset: p.off + int64(cut), Msg: scanErr.Error()}
		}
		if cut > 0 {
			tail := p.jumbo[cut:]
			p.batchOff = p.off
			p.off += int64(cut)
			p.batch = Batch{Data: p.jumbo[:cut], Events: p.tape}
			if len(tail) <= p.cfg.carryCap {
				p.carrybuf = append(p.carrybuf[:0], tail...)
				p.jumboMode = false
			} else {
				// The remainder is itself oversized; keep assembling. Copy it
				// to the front of a fresh jumbo so the emitted batch's bytes
				// stay untouched until the next call.
				// (jumbo aliases the batch; we must not memmove over it.)
				next := make([]byte, len(tail), cap(p.jumbo))
				copy(next, tail)
				p.jumbo = next
			}
			return &p.batch, nil
		}
		if srcErr != nil {
			if srcErr == io.EOF {
				return nil, &SyntaxError{Offset: p.off, Msg: "unexpected EOF inside markup"}
			}
			return nil, srcErr
		}
	}
}

func eofOr(err error) error {
	if err == io.EOF {
		return io.EOF
	}
	return err
}

var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

func checkBOM(b []byte) (skip int, err error) {
	if len(b) >= 2 {
		if (b[0] == 0xFE && b[1] == 0xFF) || (b[0] == 0xFF && b[1] == 0xFE) {
			return 0, fmt.Errorf("fastsax: UTF-16 input is not supported (BOM %02X %02X)", b[0], b[1])
		}
	}
	if bytes.HasPrefix(b, utf8BOM) {
		return 3, nil
	}
	return 0, nil
}
