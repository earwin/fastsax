package fastsax

import (
	"bytes"
	"errors"
	"io"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	defaultMaxRecordSize = 64 << 20
	workerQueueDepth     = 4         // groups buffered per worker channel
	groupTargetMax       = 256 << 10 // target bytes of records per dispatched group
)

// RecordConfig configures ParseRecords.
type RecordConfig struct {
	// Tag is the exact name of the record element. Matching is byte-exact:
	// "<Tag" must be followed by '>', '/' or whitespace, so "<recordx" never
	// matches tag "record". Same-name nesting is handled with a depth
	// counter: an inner <Tag> belongs to the outer record.
	// A nil Tag auto-detects the first repeating depth-1 element from the
	// first input window (detectTag, predict.go); OnDetect reports the choice.
	Tag []byte

	// OnDetect, if set, is called once with the auto-detected record tag
	// before parsing starts (only when Tag is nil). The slice must not be
	// mutated but stays valid after the call.
	OnDetect func(tag []byte)

	// Stats, if set, receives adaptive-scheduler counters for this run
	// (worker starvation at dispatch, predictor hit rate). See RecordStats.
	Stats *RecordStats

	// PredictLayout enables the per-worker field-layout predictor in stage-2
	// record parsing (fieldPredictor, predict.go). Off by default: once the
	// 16B SWAR text probe landed in the generic scanner, the predictor
	// measured net-negative on fixed machine layouts (-10%) and a tie on
	// shuffled ones (M1 Pro — see BenchmarkPredictScanRecord).
	// It may still pay off for layouts dominated by long repeated tags.
	PredictLayout bool

	// Workers is the number of parallel record parsers. Values <= 0 mean
	// runtime.GOMAXPROCS(0). Workers == 1 parses inline with no goroutines.
	Workers int

	// Ordered delivers records in document order from a single goroutine;
	// handle is then always called with worker == 0. Parsing itself still
	// runs in parallel — only delivery is sequenced, through a bounded
	// reorder window.
	Ordered bool

	// ChunkSize and CarryCap mirror WithChunkSize and WithCarryCap
	// (defaults 1MB and 64KB).
	ChunkSize int
	CarryCap  int

	// MaxRecordSize bounds a single record element (default 64MB, capped at
	// 4GB-1). Records larger than one window are assembled in a spill
	// buffer; this cap keeps a missing end tag from buffering the input.
	MaxRecordSize int
}

// ParseRecords streams r, cuts it into complete Tag elements and calls handle
// for every record. Content outside records (whitespace, other elements,
// comments, PIs) is skipped by design.
//
// handle runs concurrently from Workers goroutines unless Ordered is set or
// Workers == 1. rec (Data and Events) is valid only for the duration of the
// call; copy anything that must be retained. rec.Events offsets are relative
// to rec.Data, and rec.Index is the document-order record number from 0.
//
// The first error — from handle, malformed XML, or the reader — cancels the
// run: dispatch stops, queues are drained, all goroutines are joined, and
// that error is returned.
func ParseRecords(r io.Reader, cfg RecordConfig, handle func(worker int, rec Record) error) error {
	if handle == nil {
		return errors.New("fastsax: ParseRecords requires a non-nil handler")
	}
	if len(cfg.Tag) == 0 {
		// Auto-detect seam (predict.go): buffer the first window, pick the
		// tag, and re-feed the buffered bytes through the normal pipeline.
		rest, err := detectTag(r, &cfg)
		if err != nil {
			return err
		}
		r = rest
	}
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.GOMAXPROCS(0)
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = defaultChunkSize
	}
	if cfg.CarryCap <= 0 {
		cfg.CarryCap = defaultCarryCap
	}
	if cfg.MaxRecordSize <= 0 {
		cfg.MaxRecordSize = defaultMaxRecordSize
	}
	if cfg.MaxRecordSize > math.MaxUint32 {
		cfg.MaxRecordSize = math.MaxUint32 // record spans are uint32 offsets
	}
	c, err := newConfig([]Option{
		WithChunkSize(cfg.ChunkSize), WithCarryCap(cfg.CarryCap), WithPrefetch(false),
	})
	if err != nil {
		return err
	}
	cfg.ChunkSize, cfg.CarryCap = c.chunkSize, c.carryCap

	// Pool sizing. Every group/spill buffer/chunk in existence lives in
	// exactly one place (a pool, stage 1, a queue, a worker, the reorder
	// window), gets from pools block (backpressure) and returns never do
	// (capacity covers every object), so the pipeline cannot deadlock and is
	// allocation-free once warm. The populations themselves are recycled
	// through package-level sync.Pools (see recycle), so repeated calls reuse
	// warmed buffers and tapes instead of re-allocating them per run.
	nchunks := max(cfg.Workers+2, 3)
	ngroups := max(cfg.Workers*(workerQueueDepth+1)+16, 32)

	// Parallel stage 1 (stage1.go) activates only when enough workers exist
	// for pre-walking to pay for its coordination; the read-ahead ring gets
	// one extra chunk per in-flight pre-walk task.
	useS1 := cfg.Workers >= s1MinWorkers || (cfg.Workers > 1 && s1ForceParallel.Load())
	if s1ForceSeq.Load() {
		useS1 = false
	}
	s1walkers, ntasks := 0, 0
	if useS1 {
		s1walkers = min(max(cfg.Workers/2, 2), s1MaxWalkers)
		ntasks = s1walkers + 3
		nchunks += ntasks
	}

	run := &recordRun{
		cfg:         cfg,
		c:           c,
		r:           r,
		walkState:   walkState{tag: cfg.Tag, recStart: -1},
		chunkPool:   make(chan *rchunk, nchunks),
		groupPool:   make(chan *recGroup, ngroups),
		spillPool:   make(chan []byte, ngroups+2),
		groupTarget: min(groupTargetMax, c.chunkSize/4),
		pending:     make([]*recGroup, 0, 8),
	}
	run.hooks = run // sequential emitters: record → addRecord, stray → ignore
	if run.groupTarget < 16 {
		run.groupTarget = 16
	}
	bufSize := c.carryCap + c.chunkSize
	for range nchunks {
		ck, _ := recChunkGlobal.Get().(*rchunk)
		if ck == nil || len(ck.buf) != bufSize {
			ck = &rchunk{buf: make([]byte, bufSize)} // geometry changed: drop for the GC
		}
		run.chunkPool <- ck
	}
	for range ngroups {
		g, _ := recGroupGlobal.Get().(*recGroup)
		if g == nil {
			g = &recGroup{}
		}
		run.groupPool <- g
	}
	for range cap(run.spillPool) {
		b, _ := recSpillGlobal.Get().([]byte)
		run.spillPool <- b // nil until first use, then reused across runs
	}

	if cfg.Workers == 1 {
		// Sequential fallback: parse and deliver inline, no goroutines.
		tape := make([]Event, 0, 512)
		pr := run.newPredictor()
		run.sink = func(g *recGroup) error {
			var perr error
			if !run.stopped.Load() {
				perr = run.parseGroup(g, &tape, pr, 0, handle)
			}
			run.release(g)
			return perr
		}
		if err := run.scan(); err != nil && err != errRecordsStopped {
			run.setErr(err)
		}
		pr.flush(cfg.Stats)
		run.recycle()
		return run.getErr()
	}

	// Read-ahead goroutine: overlaps input copies with the skeleton scan.
	// With parallel stage 1 the reader also routes each chunk through the
	// pre-walk pipeline (stage1.go); otherwise it feeds filledCh directly.
	run.readerStop = make(chan struct{})
	run.readerDone = make(chan struct{})
	if useS1 {
		run.s1 = newS1State(ntasks)
		for range s1walkers {
			run.s1.wg.Add(1)
			go run.s1walkLoop()
		}
		go run.s1reader()
	} else {
		run.filledCh = make(chan filledChunk, 2)
		go run.reader()
	}

	chans := make([]chan *recGroup, cfg.Workers)
	for i := range chans {
		chans[i] = make(chan *recGroup, workerQueueDepth)
	}
	var resultCh chan *recGroup
	var deliveryDone chan struct{}
	if cfg.Ordered {
		// resultCh holds every group in existence, so workers never block on
		// it and the reorder window is bounded by the group pool size.
		resultCh = make(chan *recGroup, ngroups)
		deliveryDone = make(chan struct{})
		go run.deliver(resultCh, ngroups, handle, deliveryDone)
	}
	var wg sync.WaitGroup
	for i := range chans {
		wg.Add(1)
		go func(id int, in chan *recGroup) {
			defer wg.Done()
			if cfg.Ordered {
				run.orderedWorker(in, resultCh)
			} else {
				run.worker(id, in, handle)
			}
		}(i, chans[i])
	}
	run.sink = func(g *recGroup) error {
		if run.stopped.Load() {
			run.release(g)
			return errRecordsStopped
		}
		ch := chans[int(g.seq)%cfg.Workers]
		if cfg.Stats != nil {
			cfg.Stats.noteDispatch(len(ch) == 0) // starved queue => input-bound
		}
		ch <- g
		return nil
	}

	if err := run.scan(); err != nil && err != errRecordsStopped {
		run.setErr(err)
	}
	close(run.readerStop)
	for _, ch := range chans {
		close(ch)
	}
	wg.Wait()
	if cfg.Ordered {
		close(resultCh)
		<-deliveryDone
	}
	// Join the reader (it exits at its next select, or when its in-flight Read
	// returns), join and drain the parallel stage-1 pipeline if one ran, then
	// recycle the pipeline objects for the next run.
	<-run.readerDone
	run.s1shutdown()
	run.recycle()
	return run.getErr()
}

// Package-level reuse pools: pipeline objects (chunk buffers, group objects
// with their result tapes, spill buffers) survive across ParseRecords calls,
// so a steady stream of calls allocates only channels and goroutines.
// sync.Pool keeps the caching GC-bounded; buffers whose geometry does not
// match the current run are simply dropped.
var (
	recChunkGlobal sync.Pool // *rchunk
	recGroupGlobal sync.Pool // *recGroup
	recSpillGlobal sync.Pool // []byte
)

// recycle returns every object still in the run's channel pools to the
// package-level pools. Called after all goroutines are joined, so nothing can
// touch these objects anymore. Objects stranded by an error path (a window
// being scanned when the run died) are deliberately left for the GC.
func (run *recordRun) recycle() {
	if run.filledCh != nil {
		// A chunk the reader filled that stage 1 never consumed.
		select {
		case f := <-run.filledCh:
			run.chunkPool <- f.ck
		default:
		}
	}
	for {
		select {
		case ck := <-run.chunkPool:
			ck.refs.Store(0)
			recChunkGlobal.Put(ck)
			continue
		default:
		}
		break
	}
	for {
		select {
		case g := <-run.groupPool:
			recGroupGlobal.Put(g)
			continue
		default:
		}
		break
	}
	for {
		select {
		case b := <-run.spillPool:
			if b != nil {
				recSpillGlobal.Put(b[:0])
			}
			continue
		default:
		}
		break
	}
	if run.s1 != nil {
		for {
			select {
			case t := <-run.s1.taskFree:
				s1taskGlobal.Put(t)
				continue
			default:
			}
			break
		}
	}
}

// errRecordsStopped is the internal signal that another goroutine already
// recorded the first error; it is never returned to the caller.
var errRecordsStopped = errors.New("fastsax: records run stopped")

// rchunk is one pooled input window buffer. refs is the number of dispatched
// groups still referencing buf; it is set once, before any dispatch, and the
// worker that drops it to zero returns the chunk to the pool.
type rchunk struct {
	buf  []byte
	refs atomic.Int32
}

// recSpan is one complete record within a group's data, [start:end).
type recSpan struct{ start, end uint32 }

// recGroup is the dispatch unit: a run of consecutive whole records carved
// from one buffer. Chunk-backed groups alias their window and hold one chunk
// reference; spill-backed groups own a copied buffer (a record that straddled
// windows, or any record found in a non-pooled jumbo window).
type recGroup struct {
	chunk  *rchunk   // owning chunk, nil for spill-backed groups
	data   []byte    // bytes the spans index into (window or spill buffer)
	spill  []byte    // spill-backed groups: buffer returned to the pool on release
	bounds []recSpan // one complete record per span, document order
	first  int64     // record index of bounds[0]
	seq    int64     // dispatch sequence, contiguous from 0; drives ordered delivery

	// Ordered mode only: parse results filled in by the worker. tape holds
	// the concatenated event tapes of all records in the group and evOff the
	// len(bounds)+1 prefix offsets into it.
	tape  []Event
	evOff []int32
}

// recordRun is the state of one ParseRecords call: pools, error latch and the
// sequential stage-1 scanner state (window walking, spill, jumbo, grouping).
type recordRun struct {
	cfg RecordConfig
	c   config
	r   io.Reader

	// The authoritative stage-1 walker state (tag, off, depth, recStart,
	// fast). Its hooks point back at this recordRun (record → addRecord,
	// stray → ignore), so run.skeleton(...) is exactly the historical
	// sequential walk. Parallel stage 1 (stage1.go) replays captured events
	// through the same embedded state.
	walkState

	chunkPool chan *rchunk
	groupPool chan *recGroup
	spillPool chan []byte

	// Parallel stage 1 (stage1.go); nil when the run uses the sequential
	// stage-1 walk only.
	s1 *s1state

	// Parallel mode read-ahead: a reader goroutine fills free chunks and
	// stage 1 receives them from filledCh, overlapping input copies with the
	// skeleton scan. nil in the sequential fallback (fully synchronous).
	// readerDone is closed by the reader on exit; ParseRecords joins on it
	// before recycling buffers.
	filledCh   chan filledChunk
	readerStop chan struct{}
	readerDone chan struct{}

	// sink consumes a finalized group: dispatch to a worker (parallel) or
	// parse inline (sequential). It always takes ownership of the group.
	sink func(*recGroup) error

	stopped  atomic.Bool
	errMu    sync.Mutex
	firstErr error

	// stage-1 window plumbing (mirrors Parser: carry + jumbo).
	carry   []byte
	jumbo   []byte
	jumboOn bool
	started bool

	// record formation state (walker depth/recStart live in walkState).
	spill    []byte // bytes of the record straddling windows, nil when none
	recIndex int64
	seq      int64

	// current in-window group being accumulated and this window's finalized
	// groups (dispatched together after the chunk refcount is set).
	cur         *recGroup
	curSize     int
	groupTarget int
	pending     []*recGroup
}

// record and stray are recordRun's walkHooks: the sequential emitters wired
// into the embedded walkState. owner is the window's pooled chunk (nil for
// jumbo windows), passed through from the skeleton call.
func (run *recordRun) record(win []byte, start, end int, owner *rchunk) error {
	return run.addRecord(win, start, end, owner)
}

// stray ignores a matching close tag at depth 0, exactly the historical
// sequential behavior.
func (run *recordRun) stray(int) {}

func (run *recordRun) setErr(err error) {
	run.errMu.Lock()
	if run.firstErr == nil {
		run.firstErr = err
	}
	run.errMu.Unlock()
	run.stopped.Store(true)
}

func (run *recordRun) getErr() error {
	run.errMu.Lock()
	defer run.errMu.Unlock()
	return run.firstErr
}

// release returns a consumed group's resources: the chunk reference (the last
// holder recycles the chunk), the spill buffer, and the group object itself.
// Safe to call from any goroutine.
func (run *recordRun) release(g *recGroup) {
	if g.chunk != nil {
		if g.chunk.refs.Add(-1) == 0 {
			run.chunkPool <- g.chunk
		}
		g.chunk = nil
	}
	if g.spill != nil {
		run.spillPool <- g.spill[:0]
		g.spill = nil
	}
	g.data = nil
	g.bounds = g.bounds[:0]
	run.groupPool <- g
}

// filledChunk is one chunk filled by the reader goroutine: n fresh bytes at
// buf[carryCap:carryCap+n] plus the reader error observed while filling.
type filledChunk struct {
	ck  *rchunk
	n   int
	err error
}

// reader is the parallel-mode read-ahead goroutine: it takes free chunks from
// the pool, fills their fresh region and forwards them to stage 1. The carry
// head region is untouched here — stage 1 copies the carry in after receipt —
// so reading overlaps with the skeleton scan. Exits on the final read error
// (io.EOF included) or when readerStop closes during shutdown.
func (run *recordRun) reader() {
	defer close(run.readerDone)
	cc, cs := run.c.carryCap, run.c.chunkSize
	for {
		var ck *rchunk
		select {
		case ck = <-run.chunkPool:
		case <-run.readerStop:
			return
		}
		n, err := readChunk(run.r, ck.buf[cc:cc+cs])
		select {
		case run.filledCh <- filledChunk{ck, n, err}:
		case <-run.readerStop:
			run.chunkPool <- ck // hand the in-flight chunk back for recycling
			return
		}
		if err != nil {
			return
		}
	}
}

// nextFilled returns the next filled chunk: from the reader goroutine in
// parallel mode, or by a synchronous read in the sequential fallback. The
// returned error is the reader error observed while filling (io.EOF
// included); data already read is always returned alongside it.
func (run *recordRun) nextFilled() (*rchunk, int, error) {
	if run.filledCh != nil {
		f := <-run.filledCh
		return f.ck, f.n, f.err
	}
	cc := run.c.carryCap
	ck := <-run.chunkPool
	n, err := readChunk(run.r, ck.buf[cc:cc+run.c.chunkSize])
	return ck, n, err
}

// scan is stage 1: it reads windows, runs the full structural scanner over
// each and walks the tape tracking record boundaries, grouping whole records
// for dispatch. It runs on the ParseRecords goroutine.
func (run *recordRun) scan() error {
	cc := run.c.carryCap
	for {
		if run.stopped.Load() {
			return errRecordsStopped
		}
		if run.jumboOn {
			done, err := run.scanJumbo()
			if err != nil {
				return err
			}
			if done {
				return run.finish()
			}
			continue
		}

		ck, n, rerr, task := run.nextFilledTask()
		carried := len(run.carry)
		copy(ck.buf[cc-carried:cc], run.carry)
		run.carry = run.carry[:0]
		win := ck.buf[cc-carried : cc+n]
		skip := 0
		if !run.started && len(win) > 0 {
			var err error
			skip, err = checkBOM(win)
			if err != nil {
				run.s1retire(task)
				return err
			}
			win = win[skip:]
			run.off += int64(skip)
			run.started = true
		}
		if len(win) == 0 {
			run.s1retire(task)
			run.chunkPool <- ck
			if rerr == nil {
				continue
			}
			if rerr == io.EOF {
				return run.finish()
			}
			return rerr
		}

		cut, serr := run.stage1Window(win, ck, task, carried, skip)
		if serr != nil {
			return serr
		}
		run.flushCur()
		if run.depth > 0 {
			if err := run.bankSpill(win, cut); err != nil {
				return err
			}
		}

		// Route the incomplete markup tail, exactly as the sequential Parser:
		// carry it, escalate to jumbo, or fail at EOF.
		tail := win[cut:]
		var tailErr error
		switch {
		case len(tail) == 0:
		case rerr == nil && len(tail) <= cc:
			run.carry = append(run.carry[:0], tail...)
		case rerr == nil:
			run.jumbo = append(run.jumbo[:0], tail...)
			run.jumboOn = true
		case rerr == io.EOF:
			tailErr = &SyntaxError{Offset: run.off + int64(cut), Msg: "unexpected EOF inside markup"}
		default:
			tailErr = rerr
		}

		// Set the chunk refcount before any dispatch, then hand the window's
		// groups to the sink in document order.
		if len(run.pending) == 0 {
			run.chunkPool <- ck
		} else {
			ck.refs.Store(int32(len(run.pending)))
		}
		var sinkErr error
		for _, g := range run.pending {
			if sinkErr != nil {
				run.release(g)
				continue
			}
			if err := run.emit(g); err != nil {
				sinkErr = err
			}
		}
		run.pending = run.pending[:0]
		run.off += int64(cut)

		if sinkErr != nil {
			return sinkErr
		}
		if tailErr != nil {
			return tailErr
		}
		if rerr != nil {
			if rerr == io.EOF {
				return run.finish()
			}
			return rerr
		}
	}
}

// scanJumbo assembles a markup construct larger than carryCap in a dedicated
// growing buffer and processes the resolved region as a non-pooled window
// (records found there are copied out through the spill path). Returns done
// when the input ended cleanly inside jumbo mode.
func (run *recordRun) scanJumbo() (done bool, err error) {
	cc := run.c.carryCap
	for {
		if run.stopped.Load() {
			return false, errRecordsStopped
		}
		ck, n, rerr, task := run.nextFilledTask()
		run.jumbo = append(run.jumbo, ck.buf[cc:cc+n]...)
		run.s1retire(task)  // pre-walk results are useless inside a jumbo construct
		run.chunkPool <- ck // bytes are copied; recycle immediately

		cut, serr := run.skeleton(run.jumbo, nil, 0)
		if serr != nil {
			return false, serr
		}
		if cut > 0 {
			if run.depth > 0 {
				if err := run.bankSpill(run.jumbo, cut); err != nil {
					return false, err
				}
			}
			run.off += int64(cut)
			tail := run.jumbo[cut:]
			switch {
			case rerr == nil && len(tail) <= cc:
				run.carry = append(run.carry[:0], tail...)
				run.jumbo = run.jumbo[:0]
				run.jumboOn = false
				return false, nil
			case rerr == nil:
				// Records were copied out during the walk, so the consumed
				// region is dead and the tail can be moved to the front.
				run.jumbo = run.jumbo[:copy(run.jumbo, tail)]
				continue
			case rerr == io.EOF && len(tail) == 0:
				run.jumbo = run.jumbo[:0]
				run.jumboOn = false
				return true, nil
			case rerr == io.EOF:
				return false, &SyntaxError{Offset: run.off, Msg: "unexpected EOF inside markup"}
			default:
				return false, rerr
			}
		}
		if len(run.jumbo) > run.cfg.MaxRecordSize {
			return false, &SyntaxError{Offset: run.off, Msg: "markup construct exceeds MaxRecordSize"}
		}
		if rerr == io.EOF {
			return false, &SyntaxError{Offset: run.off, Msg: "unexpected EOF inside markup"}
		}
		if rerr != nil {
			return false, rerr
		}
	}
}

// finish validates end-of-input state after a clean EOF.
func (run *recordRun) finish() error {
	if run.depth > 0 {
		return &SyntaxError{Offset: run.off, Msg: "unexpected EOF: unterminated record element"}
	}
	return nil
}

// skeletonFull is the stage-1 full structural walker: one full-context pass
// over the window (identical construct rules to scanChunk, via scanMarkup)
// that tracks only record open/close for the tag — no event tape is built,
// which is what keeps stage 1 off the critical path (per-field events are
// rebuilt by the workers from each record's bytes). The dispatching entry
// point is skeleton (skeleton.go), which routes eligible windows to the
// speculative fast walker instead.
//
// owner is the window's pooled chunk, or nil for jumbo windows (whose records
// are copied out because the buffer is not refcounted). Like scanChunk it
// returns cut — the offset just past the last complete construct — and
// win[cut:] is the incomplete markup prefix to carry.
func (ws *walkState) skeletonFull(win []byte, owner *rchunk, p0 int) (int, error) {
	tag := ws.tag
	tag0 := tag[0]
	p := p0
	for p < len(win) {
		if win[p] != '<' {
			// Text: tiny runs (single-digit field values) dominate record
			// data, so probe a few bytes inline before paying the IndexByte
			// call overhead.
			q := p
			lim := min(p+8, len(win))
			for q < lim && win[q] != '<' {
				q++
			}
			if q == lim && (q == len(win) || win[q] != '<') {
				r := bytes.IndexByte(win[q:], '<')
				if r < 0 {
					return len(win), nil // trailing text needs no event
				}
				q += r
			}
			if q == len(win) {
				return len(win), nil
			}
			p = q
		}
		if p+1 == len(win) {
			return p, nil // lone '<': needs more data
		}
		switch c := win[p+1]; c {
		case '/':
			// End tag: no quoting rules apply (as in scanMarkup), a plain
			// scan to '>' is exact.
			i := p + 2
			for i < len(win) && win[i] != '>' {
				i++
			}
			if i == len(win) {
				return p, nil
			}
			if win[p+2] == tag0 && matchCloseTag(win[p:i+1], tag) {
				if ws.depth > 0 {
					ws.depth--
					if ws.depth == 0 {
						if err := ws.hooks.record(win, ws.recStart, i+1, owner); err != nil {
							return p, err
						}
						ws.recStart = -1
					}
				} else {
					ws.hooks.stray(i + 1)
				}
			}
			p = i + 1
		case '!', '?':
			// Comments, CDATA, DOCTYPE, PIs: rare — defer to scanMarkup.
			n, _, err := scanMarkup(win[p:])
			if err != nil {
				return p, &SyntaxError{Offset: ws.off + int64(p), Msg: err.Error()}
			}
			if n == 0 { // incomplete: needs more data
				return p, nil
			}
			p += n
		default:
			// Start (or self-closing) tag: single pass to '>', skipping
			// quoted attribute values so '>' (or '<') inside them never
			// terminates the tag — byte-exact with scanMarkup's outcome.
			// (A SWAR word-stepping variant of this loop measured ~8% slower
			// on Apple M1: tag interiors are too short for it.)
			i := p + 1
			for {
				for i < len(win) && win[i] != '>' && win[i] != '"' && win[i] != '\'' {
					i++
				}
				if i == len(win) {
					return p, nil
				}
				if win[i] == '>' {
					break
				}
				j := bytes.IndexByte(win[i+1:], win[i]) // skip quoted value
				if j < 0 {
					return p, nil // unterminated quote: needs more data
				}
				i += j + 2
			}
			if c == tag0 && matchOpenTag(win[p:i+1], tag) {
				if win[i-1] == '/' { // self-closing
					if ws.depth == 0 {
						if err := ws.hooks.record(win, p, i+1, owner); err != nil {
							return p, err
						}
					}
				} else {
					if ws.depth == 0 {
						ws.recStart = p
					}
					ws.depth++
				}
			}
			p = i + 1
		}
	}
	return p, nil
}

// addRecord registers one complete record at win[start:end). start == -1
// means the record began in an earlier window and its head is already in the
// spill buffer.
func (run *recordRun) addRecord(win []byte, start, end int, owner *rchunk) error {
	idx := run.recIndex
	run.recIndex++
	if start < 0 { // straddler closing: complete the spill copy
		run.spill = append(run.spill, win[:end]...)
		if len(run.spill) > run.cfg.MaxRecordSize {
			return &SyntaxError{Offset: run.off + int64(end), Msg: "record exceeds MaxRecordSize"}
		}
		return run.emitSpill(idx)
	}
	if end-start > run.cfg.MaxRecordSize {
		return &SyntaxError{Offset: run.off + int64(start), Msg: "record exceeds MaxRecordSize"}
	}
	if owner == nil { // jumbo window: buffer is not refcounted, copy out
		if run.spill == nil {
			run.spill = <-run.spillPool
		}
		run.spill = append(run.spill, win[start:end]...)
		return run.emitSpill(idx)
	}
	g := run.cur
	if g == nil {
		g = <-run.groupPool
		g.chunk = owner
		g.data = win
		g.first = idx
		run.cur = g
	}
	g.bounds = append(g.bounds, recSpan{uint32(start), uint32(end)})
	run.curSize += end - start
	if run.curSize >= run.groupTarget {
		run.flushCur()
	}
	return nil
}

// flushCur finalizes the accumulating in-window group into pending.
func (run *recordRun) flushCur() {
	if run.cur != nil {
		run.pending = append(run.pending, run.cur)
		run.cur = nil
		run.curSize = 0
	}
}

// bankSpill copies the open record's bytes from the current window into the
// spill buffer at end-of-window.
func (run *recordRun) bankSpill(win []byte, cut int) error {
	start := max(run.recStart, 0)
	if run.spill == nil {
		run.spill = <-run.spillPool
	}
	run.spill = append(run.spill, win[start:cut]...)
	run.recStart = -1
	if len(run.spill) > run.cfg.MaxRecordSize {
		return &SyntaxError{Offset: run.off + int64(cut), Msg: "record exceeds MaxRecordSize"}
	}
	return nil
}

// emitSpill wraps the finished spill buffer as a single-record group and
// dispatches it immediately. Spill groups never touch chunk refcounts, so —
// unlike chunk-backed groups — they need not wait for end-of-window.
func (run *recordRun) emitSpill(idx int64) error {
	g := <-run.groupPool
	g.chunk = nil
	g.spill = run.spill
	g.data = run.spill
	g.first = idx
	g.bounds = append(g.bounds, recSpan{0, uint32(len(run.spill))})
	run.spill = nil
	return run.emit(g)
}

// emit assigns the dispatch sequence number (document order, contiguous from
// zero) and hands the group to the sink, which takes ownership.
func (run *recordRun) emit(g *recGroup) error {
	g.seq = run.seq
	run.seq++
	return run.sink(g)
}

// parseGroup subparses every record of g with the full scanner (through the
// worker's layout predictor, predict.go) and calls handle. Used by unordered
// workers and the sequential fallback.
func (run *recordRun) parseGroup(g *recGroup, tape *[]Event, pr *fieldPredictor, worker int, handle func(int, Record) error) error {
	for i := range g.bounds {
		b := g.bounds[i]
		data := g.data[b.start:b.end]
		t, cut, err := scanRecord(data, (*tape)[:0], pr)
		*tape = t
		if err == nil && cut != len(data) {
			err = errors.New("fastsax: internal: record subparse left unconsumed bytes")
		}
		if err != nil {
			return err
		}
		if err := handle(worker, Record{Data: data, Events: t, Index: g.first + int64(i)}); err != nil {
			return err
		}
	}
	return nil
}

// worker is the unordered stage-2 loop: parse each record of each group and
// call handle directly. After an error it keeps draining its queue (releasing
// resources without parsing) until the channel closes, so nothing leaks.
func (run *recordRun) worker(id int, in <-chan *recGroup, handle func(int, Record) error) {
	tape := make([]Event, 0, 512)
	pr := run.newPredictor()
	defer pr.flush(run.cfg.Stats)
	for g := range in {
		if !run.stopped.Load() {
			if err := run.parseGroup(g, &tape, pr, id, handle); err != nil {
				run.setErr(err)
			}
		}
		run.release(g)
	}
}

// newPredictor returns a fresh layout predictor for one stage-2 loop, or nil
// when RecordConfig.PredictLayout is off (scanRecord then delegates straight
// to scanChunk).
func (run *recordRun) newPredictor() *fieldPredictor {
	if !run.cfg.PredictLayout {
		return nil
	}
	return &fieldPredictor{}
}

// orderedWorker parses records into the group's own result tape and forwards
// the group to the delivery goroutine. Groups are always forwarded — even
// after an error — so the sequencer sees every sequence number.
func (run *recordRun) orderedWorker(in <-chan *recGroup, out chan<- *recGroup) {
	pr := run.newPredictor()
	defer pr.flush(run.cfg.Stats)
	for g := range in {
		if !run.stopped.Load() {
			g.tape = g.tape[:0]
			g.evOff = append(g.evOff[:0], 0)
			for _, b := range g.bounds {
				data := g.data[b.start:b.end]
				t, cut, err := scanRecord(data, g.tape, pr)
				g.tape = t
				if err == nil && cut != len(data) {
					err = errors.New("fastsax: internal: record subparse left unconsumed bytes")
				}
				if err != nil {
					run.setErr(err)
					break
				}
				g.evOff = append(g.evOff, int32(len(g.tape)))
			}
		}
		out <- g
	}
}

// deliver is the ordered-mode sequencer: it reorders parsed groups by seq in
// a fixed ring (in-flight dispatched sequence numbers are contiguous and
// bounded by the group pool size, so the ring cannot collide) and calls
// handle in exact document order with worker == 0.
func (run *recordRun) deliver(in <-chan *recGroup, ring int, handle func(int, Record) error, done chan<- struct{}) {
	defer close(done)
	hold := make([]*recGroup, ring)
	next := int64(0)
	for g := range in {
		hold[int(g.seq%int64(ring))] = g
		for {
			slot := int(next % int64(ring))
			h := hold[slot]
			if h == nil || h.seq != next {
				break
			}
			hold[slot] = nil
			next++
			if !run.stopped.Load() {
				for i := 0; i < len(h.bounds) && i+1 < len(h.evOff); i++ {
					b := h.bounds[i]
					rec := Record{
						Data:   h.data[b.start:b.end],
						Events: h.tape[h.evOff[i]:h.evOff[i+1]],
						Index:  h.first + int64(i),
					}
					if err := handle(0, rec); err != nil {
						run.setErr(err)
						break
					}
				}
			}
			run.release(h)
		}
	}
}

// matchOpenTag reports whether the start/self-close tag bytes b ("<name...>")
// open exactly tag: the name must be followed by '>', '/' or whitespace so
// prefix collisions ("<recordx" vs "record") never match.
func matchOpenTag(b, tag []byte) bool {
	if len(b) < len(tag)+2 || b[0] != '<' {
		return false
	}
	if !bytes.Equal(b[1:1+len(tag)], tag) {
		return false
	}
	c := b[1+len(tag)]
	return c == '>' || c == '/' || whitespace[c]
}

// matchCloseTag reports whether the end tag bytes b ("</name>") close exactly
// tag.
func matchCloseTag(b, tag []byte) bool {
	if len(b) < len(tag)+3 || b[0] != '<' || b[1] != '/' {
		return false
	}
	if !bytes.Equal(b[2:2+len(tag)], tag) {
		return false
	}
	c := b[2+len(tag)]
	return c == '>' || whitespace[c]
}
