package fastsax

import (
	"bytes"
	"sync"
	"sync/atomic"
)

// This file parallelizes stage 1 (record-boundary detection), the throughput
// ceiling of ParseRecords: with a single sequential skeleton walk, worker
// scaling saturates at 2 workers. See docs/design.md (Parallel stage 1).
//
// Scheme (chained validation, anchored at the sequential state): dedicated
// pre-walk workers speculatively walk each chunk's fresh
// bytes with the EXISTING skeleton walker (black box, capture hooks, local
// depth from 0) starting at an H-guess — the position right after the first
// '>' of the chunk, believed to be top-level text context. The reducer — the
// unchanged sequential scan loop on the ParseRecords goroutine — consumes
// chunks in document order and, per window, walks only the tiny head fragment
// [carry + fresh up to the guess] sequentially. The walker's byte consumption
// from a clean point is deterministic and depth-independent, so
//
//	fragment cut == fragment len  ⟺  the guess position is a clean point
//
// (docs/design.md, Lemma 1). On proof, the captured events are replayed through
// the verbatim addRecord/bankSpill/emit machinery with the true entry depth,
// making record boundaries, indices, spill, MaxRecordSize decisions and error
// offsets byte-identical to the sequential walk by construction. On disproof
// (a quoted attribute value or construct interior spanning the border), the
// window is simply walked sequentially from the fragment's cut — the reducer
// state IS the sequential stage-1 state, so recovery is trivially sound and
// costs one window. Border-straddling constructs ride the existing
// carry/jumbo machinery unchanged (the head fragment contains the carried
// construct and the walker consumes it natively), so no chunk-geometry change
// and no resolver pass are needed: every marker-split, quote-split and
// giant-construct case reduces to code paths that already exist.
//
// Nothing is ever accepted on internal consistency: a pre-walk's results are
// consumed only after its start position is proven clean by the reducer's own
// walk, whose start is authoritative by induction from offset 0.

const (
	// s1MinWorkers is the RecordConfig.Workers threshold that activates
	// parallel stage 1. Below it the extra goroutines cannot pay for their
	// coordination (stage 2 alone saturates the walker).
	s1MinWorkers = 4
	// s1MaxWalkers bounds the dedicated pre-walk goroutines.
	s1MaxWalkers = 4
	// s1IncidentLimit permanently disables pre-walk consumption for the run
	// after this many consecutive disproved guesses: the input shape (quotes
	// or opaque construct interiors across every border) defeats speculation,
	// so stop paying for it. Mirrors the fast skeleton's adaptive disable
	// (fastOff).
	s1IncidentLimit = 4
)

// Test-only knobs mirroring skelForceFull: s1ForceSeq disables parallel
// stage 1 regardless of Workers; s1ForceParallel enables it for any parallel
// run (Workers >= 2), so differential tests can force both paths over
// identical configurations.
var (
	s1ForceSeq      atomic.Bool
	s1ForceParallel atomic.Bool
)

// s1Stats are cumulative process-wide parallel-stage-1 counters for tests and
// diagnostics (snapshot before/after a run, compare deltas).
var s1Stats struct {
	prewalks atomic.Int64 // chunks speculatively pre-walked
	hits     atomic.Int64 // windows whose guess was proven and events replayed
	misses   atomic.Int64 // windows whose guess was disproved (sequential recovery)
	disables atomic.Int64 // runs that hit s1IncidentLimit and reverted to sequential
}

// s1taskGlobal recycles task objects (with their event buffers and done
// channels) across ParseRecords calls, like recChunkGlobal for chunks.
var s1taskGlobal sync.Pool

// s1ev is one captured record event, in document order, offsets relative to
// the pre-walked slice. s >= 0: a complete record [s, e) (local depth
// returned to 0, or a self-close at 0). s == -1: a matching close tag ending
// at e seen at local depth 0 — a positive true entry depth may consume it.
type s1ev struct{ s, e int }

// s1task is one chunk moving through the parallel stage-1 pipeline.
type s1task struct {
	ck   *rchunk
	n    int   // fresh bytes at ck.buf[carryCap : carryCap+n]
	rerr error // reader error observed while filling (io.EOF included)
	base int64 // absolute input offset of the first fresh byte

	dispatched bool          // sent to a pre-walk worker; done will be signaled
	done       chan struct{} // cap 1; worker → reducer completion signal

	// Pre-walk results, valid after done.
	walked   bool   // a guess existed and the slice was walked
	guess    int    // fresh-relative slice start: first byte after the first '>'
	cut      int    // slice-relative walker cut (incomplete tail starts here)
	depth    int    // local depth at cut
	recStart int    // slice-relative open-record start when depth > 0
	err      error  // captured walker error (absolute offset already correct)
	evs      []s1ev // captured events, document order
}

// s1state is the per-run parallel stage-1 pipeline: a fixed population of
// ntasks tasks circulating reader → pre-walk workers → reducer. orderedCh
// carries every task in document order; workCh fans walkable tasks out to the
// workers. Both channels hold the whole population, so sends never block.
type s1state struct {
	orderedCh chan *s1task
	workCh    chan *s1task
	taskFree  chan *s1task
	stop      atomic.Bool // adaptive disable: reader stops dispatching pre-walks
	streak    int         // reducer-side consecutive disproved guesses
	wg        sync.WaitGroup
}

func newS1State(ntasks int) *s1state {
	s := &s1state{
		orderedCh: make(chan *s1task, ntasks),
		workCh:    make(chan *s1task, ntasks),
		taskFree:  make(chan *s1task, ntasks),
	}
	for range ntasks {
		t, _ := s1taskGlobal.Get().(*s1task)
		if t == nil {
			t = &s1task{done: make(chan struct{}, 1)}
		}
		s.taskFree <- t
	}
	return s
}

// s1capture is the pre-walkers' walkHooks implementation: record events are
// appended to the task instead of forming groups. It never errors, so a
// pre-walk always runs to its natural cut (MaxRecordSize and construct errors
// are re-discovered exactly during the reducer's replay).
type s1capture struct{ t *s1task }

func (c *s1capture) record(_ []byte, start, end int, _ *rchunk) error {
	c.t.evs = append(c.t.evs, s1ev{start, end})
	return nil
}

func (c *s1capture) stray(end int) {
	c.t.evs = append(c.t.evs, s1ev{-1, end})
}

// s1reader is the parallel-stage-1 read-ahead goroutine: like reader, but each
// filled chunk is wrapped in a task that is sent to the reducer (in order)
// and, when pre-walking is still worthwhile, to the pre-walk workers.
func (run *recordRun) s1reader() {
	defer close(run.readerDone)
	s := run.s1
	cc, cs := run.c.carryCap, run.c.chunkSize
	var absOff int64
	for {
		var ck *rchunk
		select {
		case ck = <-run.chunkPool:
		case <-run.readerStop:
			return
		}
		var t *s1task
		select {
		case t = <-s.taskFree:
		case <-run.readerStop:
			run.chunkPool <- ck // hand the chunk back for recycling
			return
		}
		n, err := readChunk(run.r, ck.buf[cc:cc+cs])
		t.ck, t.n, t.rerr, t.base = ck, n, err, absOff
		absOff += int64(n)
		dispatched := n > 0 && !s.stop.Load()
		t.dispatched = dispatched
		// Both channels have capacity for every task in existence, so these
		// sends never block and the task cannot be stranded halfway. The
		// task must not be touched after the orderedCh send (an undispatched
		// task may be consumed and retired immediately), hence the local.
		s.orderedCh <- t
		if dispatched {
			s.workCh <- t
		}
		if err != nil {
			return
		}
	}
}

// s1walkLoop is one dedicated pre-walk worker: it owns a private walkState
// with capture hooks and speculatively walks each task's fresh bytes. The
// walker is the existing skeleton entry point (fast SWAR path included) used
// as a black box; only the hooks differ from the sequential walk.
func (run *recordRun) s1walkLoop() {
	defer run.s1.wg.Done()
	var capt s1capture
	ws := walkState{tag: run.tag, hooks: &capt}
	cc := run.c.carryCap
	for t := range run.s1.workCh {
		if !run.stopped.Load() {
			capt.t = t
			s1prewalk(&ws, t, cc)
		}
		t.done <- struct{}{}
	}
}

// s1prewalk walks one chunk's fresh bytes from the H-guess (the byte after
// the first '>': if that '>' terminates a construct in top-level context —
// the overwhelmingly common case — the position is clean and the walk is
// exact). The guess is never trusted here; the reducer proves or disproves it
// (stage1Window). Local depth starts at 0 and ws.off is set so any captured
// SyntaxError already carries its exact absolute offset.
func s1prewalk(ws *walkState, t *s1task, cc int) {
	fresh := t.ck.buf[cc : cc+t.n]
	i := bytes.IndexByte(fresh, '>')
	if i < 0 {
		t.walked = false
		return
	}
	g := i + 1
	ws.off = t.base + int64(g)
	ws.depth, ws.recStart = 0, -1
	t.evs = t.evs[:0]
	cut, err := ws.skeleton(fresh[g:], nil, 0)
	t.guess, t.cut, t.err = g, cut, err
	t.depth, t.recStart = ws.depth, ws.recStart
	t.walked = true
	s1Stats.prewalks.Add(1)
}

// nextFilledTask returns the next filled chunk plus its (already completed)
// stage-1 task, or a nil task when the run has no parallel stage 1. Joining
// the task here guarantees no pre-walker touches the chunk afterwards, so the
// caller may reuse or recycle its buffer freely.
func (run *recordRun) nextFilledTask() (*rchunk, int, error, *s1task) {
	if run.s1 == nil {
		ck, n, err := run.nextFilled()
		return ck, n, err, nil
	}
	t := <-run.s1.orderedCh
	if t.dispatched {
		<-t.done
	}
	return t.ck, t.n, t.rerr, t
}

// s1retire returns a consumed task to the free ring. Safe on a nil task
// (sequential mode). The event buffer keeps its capacity for reuse.
func (run *recordRun) s1retire(t *s1task) {
	if t == nil {
		return
	}
	t.ck = nil
	t.rerr, t.err = nil, nil
	t.evs = t.evs[:0]
	t.walked, t.dispatched = false, false
	run.s1.taskFree <- t
}

// s1miss records a disproved guess and applies the adaptive disable.
func (run *recordRun) s1miss() {
	s1Stats.misses.Add(1)
	run.s1.streak++
	if run.s1.streak == s1IncidentLimit {
		run.s1.stop.Store(true)
		s1Stats.disables.Add(1)
	}
}

// stage1Window walks one window, consuming the pre-walk speculation when it
// can be proven. Contract identical to run.skeleton(win, ck, 0): returns the
// window-relative cut past the last complete construct, with all record
// bookkeeping (depth, recStart, groups, spill via addRecord) applied to the
// run's sequential state. Always retires the task.
//
// carried is the number of carry bytes at the head of win; skip is the BOM
// bytes stripped from the front of the first window (guess offsets are
// fresh-relative and must be rebased onto win).
func (run *recordRun) stage1Window(win []byte, ck *rchunk, t *s1task, carried, skip int) (int, error) {
	if t == nil {
		return run.skeleton(win, ck, 0)
	}
	if !t.dispatched || !t.walked {
		run.s1retire(t)
		return run.skeleton(win, ck, 0)
	}
	f := carried + t.guess - skip
	if f <= 0 || f > len(win) {
		run.s1retire(t)
		return run.skeleton(win, ck, 0)
	}
	// Head fragment: the carried construct plus the bytes up to the guess.
	// This walk starts at the authoritative sequential position, so its
	// outcome is exact; consuming the fragment completely proves the guess
	// position clean (docs/design.md, Lemma 1(iii)).
	cutH, err := run.skeleton(win[:f], ck, 0)
	if err != nil {
		run.s1retire(t)
		return cutH, err
	}
	if cutH != f {
		// Guess disproved: the border sat inside a quoted value or construct
		// interior. Resume the exact sequential walk from the fragment's cut;
		// nothing speculative was consumed.
		run.s1miss()
		run.s1retire(t)
		return run.skeleton(win, ck, cutH)
	}
	run.s1.streak = 0
	s1Stats.hits.Add(1)
	cut, err := run.s1replay(win, f, t, ck)
	run.s1retire(t)
	return cut, err
}

// s1replay applies a proven pre-walk to the sequential state: the captured
// events are exactly the matching-tag events the sequential walker would have
// produced over win[f:f+t.cut], replayed here under the true entry depth
// through the verbatim record-formation code. Returns the window-relative cut.
func (run *recordRun) s1replay(win []byte, f int, t *s1task, ck *rchunk) (int, error) {
	for _, ev := range t.evs {
		if ev.s >= 0 {
			// Complete local record. At true depth 0 it is a real record; at
			// true depth > 0 it is an inner same-name element of a record
			// that opened before this window (the sequential walker would
			// bump depth up and back down — net nothing).
			if run.depth == 0 {
				if err := run.addRecord(win, f+ev.s, f+ev.e, ck); err != nil {
					return 0, err
				}
			}
			continue
		}
		// Matching close tag at local depth 0: consumed by the true depth
		// when positive (possibly completing a record that straddled the
		// border), ignored otherwise — exactly the sequential branches.
		if run.depth > 0 {
			run.depth--
			if run.depth == 0 {
				if err := run.addRecord(win, run.recStart, f+ev.e, ck); err != nil {
					return 0, err
				}
				run.recStart = -1
			}
		}
	}
	if t.err != nil {
		// The walk from the proven-clean guess is exact, so this is the
		// error the sequential walker would have hit, with the same absolute
		// offset, after forming the same earlier records.
		return 0, t.err
	}
	if t.depth > 0 {
		// Locally unclosed opens: the record either extends an already-open
		// one (depth adds) or starts a new one at the captured position.
		if run.depth == 0 {
			run.recStart = f + t.recStart
		}
		run.depth += t.depth
	}
	return f + t.cut, nil
}

// s1shutdown joins the pre-walk workers and drains the ordered pipeline after
// the reader has exited, returning every chunk and task to their rings. Runs
// on the ParseRecords goroutine after all other goroutines are joined; no-op
// without parallel stage 1.
func (run *recordRun) s1shutdown() {
	if run.s1 == nil {
		return
	}
	close(run.s1.workCh) // reader has exited: no more sends
	run.s1.wg.Wait()     // workers drained workCh and signaled every done
	for {
		select {
		case t := <-run.s1.orderedCh:
			if t.dispatched {
				<-t.done
			}
			run.chunkPool <- t.ck
			run.s1retire(t)
			continue
		default:
		}
		break
	}
}
