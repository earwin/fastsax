package fastsax

import (
	"bytes"
	"encoding/binary"
	"math/bits"
	"sync/atomic"
)

// This file is the speculative fast path for stage-1 record boundary
// detection. Instead of walking every construct in the window (skeletonFull),
// it hops between SWAR-searched hits of the record tag's open/close patterns
// (findHit) and derives record boundaries directly, skipping the bytes in
// between.
//
// Speculation is kept sound entirely inside stage 1: a candidate hit is only
// trusted after a proof that it is genuine markup and not bytes inside a
// quoted attribute value (resolveGap). "<!"/"<?" constructs — whose interiors
// (comment, CDATA, PI, DOCTYPE) may contain arbitrary '<'/'>'/quote bytes —
// are handled per construct: the earliest opener candidate is proven genuine
// with the same gap proof, then the whole construct is consumed atomically
// via scanMarkup, so its interior bytes are never inspected for candidates at
// all. No wrong boundary ever reaches a worker and no re-dispatch protocol is
// needed; the workers' existing full subparse of every record (cut == len
// check in parseGroup) remains a backstop, not a load-bearing verifier.

// skelForceFull forces every window through the full structural walker.
// Test-only knob for diffing fast-path output against the reference path.
var skelForceFull atomic.Bool

// skelStats are cumulative process-wide fast-path metrics for tests and
// diagnostics (snapshot before/after a run, compare deltas). Stage 1 is a
// single goroutine per run, but runs may be concurrent, hence atomics.
var skelStats struct {
	fastWindows atomic.Int64 // windows scanned by the fast walker
	fullWindows atomic.Int64 // windows routed to the full walker (disabled/forced)
	constructs  atomic.Int64 // "<!"/"<?" constructs consumed inline by the fast walker
	gapWalks    atomic.Int64 // quoted gaps resolved by construct walking
	fakeHits    atomic.Int64 // candidate boundaries exposed as tag-interior bytes
	disables    atomic.Int64 // times a run turned the fast path off mid-stream
}

// walkHooks receives the walker's record-level decisions. The walkers
// (skeletonFull, skeletonFast) never touch grouping/spill state directly;
// they report through these two hooks, so the same byte-walking code serves
// the sequential reducer (hooks = recordRun: addRecord + ignore strays) and
// the parallel stage-1 pre-walkers (hooks = s1capture: event capture with a
// local depth counter, stage1.go).
type walkHooks interface {
	// record reports one complete record at win[start:end). start == -1
	// means the record began before this walk's window and its head bytes
	// are already banked (spill).
	record(win []byte, start, end int, owner *rchunk) error
	// stray reports a matching close tag ending at win[end] seen at depth 0.
	// The sequential reducer ignores it (exactly the old behavior); capture
	// mode records it because a positive true entry depth may consume it.
	stray(end int)
}

// walkState is the walker-facing stage-1 state: everything skeletonFull /
// skeletonFast read or write. recordRun embeds one (the sequential walk);
// each parallel stage-1 pre-walker owns another with capture hooks.
type walkState struct {
	tag      []byte
	off      int64 // absolute input offset of win[0], for error offsets
	depth    int   // matching-tag nesting depth; > 0 means inside a record
	recStart int   // window offset of the open record's start; -1: began earlier
	hooks    walkHooks
	fast     fastState
}

// fastState is the per-walk-state fast-skeleton state.
type fastState struct {
	off      bool   // fast path disabled for the rest of the run
	openPat  []byte // "<" + tag
	closePat []byte // "</" + tag

	// SWAR probes: the patterns' first min(8,len) bytes as a little-endian
	// word plus the matching byte mask (see findHit).
	open8, omask8  uint64
	close8, cmask8 uint64

	// mask is the per-window '<' bitmask consumed by findHit (one uint64 per
	// 64 window bytes), rebuilt by buildHitMask (scankern.go) at every
	// skeletonFast entry. Windows are pooled, reused buffers, so the mask
	// must never be cached by buffer identity — a stale mask over recycled
	// bytes would silently corrupt candidate search.
	mask []uint64
}

// fastOff disables the fast path for the remainder of the run: after the
// first confirmed speculation hazard (a fake hit) or a hostile data shape,
// staying on the exact full walker is both sound and cheaper than repeatedly
// probing and abandoning.
func (ws *walkState) fastOff() {
	if !ws.fast.off {
		ws.fast.off = true
		skelStats.disables.Add(1)
	}
}

// skeleton is the stage-1 entry point: route one window to the speculative
// fast walker or the full structural walker, starting at win[p0] (p0 > 0
// resumes an interrupted walk with window-relative offsets intact). Both
// walkers maintain the same walkState (depth, recStart, hooks) and return the
// same cut contract as skeletonFull, so windows of one stream can mix paths
// freely.
//
// Split constructs are sound too: an incomplete construct is always carried
// into the next window, so a window that begins inside a comment/CDATA/PI
// still starts with its "<!"/"<?" bytes and the fast walker consumes (or
// carries) it from there like any other construct.
func (ws *walkState) skeleton(win []byte, owner *rchunk, p0 int) (int, error) {
	if ws.fast.off || skelForceFull.Load() {
		skelStats.fullWindows.Add(1)
		return ws.skeletonFull(win, owner, p0)
	}
	skelStats.fastWindows.Add(1)
	return ws.skeletonFast(win, owner, p0)
}

// nextMark returns the position of the next "<m" occurrence at or after from,
// or -1. m is rare in text ('!' or '?'), so searching for m and checking the
// preceding byte is one IndexByte sweep instead of a probe per '<'. Callers
// cache the result per mark byte, so consumed construct interiors are never
// rescanned and each window is swept at most once per mark.
func nextMark(win []byte, from int, m byte) int {
	off := from + 1 // the mark byte of a construct at >= from is at >= from+1
	for off < len(win) {
		i := bytes.IndexByte(win[off:], m)
		if i < 0 {
			return -1
		}
		p := off + i
		if win[p-1] == '<' {
			return p - 1
		}
		off = p + 1
	}
	return -1
}

// resolveGap verdicts: is candidate position h genuine markup?
const (
	gapGenuine = iota // win[h] starts a construct in text context
	gapFake           // h lies inside another tag's bytes; pos is that tag's end
	gapCut            // the construct at pos is truncated by the window: cut there
)

// skeletonFast derives record boundaries from open/close pattern hits,
// mirroring skeletonFull's state transitions exactly for every genuine hit.
// "<!"/"<?" construct openers are tracked as candidates of their own: the
// earliest candidate in the window — pattern hit or opener — is resolved
// first, so every gap that gets the quote-free proof is guaranteed free of
// construct openers (any raw "<!"/"<?" before the candidate was itself
// resolved earlier) and quoted attribute values are the only place a fake can
// hide, exactly as the proof in resolveGap requires. Genuine openers are
// consumed atomically via scanMarkup, so construct interiors are never
// searched for candidates at all.
func (ws *walkState) skeletonFast(win []byte, owner *rchunk, p0 int) (int, error) {
	fs := &ws.fast
	if fs.closePat == nil {
		fs.openPat = append([]byte{'<'}, ws.tag...)
		fs.closePat = append([]byte{'<', '/'}, ws.tag...)
		fs.open8, fs.omask8 = swarPat(fs.openPat)
		fs.close8, fs.cmask8 = swarPat(fs.closePat)
	}
	fs.buildHitMask(win)
	var (
		vp     = p0 // verified position: win[vp:] is known to start in text context
		from   = p0 // search position for the next candidate
		nh     = -2 // cached pattern candidate (-2 stale, -1 exhausted)
		nhOpen bool
		nb     = -2 // cached "<!" opener candidate (-2 stale, -1 exhausted)
		nq     = -2 // cached "<?" opener candidate (-2 stale, -1 exhausted)
		hits   int  // genuine boundaries/constructs processed in this window
		walks  int  // quoted gaps that needed a construct walk
		cons   int  // "<!"/"<?" constructs consumed inline
	)
	for {
		if nh != -1 && nh < from {
			nh, nhOpen = fs.findHit(win, from)
		}
		if nb != -1 && nb < from {
			nb = nextMark(win, from, '!')
		}
		if nq != -1 && nq < from {
			nq = nextMark(win, from, '?')
		}
		nm := nb
		if nm < 0 || (nq >= 0 && nq < nm) {
			nm = nq
		}
		if nm >= 0 && (nh < 0 || nm < nh) {
			// Construct opener first: prove it stands in text context, then
			// consume the whole construct atomically. Fakes inside its
			// interior are never even seen.
			switch v, pos, err := ws.resolveGap(win, vp, nm, &walks); {
			case err != nil:
				return pos, err
			case v == gapCut:
				return ws.fastRet(pos, hits, walks, cons)
			case v == gapFake:
				skelStats.fakeHits.Add(1)
				ws.fastOff() // hazard confirmed: full walker from the next window on
				vp, from = pos, pos
				continue
			}
			n, _, err := scanMarkup(win[nm:])
			if err != nil {
				return nm, &SyntaxError{Offset: ws.off + int64(nm), Msg: err.Error()}
			}
			if n == 0 { // truncated by the window: cut at the opener, carry/jumbo
				return ws.fastRet(nm, hits, walks, cons)
			}
			hits++
			cons++
			vp, from = nm+n, nm+n
			continue
		}
		if nh < 0 {
			break // no candidates remain; resolve the tail below
		}
		h, open := nh, nhOpen
		// Prove the candidate is genuine before trusting it.
		switch v, pos, err := ws.resolveGap(win, vp, h, &walks); {
		case err != nil:
			return pos, err
		case v == gapCut:
			return ws.fastRet(pos, hits, walks, cons)
		case v == gapFake:
			skelStats.fakeHits.Add(1)
			ws.fastOff() // hazard confirmed: full walker from the next window on
			vp, from = pos, pos
			continue
		}
		if open {
			// Matching start tag: find its '>' with the attribute quoting
			// rules (also skips any fake pattern inside its own attributes).
			e := startTagEnd(win, h+len(fs.openPat))
			if e < 0 { // truncated by the window: carry from the '<'
				return ws.fastRet(h, hits, walks, cons)
			}
			hits++
			if win[e-2] == '/' { // self-closing record
				if ws.depth == 0 {
					if err := ws.hooks.record(win, h, e, owner); err != nil {
						return h, err
					}
				}
			} else {
				if ws.depth == 0 {
					ws.recStart = h
				}
				ws.depth++
			}
			vp, from = e, e
			continue
		}
		// Matching end tag: plain scan to '>' (no quoting rules in end tags,
		// exactly as skeletonFull), covering the "</tag  >" spelling.
		e := -1
		if g := h + len(fs.closePat); g < len(win) {
			if win[g] == '>' {
				e = g + 1
			} else if j := bytes.IndexByte(win[g:], '>'); j >= 0 {
				e = g + j + 1
			}
		}
		if e < 0 { // truncated by the window
			return ws.fastRet(h, hits, walks, cons)
		}
		hits++
		if ws.depth > 0 {
			ws.depth--
			if ws.depth == 0 {
				if err := ws.hooks.record(win, ws.recStart, e, owner); err != nil {
					return h, err
				}
				ws.recStart = -1
			}
		} else {
			ws.hooks.stray(e)
		}
		vp, from = e, e
	}

	// Tail: no candidates remain, find the carry cut. The tail contains no
	// construct openers (they were candidates too), so in a quote-free tail
	// every construct terminates at its first '>' and everything up to the
	// first '<' after the last '>' is complete while nothing after it can
	// close — that '<' is exactly skeletonFull's cut. Quoted tails walk
	// construct by construct instead.
	tail := win[vp:]
	if bytes.IndexByte(tail, '"') < 0 && bytes.IndexByte(tail, '\'') < 0 {
		g := bytes.LastIndexByte(tail, '>')
		x := bytes.IndexByte(tail[g+1:], '<')
		if x < 0 {
			return ws.fastRet(len(win), hits, walks, cons) // trailing text needs no event
		}
		return ws.fastRet(vp+g+1+x, hits, walks, cons)
	}
	gp := vp
	for {
		k := bytes.IndexByte(win[gp:], '<')
		if k < 0 {
			return ws.fastRet(len(win), hits, walks, cons)
		}
		t := gp + k
		e, err := constructEnd(win, t)
		if err != nil {
			return t, &SyntaxError{Offset: ws.off + int64(t), Msg: err.Error()}
		}
		if e < 0 { // incomplete: needs more data
			return ws.fastRet(t, hits, walks, cons)
		}
		gp = e
	}
}

// fastRet finishes a fast-scanned window: when most gaps needed construct
// walks the data shape (quotes throughout record content) makes hopping
// pointless, so hand the rest of the stream to the full walker. Consumed
// constructs count as hits: they are proven progress, and a construct-dense
// window (CDATA-heavy dumps) must not read as hostile.
func (ws *walkState) fastRet(cut, hits, walks, cons int) (int, error) {
	if cons > 0 {
		skelStats.constructs.Add(int64(cons))
	}
	if walks*2 > hits {
		ws.fastOff()
	}
	return cut, nil
}

// resolveGap proves whether candidate position h (win[h] == '<') is genuine
// markup, given that win[vp:] starts in text context and win[vp:h) contains
// no pattern hits.
//
// Exactness: h is the earliest unresolved candidate, so the gap contains no
// "<!"/"<?" bytes (an opener there would have been resolved first) and every
// construct in it is a start or end tag. In a quote-free gap a tag always
// terminates at the first '>', therefore h is genuine iff some '>' separates
// it from the gap's last '<'; otherwise that '<' opens a tag whose interior —
// necessarily quoted or window-truncated — contains h. Gaps that do contain
// quotes are resolved by walking their constructs with the full quoting
// rules; costlier, but only paid when record-adjacent content actually
// carries quotes.
func (ws *walkState) resolveGap(win []byte, vp, h int, walks *int) (int, int, error) {
	gap := win[vp:h]
	if bytes.IndexByte(gap, '"') < 0 && bytes.IndexByte(gap, '\'') < 0 {
		g := bytes.LastIndexByte(gap, '>')
		x := bytes.IndexByte(gap[g+1:], '<')
		if x < 0 {
			return gapGenuine, 0, nil
		}
		t := vp + g + 1 + x
		e, err := constructEnd(win, t) // may scan past h: quotes after the gap
		if err != nil {
			return 0, t, &SyntaxError{Offset: ws.off + int64(t), Msg: err.Error()}
		}
		if e < 0 {
			return gapCut, t, nil
		}
		return gapFake, e, nil // no '>' before h, so the tag at t swallows h
	}
	*walks++
	skelStats.gapWalks.Add(1)
	gp := vp
	for {
		k := bytes.IndexByte(win[gp:h], '<')
		if k < 0 {
			return gapGenuine, 0, nil
		}
		t := gp + k
		e, err := constructEnd(win, t)
		if err != nil {
			return 0, t, &SyntaxError{Offset: ws.off + int64(t), Msg: err.Error()}
		}
		if e < 0 {
			return gapCut, t, nil
		}
		if e > h {
			return gapFake, e, nil
		}
		gp = e
	}
}

// constructEnd returns the end (index past '>') of the construct at
// win[t] == '<', or -1 if the window truncates it. The bang/PI arms delegate
// to scanMarkup, so quoted-gap and quoted-tail walks stay exact across
// "<!"/"<?" constructs too.
func constructEnd(win []byte, t int) (int, error) {
	if t+1 >= len(win) {
		return -1, nil
	}
	switch win[t+1] {
	case '/':
		if j := bytes.IndexByte(win[t+2:], '>'); j >= 0 {
			return t + 3 + j, nil
		}
		return -1, nil
	case '!', '?':
		n, _, err := scanMarkup(win[t:])
		if err != nil {
			return -1, err
		}
		if n == 0 {
			return -1, nil
		}
		return t + n, nil
	default:
		return startTagEnd(win, t+1), nil
	}
}

// startTagEnd finds the index just past the '>' terminating the start tag
// whose '<' precedes win[i], skipping quoted attribute values byte-exactly
// like skeletonFull's start-tag branch. Returns -1 if the window truncates
// the tag.
func startTagEnd(win []byte, i int) int {
	for {
		for i < len(win) && win[i] != '>' && win[i] != '"' && win[i] != '\'' {
			i++
		}
		if i == len(win) {
			return -1
		}
		if win[i] == '>' {
			return i + 1
		}
		j := bytes.IndexByte(win[i+1:], win[i]) // skip quoted value
		if j < 0 {
			return -1
		}
		i += j + 2
	}
}

// SWAR constants for locating '<' anchors eight bytes at a time.
// bytes.Index is unusable here: with a '<' every dozen bytes its first-byte
// IndexByte hops never reach the vectorized cutover and stage 1 lands right
// back on the short-span IndexByte overhead this walker exists to avoid.
const (
	swarLo = 0x0101010101010101
	swarHi = 0x8080808080808080
	swarLT = swarLo * uint64('<')
)

// swarPat packs the first min(8,len) pattern bytes into a little-endian probe
// word and its byte mask, so a candidate anchor is tested with one unaligned
// load and compare.
func swarPat(pat []byte) (v, m uint64) {
	for i := range min(len(pat), 8) {
		v |= uint64(pat[i]) << (8 * i)
		m |= 0xFF << (8 * i)
	}
	return v, m
}

// findHit returns the next open or close pattern candidate at or after from,
// or -1. Both patterns share the '<' anchor, so iterating the set bits of the
// per-window '<' bitmask (fs.mask, rebuilt by every skeletonFast entry via
// buildHitMask — the kernel pass in scankern.go) serves both; each candidate
// is filtered by a one-load SWAR probe before the exact patAt check. Measured
// 2.3x the previous 8-byte SWAR sweep in candidate search, +14% e2e at w8
// (docs/performance.md): the mask is built in one amortized whole-window
// kernel pass instead of a serial 8-bytes-per-iteration loop.
// Candidates require the name-boundary guard byte ('>', '/' or whitespace for
// opens; '>' or whitespace for closes) so "<recordx" never matches "record".
// A full pattern truncated of its guard by the window end is returned as a
// candidate: its construct cannot complete either way, so the caller cuts and
// carries it. A partial pattern at the window end is not: the tail resolution
// finds the same cut through its construct scan.
func (fs *fastState) findHit(win []byte, from int) (int, bool) {
	i := max(from, 0)
	if i >= len(win) {
		return -1, false
	}
	w := i >> 6
	m := fs.mask[w] &^ (1<<(i&63) - 1)
	for {
		for m == 0 {
			w++
			if w >= len(fs.mask) {
				return -1, false
			}
			m = fs.mask[w]
		}
		p := w<<6 + bits.TrailingZeros64(m)
		m &= m - 1
		if p+8 <= len(win) { // one-load probe before the exact check
			x := binary.LittleEndian.Uint64(win[p:])
			if x&fs.cmask8 != fs.close8 && x&fs.omask8 != fs.open8 {
				continue
			}
		}
		if patAt(win, p, fs.closePat, false) {
			return p, false
		}
		if patAt(win, p, fs.openPat, true) {
			return p, true
		}
	}
}

// patAt reports whether a full pattern candidate starts at win[p] (exact
// bytes plus guard, or guard truncated by the window).
func patAt(win []byte, p int, pat []byte, open bool) bool {
	if p+len(pat) > len(win) || !bytes.Equal(win[p:p+len(pat)], pat) {
		return false
	}
	g := p + len(pat)
	if g >= len(win) {
		return true
	}
	c := win[g]
	return c == '>' || whitespace[c] || (open && c == '/')
}
