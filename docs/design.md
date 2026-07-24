# fastsax design

This document describes the architecture of fastsax as shipped: the
sequential window/tape parser, the record-parallel pipeline, the speculative
skeleton with its soundness argument, the parallel stage 1, and the per-arch
SIMD kernels. Measured numbers live in [performance.md](performance.md);
approaches that were tried and rejected are in
[negative-results.md](negative-results.md).

fastsax is a ground-up rewrite of the ideas in
[gosax](https://github.com/orisano/gosax) around two structural changes:
batched events (a flat tape per ~1MB window instead of one event per call)
and record-parallel parsing (a cheap sequential boundary scan feeding full
parsing to worker goroutines). See
[THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md) for the exact
code-level lineage.

## 1. Sequential parser

### 1.1 Windows and the head-carry invariant

Input is consumed in fixed-size windows. Every window buffer reserves
`CarryCap` bytes (default 64KB) at its *head*; fresh input (up to
`ChunkSize`, default 1MB) is read directly behind it. When the scanner stops
inside incomplete markup at the end of window *i*, the unconsumed tail is
copied into the reserved head of window *i+1*, immediately before the fresh
bytes.

The invariant this buys: **no event ever crosses a window boundary** — every
event's bytes are one contiguous slice of one buffer. Everything downstream
(the tape, record grouping, the parallel pipeline) leans on it.

Two escalations handle inputs that exceed the reserved head:

- **Jumbo path**: a single markup construct (giant comment, CDATA section,
  DOCTYPE, or tag) larger than `CarryCap` is assembled in one reusable
  growing buffer and scanned there. Slower, but bounded and allocation-stable
  after the buffer reaches the construct's size.
- **Spill** (record mode): a record larger than a window is assembled in
  pooled spill buffers.

`source` (source.go) implements the synchronous variant with a single reused
buffer. `prefetcher` (prefetch.go) is the default: a background goroutine
reads ahead, rotating a fixed ring of 3 buffers through free/filled channels;
the consumer always owns exactly one buffer, which is what gives `Batch` its
"valid until the next `Next`" lifetime. `WithPrefetch(false)` selects the
fully synchronous, goroutine-free mode.

A UTF-8 BOM is skipped before scanning; a UTF-16 BOM is rejected with an
explicit error.

### 1.2 The event tape

`Parser.Next` returns a `*Batch`: the window's bytes plus a flat `[]Event`
tape. An `Event` is 12 bytes and pointer-free — kind, start/end offsets into
`Batch.Data`, and the start-tag name length. Consuming an event is an array
index, not a function call; the tape is GC-inert.

The scanner (`scanChunk`, scan.go) is a single loop over the window:

- **Text branch**: probes 16 bytes inline (two unaligned 8-byte SWAR words
  looking for `'<'`) before escalating to `bytes.IndexByte`. Tiny text runs
  (single-digit field values) are common, so staying inline beats any
  per-span call.
- **Markup branch** (`scanMarkup`): dispatches on the byte after `'<'` —
  `'/'` end tag, `'?'` PI, `'!'` then `'['` CDATA / `'-'` comment /
  `'D'`/`'d'` DOCTYPE (with bracket counting for internal subsets). This
  dispatch and its lenient spellings (the `CDATA[` characters are not
  verified) follow gosax; see
  [THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md).
- **Start tags**: the name is scanned with a stop-byte table and its length
  recorded in the tape (`Event.NameLen`), so `Batch.Name`/`Record.Name`
  slice the name without re-scanning the tag. Attribute bytes are skipped
  with quote-aware `IndexByte` hops (quoted values may contain `>` and `<`).

`KindText` events may split at a window boundary (a run touching the window
end can continue in the next batch); markup events never split. Malformed
markup surfaces as `*SyntaxError` with the absolute input offset.

## 2. Record mode: the parallel pipeline

```
io.Reader → [reader goroutine] → chunk ring → [stage 1: skeleton scan]
          → per-worker group queues → [N workers: subparse + handler]
          → chunk refcount → back to the ring
```

- **Stage 1 (skeleton)**: one logical walk over each window with full
  structural context (quotes, CDATA, comments, PIs, DOCTYPE) that builds no
  event tape — it only finds record open/close boundaries, carving runs of
  whole records into ~256KB *groups*. Chunk refcounts are set once, before
  any dispatch.
- **Stage 2 (workers)**: each worker subparses its groups' records with the
  full scanner (`scanRecord`, byte-identical in output to `scanChunk`) and
  calls the handler once per record. `parseGroup` checks `cut == len` on
  every record as a backstop.
- **Tag selection**: `RecordConfig.Tag` is matched byte-exactly with a
  name-boundary guard (`<recordx` never matches `record`) and a same-name
  depth counter (an inner `<record>` belongs to the outer record). A nil tag
  auto-detects the first repeating depth-1 element from the first window
  (`detectTag`, predict.go) and re-feeds the buffered bytes — no data loss.
- **Ordered mode**: groups are parsed in parallel into per-group tapes and
  delivery is sequenced through a bounded reorder ring; the handler always
  runs on worker 0 in document order. `Workers: 1` degrades to a fully
  inline, goroutine-free run.
- **Pooling**: all pipeline objects flow through fixed-population channel
  pools — gets provide backpressure, returns can never block, so no
  deadlocks — and the populations are recycled through package-level
  `sync.Pool`s across `ParseRecords` calls. Steady state allocates nothing
  per record (enforced by alloc gates in alloc_test.go).
- **Errors**: the first error recorded (handler, syntax, or reader) cancels
  the run; queues drain, all goroutines join, and that error is returned.
  Stage-1 structural errors are detected in document order at the exact
  offset a sequential run would report.

## 3. The speculative fast skeleton

The naive stage 1 (`skeletonFull`) walks every construct in the window. The
fast path (`skeletonFast`, skeleton.go) instead hops between candidate hits
of the record tag's open/close patterns (`<tag`, `</tag`) and derives record
boundaries directly, skipping the bytes in between.

Speculation is kept sound *entirely inside stage 1* — no wrong boundary ever
reaches a worker, and no re-dispatch protocol is needed:

- A candidate hit is trusted only after a proof that it is genuine markup
  and not bytes inside a quoted attribute value (`resolveGap`): quote-free
  gaps are proven by an exact `>`/`<` separation argument; gaps containing
  quotes are resolved by walking their constructs.
- `"<!"`/`"<?"` constructs — whose interiors (comments, CDATA, PIs, DOCTYPE)
  may contain arbitrary `<`/`>`/quote bytes — are handled per construct: the
  earliest opener candidate is proven genuine with the same gap proof, then
  the whole construct is consumed atomically via the reference `scanMarkup`,
  so fake boundaries inside interiors are never even seen. Constructs larger
  than the window ride the ordinary carry/jumbo escalation.
- Hazards and hostile data shapes (fake hits, quote-dense gaps) increment
  counters, and the fast path adaptively disables itself for the rest of the
  run (`fastOff`) when the proofs stop paying — degrading to `skeletonFull`,
  never below it.

`skelForceFull` is a test-only knob that forces every window through the
full walker so differential tests can compare both paths.

### 3.1 The SIMD hit mask

`findHit`'s candidate search is driven by a per-window `'<'` bitmask: one
whole-window kernel pass (`buildHitMask`, scankern.go) fills a `[]uint64`
where bit *j* of word *i* says `win[64i+j] == '<'`. Candidate iteration is
then bit iteration — `TrailingZeros64` hops — with a one-load SWAR probe and
an exact `patAt` check per candidate. See §5 for the kernels.

The mask is rebuilt on every `skeletonFast` entry: window buffers are pooled
and reused, so a mask cached by buffer identity would go stale. This rule is
load-bearing.

## 4. Parallel stage 1

With one sequential skeleton walk, worker scaling saturates around 2 workers
— stage 1 is the pipeline ceiling. Parallel stage 1 (stage1.go) removes it
by *speculatively pre-walking* upcoming chunks on dedicated goroutines while
the sequential reducer proves each speculation before consuming it.

### 4.1 Contract

Parallel stage 1 must be observably identical to sequential stage 1:

- byte-identical record boundaries `[start, end)` and `rec.Index` order;
- identical `SyntaxError`s (message and absolute offset) and identical
  first-error-in-document-order semantics;
- `MaxRecordSize` enforcement on records and on unresolved constructs
  (jumbo parity);
- Ordered mode, error propagation, goroutine joining, spill/jumbo behavior,
  zero steady-state allocations.

Grouping internals (which records share a group, which buffer backs a
record) may differ; nothing user-visible depends on them.

### 4.2 Scheme

- The reader wraps each filled chunk in a task carrying an **H-guess**: the
  position right after the chunk's first `'>'`, *believed* to be top-level
  text context (it is wrong exactly when that `'>'` sits inside a quoted
  attribute value or construct interior spanning the border).
- **Pre-walk workers** (up to 4, activated at `Workers >= 4`) walk the
  chunk's fresh bytes from the guess with the *existing* skeleton walker as
  a black box — same byte-consumption code, but with capture hooks
  (`walkHooks`) and a local depth counter starting at 0. Captured events are
  just "complete record `[s,e)`" and "matching close tag at depth 0"; a
  pre-walk never errors and never touches shared state.
- The **reducer** — the unchanged sequential scan loop on the `ParseRecords`
  goroutine — consumes chunks in document order. Per window it walks only
  the tiny head fragment (the carried tail plus fresh bytes up to the guess)
  sequentially from its authoritative position. If the fragment is consumed
  exactly (`cut == len`), the guess is proven correct and the captured
  events are replayed through the verbatim `addRecord`/`bankSpill`/`emit`
  machinery under the true entry depth. If not, the guess is disproved and
  the reducer simply walks the rest of the window sequentially from the
  fragment's cut — nothing speculative was consumed.
- Border-straddling constructs need no special protocol: the head fragment
  contains the carried construct and the walker consumes it natively, so
  marker splits, quote splits and giant constructs all reduce to code paths
  that already exist (carry/jumbo).

### 4.3 Soundness

Let R be the reference sequential walk of the whole input. Call a position x
a **clean point** if, in R, x lies in top-level text context: not inside any
construct, tag, or quoted attribute value.

**Lemma 1 (locality).** If x is a clean point and the walker runs over
`win = input[x:y)`, then (i) it consumes constructs byte-identically with R
on `[x, x+cut)`, capturing the same matching-tag events; (ii) `x+cut` is a
clean point; (iii) `cut == len(win)` **iff** y is a clean point.

*Proof sketch.* The walker's byte consumption is deterministic, stateless
across constructs, and independent of `depth` (depth affects only event
bookkeeping, never movement). From a clean point every construct is
self-delimiting: start tags end at the first unquoted `'>'`, end tags at the
first `'>'`, comments/CDATA/PIs at the first occurrence of their fixed
closer searched from the position `scanMarkup` mandates, DOCTYPE by bracket
count. "First occurrence" searches give the same answer on `win` as on the
full input whenever the closer lies inside `win`. If a construct straddles y
the walker returns `cut` at that construct's `'<'` — strictly less than
`len(win)`; conversely `cut == len(win)` only on trailing text or a
construct ending exactly at y, both clean. ∎

The reducer's head-fragment walk starts at the authoritative sequential
position — a clean point by induction from offset 0 — so by Lemma 1(iii),
consuming the fragment exactly proves the guess position clean, and by
Lemma 1(i) the pre-walk's captured events from that position equal R's
events. Replaying them through the sequential record-formation code under
the true depth therefore reproduces record boundaries, indices, spill
contents, `MaxRecordSize` decisions and error selection byte-identically.

Two properties are worth stating explicitly:

- **Validation is exact, not statistical.** `cut == len` is a theorem about
  the walker's behavior from a proven-clean start, not a heuristic
  consistency check. A pre-walk that starts inside a CDATA section can look
  internally consistent; it is never judged by its own consistency — its
  results are consumed only after the reducer's own walk proves its start
  position, rooted at offset 0.
- **The guess needs no trust.** A wrong H-guess costs time (one sequential
  window walk), never correctness. Errors captured by pre-walks are not
  acted on; `MaxRecordSize` and construct errors are re-discovered exactly
  during the reducer's replay, preserving first-error-in-document-order.

### 4.4 Adversarial cases

| input | behavior |
|---|---|
| stray `]]>` / `-->` / `?>` in text | The walker treats them as text in both modes; identical output. |
| quoted attribute value spanning a chunk border | The H-guess may land inside the quote; the head-fragment walk then cannot consume to the guess exactly (`cut < len`) → guess disproved → that window is walked sequentially. Boundaries stay exact; cost is one incident. |
| fake `<record>` inside CDATA/comments/quoted values | Handled inside a single walker by the fast skeleton's gap proofs and atomic construct consumption (§3); both modes run the same walker, so output is identical. |
| document that is one giant construct | The construct rides carry/jumbo; `MaxRecordSize` construct-parity errors fire at the same offset as sequential. |
| `<!x` (unknown bang) | The reducer's replay re-discovers the error at the sequential offset, after every earlier window is proven — first-error-in-document-order preserved. |
| records straddling 2+ chunk borders | The capture events (`open`-depth carryover, stray closes) replay through `bankSpill` exactly as the sequential per-window walk banks spill. |
| quotes or opaque interiors across *every* border | Every guess is disproved; after 4 consecutive incidents (`s1IncidentLimit`) pre-walk consumption is disabled for the run — sequential throughput, no further speculation cost. |

These cases (and border sweeps placing each pattern at every offset around
chunk borders) are locked in by `TestParallelStage1Adversarial`,
`TestParallelStage1SoundnessAttack` and the differential harness in
stage1_test.go.

### 4.5 Fallback and recovery

Sequential stage 1 remains fully intact and is the default:

1. **Static**: `Workers < 4` never activates parallel stage 1 (the extra
   goroutines cannot pay for their coordination below that).
2. **Incident recovery**: a disproved guess falls back to walking that
   window sequentially — the reducer state *is* the sequential stage-1
   state, so recovery is trivially sound and costs one window.
3. **Adaptive disable**: 4 consecutive disproved guesses permanently stop
   pre-walk dispatch for the run (mirroring the fast skeleton's `fastOff`).
4. **Test knobs**: `s1ForceSeq` / `s1ForceParallel` (mirroring
   `skelForceFull`) let differential tests force both paths over identical
   configurations; `s1Stats` counters (prewalks/hits/misses/disables) make
   the routing observable.

Worst case is sequential throughput plus bounded wasted pre-walk work —
never unsound.

## 5. SIMD kernels (scankern)

Exactly one assembly kernel exists: `maskByte64`, a byte-equality movemask —
it fills `masks[i]` with one bit per byte of each complete 64-byte block of
the input (`bit j of masks[i]` set iff `b[64i+j] == c`). The ragged tail is
finished in Go. It has exactly one production call site — `buildHitMask`,
once per `skeletonFast` window — where the cost amortizes over a
whole-window pass. Per-short-span kernel calls were measured and rejected
(-38%, see [negative-results.md](negative-results.md)): do not add call
sites inside the byte-level scan loops.

Per-arch implementations:

- **arm64** (scankern_arm64.s): NEON, CMEQ then an AND with a bit pattern
  and a 3×ADDP fold per 64-byte block (simdjson-style movemask).
- **amd64** (scankern_amd64.s): SSE2 baseline (4× PCMPEQB + PMOVMSKB per
  block, no feature detection needed) and an AVX2 variant (2× 32-byte
  VPCMPEQB/VPMOVMSKB). AVX2 is selected once at init by full runtime
  detection — CPUID feature bits plus OSXSAVE/XGETBV OS-state checks;
  anything less capable, including Rosetta 2 on Apple Silicon (which does
  not translate AVX2), falls back to SSE2.
- **everything else** (scankern_generic.go): a portable pure-Go SWAR kernel.

The asm kernels are ABI0, NOSPLIT, `//go:noescape`; call overhead is
irrelevant at one call per window. Both loop only while `remaining >= 64`,
reading exactly `floor(len/64)*64` bytes and writing exactly `floor(len/64)`
mask words; `len(masks) >= len(b)/64` is the caller's obligation, and the
sole production caller passes the exact-length subslice.

**A SWAR correctness subtlety** worth preserving: the cheap SWAR equality
form `(x-lo) &^ x & hi` is *wrong* for a full bitmask — its cross-byte
borrow flags a byte equal to `c^1` immediately after a true match. That is a
false positive only, so it remains fine for first-match probes (the text
scan), but `maskByte64Generic` must use the carry-free Hacker's Delight
zero-byte test `^(((x & 0x7f..) + 0x7f..) | x | 0x7f..)`. The randomized leg
of `TestScanKernParity` catches the difference immediately; the NEON/SSE2
kernels (per-byte compares) are unaffected.

Safety and parity coverage:

- `TestScanKernParity` and friends: asm vs generic masks over corpus windows
  plus adversarial lengths and alignments (every 16/32/64/128/192 block
  boundary, a structural byte slid through a window, randomized buffers);
  SSE2 vs AVX2 parity via the `scankernForceSSE2` knob on AVX2 hardware.
- Guard-page tests (scankern_guard_test.go and the Windows counterpart):
  input *and* output slices whose backing arrays end exactly at an unmapped
  page (`PROT_NONE` mmap on darwin/linux, `VirtualProtect` on Windows), so
  any read or write past `len` by the assembly faults instead of silently
  passing. Rosetta 2 enforces `PROT_NONE` faithfully, so the SSE2 kernel is
  exercised under it too.
- `scankernForceGeneric` routes `buildHitMask` through the pure-Go kernel;
  fuzz targets parse every input with the knob on and off and require
  identical event streams and errors.

## 6. The field-layout predictor (opt-in)

`RecordConfig.PredictLayout` enables a per-worker predictor
(`fieldPredictor`/`scanRecord`, predict.go) in stage 2. Real record streams
tend to repeat the same child-element sequence, so the predictor learns the
markup token literals of a record layout and confirms each subsequent token
with a single memcmp against the literal seen at the same position in the
previous record — byte-identical output by construction, with insertion
resync and a miss budget that pauses and then kills prediction on shuffled
layouts. It also caches the tape name length per learned token.

It is **off by default**: once the 16-byte SWAR text probe landed in the
generic scanner, the predictor measured net-negative on fixed layouts and a
tie on shuffled ones (see [negative-results.md](negative-results.md)). It
may still pay off for layouts dominated by long repeated tags.
