# fastsax

A streaming, zero-allocation SAX-style XML scanner for Go with a parallel
record mode. fastsax is a ground-up rewrite of the ideas in
[gosax](https://github.com/orisano/gosax) around two structural changes:

1. **Batch events instead of an event per call.** The sequential API returns
   ~1MB windows with a flat tape of `Event`s (12 bytes each, no pointers), so
   the per-event overhead is an array index, not a function call.
2. **Record-parallel parsing.** For the dominant real-world shape — a large
   document that is a long run of similar elements (`<record>…</record>`) — a
   cheap sequential stage finds record boundaries and full parsing fans out to
   worker goroutines.

Input is any `io.Reader`; memory use is bounded regardless of input size.
Full SAX-correctness is maintained: CDATA, comments, PIs, DOCTYPE (with
internal subsets), quoted attribute values containing `>` or `<`, and
self-closing tags are handled everywhere, verified by differential fuzzing
against `encoding/xml`.

## Installation

```sh
go get github.com/Muravlev/fastsax
```

```go
import "github.com/Muravlev/fastsax"
```

Requires Go 1.25+. No dependencies (the only entry in go.mod is a test-only
benchmark-parity dependency on gosax).

## Quick start

Sequential — every event of the document, in order, on one goroutine:

```go
p, err := fastsax.NewParser(file)
if err != nil { /* handle */ }
defer p.Close()
for {
    b, err := p.Next()
    if err == io.EOF {
        break
    }
    if err != nil { /* handle */ }
    for _, e := range b.Events {
        if e.Kind == fastsax.KindStart {
            fmt.Printf("<%s>\n", b.Name(e))
        }
    }
}
```

Parallel — one handler call per complete `<record>` element, from worker
goroutines:

```go
err := fastsax.ParseRecords(file, fastsax.RecordConfig{
    Tag: []byte("record"), // nil => auto-detect
}, func(worker int, rec fastsax.Record) error {
    for _, e := range rec.Events {
        if e.Kind == fastsax.KindText {
            _ = rec.Bytes(e) // valid only during this call; copy to keep
        }
    }
    return nil
})
if err != nil { /* first handler/syntax/reader error */ }
```

**Which API do I want?** Use the sequential `Parser` when you need every
event of the document in order (mixed content, cross-element state, or no
repeating structure) or when everything must stay on one goroutine. Use
`ParseRecords` when the document is a long run of similar elements and each
record can be handled independently — the handler runs concurrently and must
be safe for that. If you want the parallel parsing speed but document-order,
single-goroutine delivery, set `Ordered: true` (the handler is then always
called with `worker == 0`); `Workers: 1` degrades gracefully to a fully
inline, goroutine-free run.

## Sequential API

```go
p, err := fastsax.NewParser(r) // options: WithChunkSize, WithCarryCap, WithPrefetch
if err != nil { /* handle */ }
defer p.Close()

for {
    b, err := p.Next() // *Batch: Data []byte + Events []Event
    if err == io.EOF {
        break
    }
    if err != nil { /* handle */ }
    for _, e := range b.Events {
        switch e.Kind {
        case fastsax.KindStart:
            name, attrs := fastsax.Name(b.Bytes(e))
            _ = attrs // fastsax.NextAttribute(attrs) to iterate
            _ = name
        case fastsax.KindText:
            text, _ := fastsax.Unescape(b.Data[e.Start:e.End]) // in-place
            _ = text
        }
    }
}
```

Kinds: `KindStart`, `KindEnd`, `KindSelfClose`, `KindText`, `KindCData`,
`KindComment`, `KindPI`, `KindDocType`. Helpers (`Name`, `NextAttribute`,
`CData`, `Comment`, `Unescape`, `UnescapeAppend`) are allocation-free;
`NextAttribute` returns values without their quotes and does not decode
entities; `Unescape` decodes the predefined and numeric entities and
normalizes `\r`/`\r\n` to `\n` in place. `Parser.InputOffset` reports the
absolute input offset of the current batch.

Prefer `b.Name(e)` (and `rec.Name(e)` in record mode) over `Name(b.Bytes(e))`
for element names: the scanner records the name length of `KindStart` and
`KindSelfClose` events in the tape (`Event.NameLen`), so the accessor slices
the name without re-scanning the tag bytes — measured ~10% end-to-end on the
name-heavy benchmark workload. For `KindEnd` (and degenerate tags) it falls
back to `Name` transparently; recording end-tag name lengths was measured and
rejected (~2% scanner cost for names consumers rarely need).

## Parallel record API

```go
err := fastsax.ParseRecords(r, fastsax.RecordConfig{
    Tag:     []byte("record"), // nil => auto-detect the repeating depth-1 element
    Workers: 0,                // 0 => GOMAXPROCS; 1 => fully inline, no goroutines
    Ordered: false,            // true => document order, handler always worker 0
}, func(worker int, rec fastsax.Record) error {
    // rec.Data is the record's bytes, rec.Events its full event tape
    // (offsets relative to rec.Data), rec.Index its document-order number.
    return nil // a non-nil error cancels the whole run and is returned
})
if err != nil { /* handle */ }
```

`RecordConfig` also exposes `ChunkSize`, `CarryCap`, `MaxRecordSize` (default
64MB), `OnDetect` (reports the auto-detected tag), `Stats` (`RecordStats`:
dispatch starvation and predictor hit counters; `Stats.IOBound()` tells you
the run was input-bound and fewer workers would do) and `PredictLayout`
(opt-in stage-2 field-layout predictor, off by default — see Design and the
Performance notes).

Content outside record elements (prologue, whitespace, other elements,
comments) is skipped by design. Record matching is byte-exact on the tag name
with a same-name nesting depth counter, so `<record>` inside `<record>`
belongs to the outer record and `<recordx>` never matches `record`.

## Examples

Runnable examples live in the package's `example_test.go` (shown on the
package's godoc page and run as part of `go test`) and in the `examples/`
directory. Start there for end-to-end patterns: attribute iteration,
unescaping, ordered parallel parsing, tag auto-detection.

## Design

```
io.Reader → [reader goroutine] → chunk ring → [stage 1: skeleton scan]
          → per-worker group queues → [N workers: subparse + handler]
          → chunk refcount → back to the ring
```

- **Head-carry invariant.** Every window buffer reserves `CarryCap` bytes at
  its head; fresh input is read behind it. The incomplete markup tail of
  window *i* is copied into the reserved head of window *i+1*, so **no event
  or record ever crosses a window boundary** and every span is one contiguous
  slice. Constructs larger than `CarryCap` fall back to a *jumbo* path (one
  reusable growing buffer); records larger than a window are assembled in
  pooled *spill* buffers.
- **Stage 1 (skeleton).** One goroutine walks each window with full
  structural context (quotes, CDATA, comments, PIs, DOCTYPE) but builds no
  event tape — it only tracks record open/close, carving runs of whole
  records into ~256KB *groups*. Chunk refcounts are set once, before any
  dispatch.
- **Speculative fast skeleton.** Windows are scanned by hopping between
  candidate hits of `<tag`/`</tag`; every candidate is *proven* genuine
  before use (quote-free gaps by an exact `>`/`<` separation argument, quoted
  gaps by walking their constructs), so speculation is sound entirely inside
  stage 1 and no wrong boundary can reach a worker. `"<!"`/`"<?"` constructs
  (CDATA, comments, PIs, DOCTYPE) are candidates of their own: the earliest
  one is proven genuine the same way, then consumed atomically via the
  reference construct scanner — fake boundaries inside their interiors are
  never even seen, and constructs larger than the window ride the ordinary
  carry/jumbo escalation. Hazards and hostile data shapes adaptively disable
  the fast path for the rest of the run.
- **SIMD hit mask** (see [docs/design.md](docs/design.md)). The fast
  skeleton finds its
  candidates through a per-window `'<'` bitmask built by one per-arch
  byte-equality kernel, `maskByte64` (one movemask word per 64 input bytes):
  NEON assembly on arm64, SSE2/AVX2 assembly on amd64, a portable pure-Go
  SWAR kernel elsewhere. On amd64 the AVX2 kernel is selected once at init by
  full runtime detection (CPUID feature bits plus OSXSAVE/XGETBV
  OS-state checks); anything less capable — including Rosetta 2 on Apple
  Silicon, which does not translate AVX2 — falls back to the SSE2 baseline.
  The kernels never read past the slice, pinned by mmap guard-page tests on
  darwin/linux and a `VirtualProtect` counterpart on Windows (the platform
  where real AVX2 hardware actually runs them). The mask is rebuilt on every
  window walk because window buffers are pooled and reused.
- **Parallel stage 1** (`Workers >= 4`, see [docs/design.md](docs/design.md)). The
  sequential skeleton walk is the pipeline's ceiling, so dedicated pre-walk
  goroutines speculatively walk each chunk's fresh bytes with the same walker
  (capture hooks, local depth from 0) starting right after the chunk's first
  `'>'`. The reducer — the unchanged sequential loop — walks only the tiny
  head fragment of each window up to that guess; consuming the fragment
  exactly *proves* the guess was top-level text context (walker byte
  consumption from a clean point is deterministic and depth-independent), and
  only then are the captured events replayed through the verbatim
  record-formation code under the true nesting depth. A disproved guess (a
  quoted value or construct interior spanning the border) falls back to
  walking that window sequentially — never wrong, at worst slow — and
  repeated incidents disable speculation for the run. Boundaries, indices,
  spill, `MaxRecordSize` decisions and error offsets are byte-identical to
  the sequential walk by construction.
- **Stage 2 (workers).** Each worker subparses its records with the full
  scanner (`scanRecord`, byte-identical in output to the sequential
  `scanChunk`). An optional per-worker **field-layout predictor**
  (`RecordConfig.PredictLayout`, **off by default**) recognizes repeated
  record layouts and confirms each markup token with a single memcmp against
  the literal seen at the same position in the previous record
  (byte-identical by construction, with insertion resync and a miss budget
  that pauses/kills prediction on shuffled layouts). Once the 16B SWAR text
  probe landed in the generic scanner the predictor measured net-negative on
  typical layouts, which is why it is opt-in — see the Performance notes.
  `Ordered` mode parses in parallel into per-group tapes and sequences
  delivery through a bounded reorder ring.
- **Pooling.** All pipeline objects flow through fixed-population channel
  pools (gets provide backpressure, returns can never block, so no
  deadlocks), and the populations themselves are recycled through
  package-level `sync.Pool`s across `ParseRecords` calls.

Three documents under [docs/](docs/) go deeper:
[design.md](docs/design.md) is the full architecture — the batch tape, the
head-carry invariant, the speculative skeleton and its soundness argument
(Lemma 1), parallel stage 1, and the SIMD kernels;
[performance.md](docs/performance.md) holds the measured tables and the
benchmarking methodology; [negative-results.md](docs/negative-results.md)
catalogs the optimizations that were measured and rejected, with their
numbers, so they are not retried blind.

## Lifetime and concurrency contracts

- A `Batch` (both `Data` and `Events`) is valid **only until the next call**
  to `Next`, `Reset` or `Close`. `Parser` is not safe for concurrent use.
- A `Record` is valid **only for the duration of the handler call**; copy
  what you keep. The handler runs concurrently from `Workers` goroutines
  unless `Ordered` is set or `Workers == 1`.
- `KindText` events may be **split at window boundaries** (a text run that
  touches the end of a batch may continue at the start of the next one);
  coalesce adjacent text events when exact runs matter. Markup events are
  never split.
- `Unescape` decodes **in place**, mutating the bytes you pass it (safe on
  batch/record slices, which are disjoint views); use `UnescapeAppend` when
  the decoded text must outlive the batch or record. `NextAttribute` values
  are raw: quotes stripped, entities not decoded.
- Malformed markup surfaces as `*SyntaxError` carrying the **absolute input
  offset** of the offending construct. The sequential parser returns `io.EOF`
  after the final batch; truncated input inside markup is a `SyntaxError`
  ("unexpected EOF inside markup"), reported after any completed batch of
  valid events has been delivered.
- In `ParseRecords`, stage-1 structural errors are detected **in document
  order**, at the same offset a sequential run would report — parallel
  stage 1 keeps error offsets byte-identical by construction. The first error
  recorded (handler, syntax, or reader — concurrent failures race for this
  slot) cancels the run: dispatch stops, queues are drained, all goroutines
  are joined, and that error is returned. Cancellation has group granularity,
  so a handful of handler calls already in flight on other workers may still
  complete.
- Input must be **UTF-8** (or ASCII). A UTF-8 BOM is skipped; a UTF-16 BOM is
  rejected with an explicit error rather than silently mis-parsed.
- Steady state allocates nothing: the sequential parser is 0 allocs/op once
  warm (enforced by `TestAllocsSequentialNext`), and `ParseRecords` costs a
  small per-call constant (channels + goroutines) with 0 allocs per record
  (`TestAllocsParseRecordsAmortized`).

## Performance

Apple M1 Pro (10 cores), Go 1.25, 100MB corpus generated by `cmd/genxml`
(the same workload gosax's benchmark uses; ~193k records, 2 attributes per
record, 2% CDATA fields, 1% comments — "default" below), plus a
construct-free variant (`cmd/genxml -cdata 0 -comments 0` — "clean", via
`FASTSAX_CLEAN_XML`). Workload: for every `<location>` field, substring
search in its text (gosax's `BenchmarkReader_Event` workload). `go test
-bench . -benchtime 10x`, quiet machine; parallel numbers vary ±5–10% run
to run. The corpus is not checked in — benchmarks skip until you generate
it:

```sh
go run ./cmd/genxml -size 100MB -out testdata/out.xml
```

(or point `FASTSAX_BENCH_XML` at an existing corpus).

**Headline (vs gosax's 611 MB/s file baseline): sequential 1.6–1.9x;
parallel ~8x on the default corpus, 8.4x on the clean corpus.** On an AMD
Ryzen 7 5800H (Windows, AVX2): gosax 820 MB/s, fastsax w8 3911 MB/s = 4.8x;
the AVX2 mask kernel measures 70.7 GB/s vs 42.8 (SSE2) and 4.9 (generic).
The field-layout predictor is now opt-in (`RecordConfig.PredictLayout`):
after the 16B SWAR text probe it measured net-negative even on fixed
layouts (-10%), and disabling it bought ~+3% at w8. (Numbers
below include the per-arch SIMD stage-1 hit mask and the 16B SWAR text
probe; see [docs/performance.md](docs/performance.md).)

| benchmark | MB/s | vs gosax |
|---|---|---|
| gosax, file | 611 | 1.00x |
| fastsax sequential, file, prefetch | 1013 | 1.66x |
| fastsax sequential, in-memory | 982 | 1.61x |
| fastsax sequential, in-memory, tape names (`Batch.Name`) | 1141 | 1.87x |
| fastsax parallel, 1 worker (pipeline cost) | 697 | 1.14x |
| fastsax parallel, 2 workers | 1717 | 2.81x |
| fastsax parallel, 4 workers | 2922 | 4.78x |
| fastsax parallel, 8 workers | 4753 | 7.78x |
| fastsax parallel, 10 workers (GOMAXPROCS) | 4720 | 7.73x |
| fastsax parallel, heavy handler, 8 workers | 3358 | 5.50x |
| fastsax parallel, file input, 8 workers | 4307 | 7.05x |
| fastsax parallel, clean corpus, 8 workers | 5104 | 8.35x |

The parallel plateau used to be the sequential stage-1 skeleton; stage 1 now
pre-walks upcoming windows on otherwise-idle workers (activated at
`Workers >= 4`), which is where the jump beyond ~1.7 GB/s comes from.
Back-to-back same-process comparison, stage 1 forced sequential vs forced
parallel (`BenchmarkParallelStage1`, MB/s):

| corpus | stage1 | w=1 | w=2 | w=4 | w=8 |
|---|---|---|---|---|---|
| default | seq | 619 | 1727 | 1738 | 1664 |
| default | par | 619 | 1694 | 3248 | 4234 |
| clean | seq | 633 | 1765 | 1840 | 1803 |
| clean | par | 625 | 1766 | 3285 | 4453 |

Sequential stage 1 caps the whole pipeline at ~1.7–1.8 GB/s no matter the
worker count; parallel stage 1 lifts that cap, and with the SIMD hit mask
the pipeline reaches 4.75 GB/s (default) / 5.1 GB/s (clean) at 8 workers —
7.8x / 8.4x over gosax. (The stage-1 grid above predates the SIMD kernels;
it still illustrates the seq-vs-par split accurately.)

Reading points honestly:

- **Tape names are the cheapest win**: the scanner records start-tag name
  lengths in the tape for free (the name loop's several compares became one
  table load — interleaved A/B measured 0% scanner cost), and a consumer that
  switches from `Name(b.Bytes(e))` to `b.Name(e)` gains ~10% end-to-end on
  the benchmark workload (`BenchmarkSequentialMemTapeName` vs
  `BenchmarkSequentialMem`). End-tag name lengths are deliberately *not*
  recorded: that variant cost ~2% in the scanner for names consumers rarely
  need. In record mode the predictor caches the name length per learned
  token; the residual cost on `ParseRecords` workers=1 measured ~1.5%
  (invisible at ≥2 workers, where stage 1 is the ceiling).
- **Negative results** (measured, interleaved A/B — the full catalog with
  numbers is in [docs/negative-results.md](docs/negative-results.md); do
  not retry blindly): software prefetch / memory-level parallelism in
  stage 2 (measured bound: zero), per-span SIMD kernel calls (-38%),
  multi-class SIMD masks including on attribute-heavy corpora (loses on
  both NEON and real AVX2), PGO (a wash), text-span length prediction
  (-19%), and the field-layout predictor on by default (net-negative after
  the 16B SWAR probe).

- **Numbers are input-dependent.** The fast walker consumes `"<!"`/`"<?"`
  constructs per-construct, so sparse CDATA/comments (the default corpus's 2%
  CDATA fields) no longer knock whole windows back to the full walker — the
  default-vs-clean gap at 8 workers is now ~5%, not 35%. Only truly hostile
  shapes (record-tag text inside quoted attributes, quotes throughout
  record-adjacent content) disable the fast path, degrading toward the
  stage1=seq row above, never below it.
- **Activation thresholds**: the parallel stage-1 pre-walk engages at
  `Workers >= 4`; below that, stage 1 runs sequentially and throughput tops
  out around 1.6–1.8 GB/s. With pre-walking active, scaling continues to
  8–10 workers (stage-2 workers, not stage 1, become the ceiling again).
- **Heavy handlers** (FNV-hash every name and payload) now cost what they
  compute: 3358 vs 4753 MB/s at 8 workers in the table above. When
  sequential stage 1 was the ceiling they looked free (its plateau hid the
  handler cost); parallel stage 1 removed that plateau, so handler work
  shows up — and keeps scaling with workers.
- **Allocations**: sequential parsing is 0 allocs/op steady-state and
  `ParseRecords` is 0 allocs per record; each `ParseRecords` *call* pays a
  setup constant (~60–230 allocs, growing with worker count) for goroutines
  and channels. Both are enforced by tests (`TestAllocsSequentialNext`,
  `TestAllocsParseRecordsAmortized`).
- File vs in-memory input barely differs on a page-cached file; prefetch
  matters more for genuinely slow readers.
- On slow inputs (network) everything is I/O-bound; `RecordStats.IOBound()`
  reports it so callers can drop `Workers` for subsequent runs.

## Limitations

- **No DTD processing.** `<!DOCTYPE>` (including internal subsets) is scanned
  and reported as a single `KindDocType` event, but custom entities declared
  there are not expanded; `Unescape` handles only the predefined five and
  numeric character references. Unknown entities are returned as errors by
  `Unescape` (the raw text is always available).
- **Lenient markup spellings** (as in gosax): the `CDATA[` in `<![…` is not
  verified, end-tag names are not checked against their start tags
  (well-formedness of nesting is not enforced), and attribute syntax inside
  tags is not validated beyond quote matching. Malformed input never panics
  and never produces out-of-bounds spans — it produces either a lenient parse
  or a `SyntaxError` — but fastsax is not a validator.
- **Namespace-unaware**: names are reported as raw bytes
  (`prefix:local`); no prefix resolution or `xmlns` tracking.
- **UTF-8 only** (UTF-16 input is rejected, not converted).
- Record mode requires records to fit in `MaxRecordSize` and treats
  everything outside the record tag as skippable framing.
- `KindText` may split at window boundaries (see contracts above).

## Testing

- Differential fuzzing against `encoding/xml` (`FuzzDifferential`) plus a
  big-window/tiny-window self-consistency check on every input.
- Boundary sweep: every corpus document is parsed at dozens of
  chunk/carry geometries and byte-offset alignments; all runs must produce
  identical coalesced event streams (this covers carry, jumbo, spill and
  record-straddling paths).
- Adversarial fast-skeleton tests (fake open/close tags inside CDATA,
  comments, PIs and quoted attributes) with counter assertions that the
  intended defense fired, and forced-full parity runs.
- SIMD kernel parity and safety: randomized cross-kernel parity
  (`TestScanKernParity`, generic vs assembly, SSE2 vs AVX2 via a force knob)
  and guard-page tests that place inputs flush against an unmapped page on
  darwin/linux and Windows, so any out-of-bounds kernel read faults.
- Race tests over the full worker/ordered/error matrix, goroutine-leak
  checks, and the allocation gates described above.

## Development docs

- [docs/design.md](docs/design.md) — the architecture: windows and the
  head-carry invariant, the event tape, the record pipeline, the speculative
  skeleton and its soundness argument, parallel stage 1, the SIMD kernels,
  and the field-layout predictor.
- [docs/performance.md](docs/performance.md) — measured results (M1 Pro and
  Ryzen 7 5800H) with the benchmarking methodology.
- [docs/negative-results.md](docs/negative-results.md) — optimizations that
  were measured and rejected, with their numbers.

## Credits

fastsax is a ground-up rewrite of the ideas in
[gosax](https://github.com/orisano/gosax) by Nao Yonashiro — batching and
parallelism are new, but the idea of a fast, lenient, span-reporting SAX
scanner for Go (and its treatment of markup spellings) comes from there, and
gosax is also the performance baseline this library is measured against.

Beyond the ideas, some code is directly derived from gosax sources under the
BSD 2-Clause License: the helpers in `helpers.go` (`Name`, `NextAttribute`,
`Unescape`, the whitespace table, `CData`/`Comment`), `scanMarkup`'s markup
dispatch in `scan.go`, and the benchmark parity workload in `bench_test.go`.
See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for the exact inventory
and license text.

## License

MIT — see [LICENSE](LICENSE). Portions derived from gosax remain under the
BSD 2-Clause License; see
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
