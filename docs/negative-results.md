# fastsax negative results

Optimizations that were prototyped, measured, and rejected. They are
documented with their numbers so nobody relitigates them blind — every entry
says what was measured, the verdict, and *why* the idea loses on this
codebase. Methodology and corpora are described in
[performance.md](performance.md); unless noted, numbers are from the Apple
M1 Pro machine on the 100MB default corpus.

The discipline behind these verdicts: a candidate had to clear both an
isolated bar (~≥10% on the exact production loop shape) and an interleaved
end-to-end A/B bar (~≥3–5%, separately built binaries, medians) to land.
Nothing was tuned blind — e.g. no AVX2 kernel was written until real AVX2
hardware was available to measure on.

## 1. Memory-level parallelism / software prefetch in stage 2 — NO-GO

**Hypothesis**: stage-2 record parsing is memory-latency-bound, so software
prefetch or interleaving two record parses would hide DRAM latency.

**Bound experiment first**: the exact stage-2 worker inner loop (scanRecord
through a live predictor + trivial-handler tape walk) over
production-shaped ~256KB groups, with scanRecord as the first toucher of
the data:

| variant | 1 thread | 8 threads (aggregate) |
|---|---|---|
| cold (stream 100MB from DRAM) | 954–961 MB/s | 6.40–6.76 GB/s |
| cold + pretouch (1 read/64B line before parsing) | 951–956 MB/s | 6.38–6.63 GB/s |
| hot (same L2-resident group repeatedly) | 951–955 MB/s | 6.31–6.77 GB/s |

**Hot == cold == pretouch at both 1 and 8 threads.** Even if every byte
were already cache-resident — better than any prefetch can achieve —
stage-2 throughput would not move. Group data is consumed as a contiguous
stream and the M1 hardware prefetcher fully hides DRAM; stage 2 is bound by
the scan loops' serial dependencies and branches. The maximum possible MLP
win is ~0%.

**In-situ confirmation**: a two-pass parseGroup (touch every 64B line of
the group's record span, then parse) measured end-to-end, interleaved,
6 rounds:

| bench | stock | pretouch |
|---|---|---|
| ParseRecords mem/trivial workers=4 | 3216 MB/s | 2999 MB/s (**-6.8%**) |
| ParseRecords mem/trivial workers=8 | 4223 MB/s | 3940 MB/s (**-6.7%**) |

The extra pass costs real time and buys nothing — and it also covers the
"remote-dirty lines from the reader core" scenario the isolated benchmark
cannot reproduce (pulling those lines early still loses). A genuine
interleave of two scanRecord state machines was not built: with the bound
at zero there is no upside to justify the rewrite.

Two follow-on facts worth keeping:

- The earlier "memory-latency-bound" inference (worker oversubscription
  keeps scaling past physical cores) is better explained by scheduling
  slack — oversubscribed goroutines fill pipeline bubbles when workers
  block on group handoff — not by cache misses.
- The isolated 8-thread stage-2 loop reaches ~6.5 GB/s aggregate while the
  full pipeline plateaued at ~4.2 GB/s at the time: the gap is pipeline
  overhead (stage 1 sharing cores, dispatch, reader), not stage-2 memory
  behavior.

**Do not retry** without first showing a nonzero hot-vs-cold gap on the
target hardware.

## 2. Per-span SIMD kernel calls in the scanners — NO-GO

A scanChunk variant routing the text scan and the tag-attribute scan
through a NEON `indexAny4` kernel (16-byte CMEQ blocks, RBIT+CLZ) per span:

| shape | stock scanChunk | per-span kernel |
|---|---|---|
| 1MB windows | 1455 MB/s | 901 MB/s (**-38%**) |
| 540B record spans | 1443 MB/s | 912 MB/s (**-37%**) |

Spans average ~9.8 bytes/event on the default corpus; a non-inlinable asm
call per span loses to the inline byte loops. This is why `maskByte64` has
exactly one call site, amortized over a whole window (see design.md §5) —
do not add kernel calls inside the byte-level scan loops.

## 3. Bitmask event tape inside scanChunk/scanRecord — NO-GO

A variant building `'<'` + `{'>','"','\''}` masks per scan call (both
NEON), with the scan loops consuming bits instead of bytes, raced against
stock and against the pure-Go 16-byte SWAR text probe ("probe" below, the
variant that eventually landed):

| shape | stock | mask tape | 16B probe |
|---|---|---|---|
| 1MB windows | 1455 MB/s | 1592 MB/s (+9.4%) | 1602 MB/s (+10.4%) |
| 540B record spans | 1443 MB/s | 1425 MB/s (**-1%**) | 1597 MB/s (+10.7%) |

At window granularity the mask tape only ties the pure-Go probe (no asm, no
state). At record granularity — the shape stage-2 workers actually run —
two kernel calls plus mask setup per ~540B record eat the entire win.
(Measured with the field-layout predictor active, which already bypassed
tag scanning on most markup, leaving text scanning as the only hot loop —
which the inline probe covers.) Strictly dominated.

## 4. Multimask: `'>'` + quote masks for attribute-heavy shapes — NO-GO

Motivated by the question whether extending the SIMD masks to `'>'` and
quote characters would pay off on attribute-heavy XML. Result 3 above had measured a
multi-class mask tape NO-GO on the *default* corpus (2 short attrs/record);
the open question was whether attribute-heavy shapes flip the verdict. They
do not. Measured on both the M1 Pro (NEON) and a real-AVX2 Zen 3 machine
(Ryzen 7 5800H); the prototypes were test-only and production code was
never changed.

**Corpora** (recipes in performance.md): attrheavy — 8 attrs/record,
values 20–60B, ~25% containing a raw `'>'`, ~25% an apostrophe; start-tag
bytes ≈ 45% of scan time. attrdense — 12 attrs/record + 4 attrs on each of
4 fields, records nearly all start-tag bytes. Both regenerate
byte-identically across platforms (seeded PRNG).

**Baseline profile first** (stock, M1): on attrheavy record spans
(1.95–2.0 GB/s single-thread), the start-tag region is ~45–50% of scan time
— name loop ~17%, attr byte loop ~10%, quoted-value IndexByte skips ~16% —
text branch ~28%, end tags ~13%. So start-tag scanning *is* hot on
attrheavy: the question was live, and the no-go below is a measured loss,
not an irrelevance.

**Structural finding**: on attrdense the fast skeleton turns *itself* off —
quotes inside every record body make the quote-free gap proof fail by
design (2070 gap walks; walks×2 > hits → `fastOff`; 159/169 windows go
through `skeletonFull`, which runs at the same ~2.5 GB/s there). Masks
would not remove the construct walks (the quotes are really there), so
per-window stage-1 masks never amortize on exactly the corpus that
motivated them.

**Prototypes raced** (isolated = the exact stage-2 shape: record spans
carved like groups; interleaved single-invocation rounds, order-balanced,
medians):

- *swar3* (null hypothesis, pure Go): attr loop probes 8B per iteration
  with three SWAR compares (`'>'`|`'"'`|`'\''`), byte-loop tail.
- *maskgroup2*: `'>'`/`'"'`/`'\''` bitmasks built per ~256KB group with
  `maskByte64` (amortized like stage 1 — a granularity result 3 never
  tried), consumed with manually inlined bit-cursor hops; quoted values
  skipped via the per-quote-char mask.
- *maskrec*: same masks built per record (result 3's granularity).
- *maskpre* (diagnostic): masks prebuilt outside the timed loop, as a
  free-build upper bound. Caveat found: prebuilding masks for the whole
  corpus adds ~5MB of mask footprint whose cache misses make it
  *underperform* maskgroup2 — one hot 12KB mask buffer reused per group is
  the only fair configuration, so the free-build bound was computed as
  maskgroup2 minus measured build time instead.

Parity: all variants byte-exact vs scanChunk on
default+attrheavy+attrdense+clean (windows, record spans, group offsets)
plus adversarial truncation sweeps, on NEON, SSE2, and the real-AVX2 kernel.

**Results** (MB/s, medians; stock = current scanners):

M1 Pro (6 rounds):

| corpus | stock | swar3 | maskgroup2 | maskrec |
|---|---|---|---|---|
| attrheavy | 1906 | 1594 (**-16%**) | 1609 (**-16%**) | 1541 (**-19%**) |
| attrdense | 2668 | 2181 (**-18%**) | 2461 (**-8%**) | 2594 (**-3%**) |
| default | 1537 | 1215 (**-21%**) | 1191 (**-23%**) | — |

Ryzen 7 5800H, real AVX2 (8 samples, order-balanced ABBA):

| corpus | stock | swar3 | maskgroup2 | maskrec |
|---|---|---|---|---|
| attrheavy | 1605 | 1542 (-3.9%) | 1409 (**-12%**) | 1278 (**-20%**) |
| attrdense | 2991 | 2997 (+0.2%) | 3022 (+1.0%) | 2783 (**-7%**) |
| default | 1158 | 1102 (-4.8%) | 981 (**-15%**) | 886 (**-23%**) |

Best case anywhere: maskgroup2 +1.0% on Zen3/attrdense — a tie, on one
arch, on the most favorable corpus. Everything else is a regression, on
both architectures. No e2e A/B was needed: an isolated loss on the exact
stage-2 byte-for-byte workload upper-bounds e2e.

**Why stock wins**:

- The stock loop already offloads the *bulk* bytes: quoted values (the
  majority of attr bytes) are skipped with one `bytes.IndexByte` (SIMD
  inside the runtime) per value; the inter-quote gaps the byte loop
  actually walks are ~8 bytes (` attrN="`), where predictable per-byte
  branches on an 8-wide OoO core beat SWAR word setup plus an unpredictable
  match branch.
- Mask consumption replaces one IndexByte call per value with two
  bit-cursor hops (class hop + quote hop) plus mask-word loads; even
  manually inlined it nets slower on M1 and only ties on Zen 3.
- The single hottest start-tag loop — the element-name scan (7-character
  stop set including whitespace) — is not covered by 3-character masks at
  all.
- Mask build at group granularity costs ~11% of scan time (17.7 GB/s for
  three `maskByte64` passes vs ~2 GB/s scan); per-record build is worse on
  M1 except on attrdense, where the smaller cache footprint partially
  compensates.

**Stage-1 `'>'`+quote masks (bound, not built)**: resolveGap's IndexByte
quote sweeps are 13% of a walk that runs at 2.7 GB/s on attrheavy; two
extra per-window kernel passes cost ~10% of the same walk, so the ceiling
is ~0 even before replacing the sweeps with (non-free) masked range tests.
The dominant stage-1 cost on attrheavy is startTagEnd (49%) — the same loop
shape that lost in stage 2. And on attrdense stage 1 runs `skeletonFull`
(see the structural finding), where per-window masks never get built.

**One-pass 3-class kernel (bound, not built)**: three `maskByte64` calls
build at 17.7 GB/s (M1); a perfect fused ×3 kernel could at most halve the
~11% build overhead, i.e. recover ~5% — lifting maskgroup2 from -8% to
~parity on M1/attrdense and leaving every other cell negative.

The genxml flags added for these corpora (`-fields`, `-fieldattrs`,
`-attrvaluelen`) are kept as test tooling; default-flag output is
byte-identical to the original corpus.

## 5. Field-layout predictor on by default — reverted to opt-in

The per-worker field-layout predictor (design.md §6) predates the 16-byte
SWAR text probe in the scanners. Once the probe landed, the predictor
measured **net-negative ~-10% even on fixed layouts** and a tie on shuffled
ones (`BenchmarkPredictScanRecord`), and disabling it bought ~+3% at
8 workers: most of what prediction saved was the text scanning the probe
now does cheaply, while the predictor's bookkeeping remained. It is kept
behind `RecordConfig.PredictLayout` (off by default) because it may still
pay off for layouts dominated by long repeated tags.

## 6. Text-span length prediction — NO-GO

Guessing each field's text length from the previous record (probe
`data[p+prevLen] == '<'`, then a verifying walk): the layout repeats but
text lengths vary record to record on realistic corpora, so the guess
almost never hit, while the per-text-event bookkeeping cost **+19%** on
`BenchmarkPredictScanRecord/fixed/predict` and +6% on ParseRecords
workers=1 (interleaved A/B, M1 Pro). See the comment in `scanRecord`
(predict.go). Do not retry without a corpus of truly fixed-width fields.

## 7. End-tag name lengths in the tape — NO-GO

Recording start-tag name lengths in the tape is free (the name loop's
compares became one table load; 0% measured scanner cost) and gives
consumers ~10% end-to-end. Extending the same to *end tags* cost ~2% in the
scanner — for names consumers rarely need, since `Batch.Name` falls back to
`Name()` transparently on `KindEnd`. Not recorded.

## 8. Profile-guided optimization — wash

A representative profile (sequential + tape-name + record-mode benchmarks)
was built and A/B-measured against `-pgo=off` twice with independent
profiles: ~-2.5% on ParseRecords workers=1, +1–1.6% on the tape-name
sequential bench, +~1% at 4 workers — a wash. The hot loops are already
hand-inlined and interface-free, which is most of what PGO buys. Go also
does not auto-apply a library's `default.pgo` (only main packages get
`-pgo=auto`), so a profile is not shipped.

To retry: `go test -run '^$' -bench '<representative set>' -cpuprofile
default.pgo .`, then benchmark `go test -c -pgo=default.pgo` vs `-pgo=off`,
interleaved.
