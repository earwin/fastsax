package fastsax

import (
	"bytes"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
)

// Alloc gates. Steady-state sequential parsing must be exactly
// allocation-free; ParseRecords may spend a small constant per call (channels,
// goroutines, run state) but nothing per record.

// allocDoc builds a document of n identical small records.
func allocDoc(n int) []byte {
	var b bytes.Buffer
	b.WriteString("<root>\n")
	for i := range n {
		fmt.Fprintf(&b, "<rec id=\"%d\"><a>alpha %d</a><b>beta</b><c/></rec>\n", i, i)
	}
	b.WriteString("</root>\n")
	return b.Bytes()
}

// TestAllocsSequentialNext: after warm-up, a full parse through a reused
// Parser (prefetch off, Reset per pass) performs zero allocations.
func TestAllocsSequentialNext(t *testing.T) {
	doc := allocDoc(2000)
	rd := bytes.NewReader(doc)
	p, err := NewParser(rd, WithPrefetch(false), WithChunkSize(8192), WithCarryCap(512))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	var sink int
	pass := func() {
		rd.Reset(doc)
		p.Reset(rd)
		for {
			b, err := p.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			sink += len(b.Events)
		}
	}
	for range 3 { // warm-up: tape and carry buffers reach their final capacity
		pass()
	}
	if allocs := testing.AllocsPerRun(10, pass); allocs != 0 {
		t.Fatalf("sequential steady state: %v allocs per full parse, want 0", allocs)
	}
	if sink == 0 {
		t.Fatal("no events parsed")
	}
}

// TestAllocsParseRecordsAmortized: ParseRecords allocates a small per-call
// constant, and nothing that scales with the number of records — measured as
// the alloc delta between a 100-record and an 1100-record document (same
// shape), which must be ~0 per extra record for every mode.
func TestAllocsParseRecordsAmortized(t *testing.T) {
	small, big := allocDoc(100), allocDoc(1100)
	var sink atomic.Int64 // handle runs concurrently from unordered workers
	handle := func(_ int, rec Record) error {
		sink.Add(int64(len(rec.Events)))
		return nil
	}
	for _, tc := range []struct {
		name    string
		workers int
		ordered bool
		chunk   int
		carry   int
	}{
		{"workers=1", 1, false, 0, 0},
		{"workers=4", 4, false, 0, 0},
		{"workers=4/ordered", 4, true, 0, 0},
		// Small chunks force many windows through the parallel stage-1
		// pipeline (Workers >= 4 activates it), so pre-walk tasks, event
		// buffers and replay must all be allocation-free per window too.
		{"workers=4/pstage1-multichunk", 4, false, 2048, 256},
		{"workers=8/pstage1-multichunk/ordered", 8, true, 2048, 256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if raceEnabled && tc.chunk != 0 {
				// Multichunk runs cycle many pooled objects (groups, spill,
				// stage-1 tasks) per run; under -race sync.Pool.Put randomly
				// drops items by design, so the warm-run alloc count scales
				// with windows and the zero-alloc gate is unmeasurable.
				t.Skip("pool-drop noise under the race detector")
			}
			cfg := RecordConfig{Tag: []byte("rec"), Workers: tc.workers, Ordered: tc.ordered,
				ChunkSize: tc.chunk, CarryCap: tc.carry}
			measure := func(doc []byte) float64 {
				rd := bytes.NewReader(doc)
				for range 2 { // warm the package-level pools for this geometry
					rd.Reset(doc)
					if err := ParseRecords(rd, cfg, handle); err != nil {
						t.Fatal(err)
					}
				}
				return testing.AllocsPerRun(5, func() {
					rd.Reset(doc)
					if err := ParseRecords(rd, cfg, handle); err != nil {
						t.Fatal(err)
					}
				})
			}
			aSmall := measure(small)
			aBig := measure(big)
			perRecord := (aBig - aSmall) / 1000
			// Both docs must cost the same warm per-call constant; the slack
			// absorbs sync.Pool per-P cache effects under the race scheduler.
			if perRecord > 0.05 {
				t.Fatalf("per-record allocs = %.3f (small=%v big=%v), want ~0",
					perRecord, aSmall, aBig)
			}
			// And the constant itself stays a per-run constant: parsing 1100
			// records costs (integer) zero allocations per record.
			if int(aBig)/1100 != 0 {
				t.Fatalf("warm run allocs = %v for 1100 records; not a per-run constant", aBig)
			}
		})
	}
	if sink.Load() == 0 {
		t.Fatal("no events parsed")
	}
}
