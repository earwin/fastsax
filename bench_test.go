// countAfricaGosax and the countAfrica workload are translated from gosax's
// bench_test.go (https://github.com/orisano/gosax), Copyright (c) 2024
// Nao Yonashiro, licensed under the BSD 2-Clause License. See
// THIRD_PARTY_NOTICES.md.

package fastsax

import (
	"bytes"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/orisano/gosax"
)

// benchXML is the 100MB benchmark corpus (the same workload gosax's baseline
// benchmark uses, so numbers are directly comparable). Generate it with
//
//	go run ./cmd/genxml -size 100MB -out testdata/out.xml
//
// or set FASTSAX_BENCH_XML to point at an existing corpus.
var benchXML = func() string {
	if p := os.Getenv("FASTSAX_BENCH_XML"); p != "" {
		return p
	}
	return "testdata/out.xml"
}()

// benchXMLHint is appended to skip messages when the corpus is missing.
const benchXMLHint = "generate it with: go run ./cmd/genxml -size 100MB -out testdata/out.xml"

var africa = []byte("Africa")

// countAfrica replicates the workload of gosax's BenchmarkReader_Event: for
// every <location> element, count text runs containing "Africa". Because
// fastsax may split a text node at a window boundary (gosax never does), a
// text event that ends exactly at the window edge is buffered in pend and the
// Contains check is deferred until the run is known to be complete. scratch is
// returned so callers can reuse the buffer across iterations.
func countAfrica(p *Parser, scratch []byte) (count int, _ []byte, err error) {
	inLoc := false
	pend := scratch[:0]
	open := false // pend holds a text run that may continue in the next window
	for {
		b, err := p.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, pend, err
		}
		edge := uint32(len(b.Data))
		for _, e := range b.Events {
			if e.Kind == KindText {
				if !inLoc {
					continue
				}
				t := b.Data[e.Start:e.End]
				if e.End == edge {
					// May be cut at the window edge; defer the check.
					if !open {
						pend = pend[:0]
						open = true
					}
					pend = append(pend, t...)
					continue
				}
				if open {
					pend = append(pend, t...)
					t = pend
					open = false
				}
				if bytes.Contains(t, africa) {
					count++
				}
				continue
			}
			// Any markup event terminates a pending text run.
			if open {
				if bytes.Contains(pend, africa) {
					count++
				}
				open = false
			}
			switch e.Kind {
			case KindStart:
				name, _ := Name(b.Bytes(e))
				inLoc = string(name) == "location"
			case KindEnd, KindSelfClose:
				inLoc = false
			}
		}
	}
	if open && bytes.Contains(pend, africa) {
		count++
	}
	return count, pend, nil
}

// countAfricaTape is countAfrica with element names sliced straight from the
// tape (Batch.Name, using the name length the scanner recorded) instead of
// re-scanning tag bytes with Name(). Kept byte-for-byte parallel to
// countAfrica so the two benches isolate exactly that difference.
func countAfricaTape(p *Parser, scratch []byte) (count int, _ []byte, err error) {
	inLoc := false
	pend := scratch[:0]
	open := false // pend holds a text run that may continue in the next window
	for {
		b, err := p.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, pend, err
		}
		edge := uint32(len(b.Data))
		for _, e := range b.Events {
			if e.Kind == KindText {
				if !inLoc {
					continue
				}
				t := b.Data[e.Start:e.End]
				if e.End == edge {
					// May be cut at the window edge; defer the check.
					if !open {
						pend = pend[:0]
						open = true
					}
					pend = append(pend, t...)
					continue
				}
				if open {
					pend = append(pend, t...)
					t = pend
					open = false
				}
				if bytes.Contains(t, africa) {
					count++
				}
				continue
			}
			// Any markup event terminates a pending text run.
			if open {
				if bytes.Contains(pend, africa) {
					count++
				}
				open = false
			}
			switch e.Kind {
			case KindStart:
				inLoc = string(b.Name(e)) == "location"
			case KindEnd, KindSelfClose:
				inLoc = false
			}
		}
	}
	if open && bytes.Contains(pend, africa) {
		count++
	}
	return count, pend, nil
}

// countAfricaGosax is gosax's countAfrica verbatim, returning the count.
func countAfricaGosax(r io.Reader) (int, error) {
	gr := gosax.NewReader(r)
	count := 0
	inLoc := false
	for {
		e, err := gr.Event()
		if err != nil {
			return 0, err
		}
		if e.Type() == gosax.EventEOF {
			return count, nil
		}
		switch e.Type() {
		case gosax.EventStart:
			name, _ := gosax.Name(e.Bytes)
			inLoc = string(name) == "location"
		case gosax.EventEnd:
			inLoc = false
		case gosax.EventText:
			if inLoc && bytes.Contains(e.Bytes, africa) {
				count++
			}
		}
	}
}

var benchDataOnce sync.Once
var benchData []byte
var benchDataErr error

func loadBenchData(tb testing.TB) []byte {
	benchDataOnce.Do(func() {
		benchData, benchDataErr = os.ReadFile(benchXML)
	})
	if benchDataErr != nil {
		tb.Skipf("bench corpus unavailable: %v (%s)", benchDataErr, benchXMLHint)
	}
	return benchData
}

// TestCountAfricaParity asserts that fastsax and gosax agree on the benchmark
// workload over the shared 100MB corpus, including window geometries that
// force text splits and carry.
func TestCountAfricaParity(t *testing.T) {
	data := loadBenchData(t)
	want, err := countAfricaGosax(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("gosax: %v", err)
	}
	if want == 0 {
		t.Fatal("gosax found no matches; corpus is unsuitable")
	}
	for _, tc := range []struct {
		name string
		opts []Option
	}{
		{"default", nil},
		{"noprefetch", []Option{WithPrefetch(false)}},
		{"tiny-windows", []Option{WithPrefetch(false), WithChunkSize(4096), WithCarryCap(512)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := NewParser(bytes.NewReader(data), tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			got, _, err := countAfrica(p, nil)
			if err != nil {
				t.Fatalf("fastsax: %v", err)
			}
			if got != want {
				t.Fatalf("count mismatch: fastsax=%d gosax=%d", got, want)
			}
		})
	}
}

// BenchmarkSequential mirrors gosax's BenchmarkReader_Event: open the file
// and build the parser inside every iteration, so setup cost and disk I/O are
// part of the measured work.
func BenchmarkSequential(b *testing.B) {
	st, err := os.Stat(benchXML)
	if err != nil {
		b.Skipf("bench corpus unavailable: %v (%s)", err, benchXMLHint)
	}
	for _, tc := range []struct {
		name string
		opts []Option
	}{
		{"prefetch", nil},
		{"noprefetch", []Option{WithPrefetch(false)}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(st.Size())
			var pend []byte
			for i := 0; i < b.N; i++ {
				f, err := os.Open(benchXML)
				if err != nil {
					b.Fatal(err)
				}
				p, err := NewParser(f, tc.opts...)
				if err != nil {
					f.Close()
					b.Fatal(err)
				}
				var count int
				count, pend, err = countAfrica(p, pend)
				p.Close()
				f.Close()
				if err != nil {
					b.Fatal(err)
				}
				if count == 0 {
					b.Fatal("no matches")
				}
			}
		})
	}
}

// BenchmarkSequentialMem parses the corpus from memory through a reused
// parser, isolating pure parse throughput from disk and setup cost.
func BenchmarkSequentialMem(b *testing.B) {
	data := loadBenchData(b)
	for _, tc := range []struct {
		name string
		opts []Option
	}{
		{"prefetch", nil},
		{"noprefetch", []Option{WithPrefetch(false)}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			rd := bytes.NewReader(data)
			p, err := NewParser(rd, tc.opts...)
			if err != nil {
				b.Fatal(err)
			}
			defer p.Close()
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			var pend []byte
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rd.Reset(data)
				p.Reset(rd)
				var count int
				count, pend, err = countAfrica(p, pend)
				if err != nil {
					b.Fatal(err)
				}
				if count == 0 {
					b.Fatal("no matches")
				}
			}
		})
	}
}

// BenchmarkSequentialMemTapeName is BenchmarkSequentialMem's prefetch variant
// with countAfricaTape: element names come from the tape (Batch.Name) instead
// of re-scanning tag bytes with Name(). The delta against
// BenchmarkSequentialMem/prefetch is the end-to-end value of tape name
// lengths.
func BenchmarkSequentialMemTapeName(b *testing.B) {
	data := loadBenchData(b)
	rd := bytes.NewReader(data)
	p, err := NewParser(rd)
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	var pend []byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rd.Reset(data)
		p.Reset(rd)
		var count int
		count, pend, err = countAfricaTape(p, pend)
		if err != nil {
			b.Fatal(err)
		}
		if count == 0 {
			b.Fatal("no matches")
		}
	}
}

// BenchmarkGosaxMem is the gosax workload over the same in-memory corpus, for
// an apples-to-apples comparison with BenchmarkSequentialMem.
func BenchmarkGosaxMem(b *testing.B) {
	data := loadBenchData(b)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	rd := bytes.NewReader(data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rd.Reset(data)
		count, err := countAfricaGosax(rd)
		if err != nil {
			b.Fatal(err)
		}
		if count == 0 {
			b.Fatal("no matches")
		}
	}
}
