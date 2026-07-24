package fastsax

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"testing"
)

// maxParWorkers bounds the per-worker stat slice; comfortably above any
// workers value used below (ordered mode always reports worker 0).
const maxParWorkers = 64

// parStat is one worker's accumulator, padded to a cache line so concurrent
// workers never false-share.
type parStat struct {
	records int64
	africa  int64
	hash    uint64
	_       [40]byte
}

func clearParStats(stats []parStat) {
	for i := range stats {
		stats[i] = parStat{}
	}
}

func sumParStats(stats []parStat) (records, africa int64) {
	for i := range stats {
		records += stats[i].records
		africa += stats[i].africa
	}
	return records, africa
}

// trivialRecordHandler is the record-mode twin of countAfrica: count records,
// and count "Africa" mentions in the text of <location> fields. It walks the
// record tape the way a real consumer would (Name on start tags).
func trivialRecordHandler(stats []parStat) func(int, Record) error {
	return func(w int, rec Record) error {
		st := &stats[w]
		st.records++
		inLoc := false
		for _, e := range rec.Events {
			switch e.Kind {
			case KindStart:
				name, _ := Name(rec.Data[e.Start:e.End])
				inLoc = string(name) == "location"
			case KindEnd, KindSelfClose:
				inLoc = false
			case KindText:
				if inLoc && bytes.Contains(rec.Data[e.Start:e.End], africa) {
					st.africa++
				}
			}
		}
		return nil
	}
}

// heavyRecordHandler FNV-1a-hashes every field name and every text/CDATA
// payload, simulating a handler that actually consumes all record data (the
// regime where parallelism should approach linear scaling).
func heavyRecordHandler(stats []parStat) func(int, Record) error {
	return func(w int, rec Record) error {
		st := &stats[w]
		st.records++
		h := uint64(14695981039346656037) // FNV-1a offset basis
		for _, e := range rec.Events {
			switch e.Kind {
			case KindStart, KindEnd, KindSelfClose:
				name, _ := Name(rec.Data[e.Start:e.End])
				h = fnv1a(h, name)
			case KindText, KindCData:
				h = fnv1a(h, rec.Data[e.Start:e.End])
			}
		}
		st.hash ^= h
		return nil
	}
}

func fnv1a(h uint64, b []byte) uint64 {
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}

// seqRecordCount counts <record> start tags with the sequential Parser —
// the reference for TestParallelParity's record counts.
func seqRecordCount(data []byte) (int64, error) {
	p, err := NewParser(bytes.NewReader(data), WithPrefetch(false))
	if err != nil {
		return 0, err
	}
	defer p.Close()
	var n int64
	for {
		b, err := p.Next()
		if err != nil {
			if err == io.EOF {
				return n, nil
			}
			return 0, err
		}
		for _, e := range b.Events {
			if e.Kind != KindStart && e.Kind != KindSelfClose {
				continue
			}
			name, _ := Name(b.Bytes(e))
			if string(name) == "record" {
				n++
			}
		}
	}
}

// TestParallelParity guards the meaning of the parallel benchmarks: record
// and Africa counts must be identical across worker counts, ordered and
// unordered delivery, and must match the sequential Parser over the same file.
func TestParallelParity(t *testing.T) {
	data := loadBenchData(t)
	wantRec, err := seqRecordCount(data)
	if err != nil {
		t.Fatalf("sequential record count: %v", err)
	}
	p, err := NewParser(bytes.NewReader(data), WithPrefetch(false))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	seqAfrica, _, err := countAfrica(p, nil)
	if err != nil {
		t.Fatalf("sequential countAfrica: %v", err)
	}
	if wantRec == 0 || seqAfrica == 0 {
		t.Fatalf("degenerate corpus: records=%d africa=%d", wantRec, seqAfrica)
	}
	stats := make([]parStat, maxParWorkers)
	for _, tc := range []struct {
		workers int
		ordered bool
	}{
		{1, false}, {1, true}, {8, false}, {8, true},
	} {
		name := fmt.Sprintf("workers=%d/ordered=%v", tc.workers, tc.ordered)
		t.Run(name, func(t *testing.T) {
			clearParStats(stats)
			err := ParseRecords(bytes.NewReader(data), RecordConfig{
				Tag: []byte("record"), Workers: tc.workers, Ordered: tc.ordered,
			}, trivialRecordHandler(stats))
			if err != nil {
				t.Fatal(err)
			}
			rec, afr := sumParStats(stats)
			if rec != wantRec || afr != int64(seqAfrica) {
				t.Fatalf("records=%d africa=%d, want records=%d africa=%d",
					rec, afr, wantRec, seqAfrica)
			}
		})
	}
}

// BenchmarkParallelRecords is the scaling matrix: workers 1..8 ×
// {preloaded bytes.Reader, os.File} × {trivial, heavy} handler over the
// shared 100MB corpus. File mode reopens the file every iteration so I/O and
// setup are part of the measured work (mirrors BenchmarkSequential).
func BenchmarkParallelRecords(b *testing.B) {
	data := loadBenchData(b)
	size := int64(len(data))
	stats := make([]parStat, maxParWorkers)
	handlers := []struct {
		name string
		mk   func([]parStat) func(int, Record) error
	}{
		{"trivial", trivialRecordHandler},
		{"heavy", heavyRecordHandler},
	}
	for _, input := range []string{"mem", "file"} {
		for _, h := range handlers {
			for _, w := range []int{1, 2, 4, 8} {
				b.Run(fmt.Sprintf("%s/%s/workers=%d", input, h.name, w), func(b *testing.B) {
					handle := h.mk(stats)
					cfg := RecordConfig{Tag: []byte("record"), Workers: w}
					b.SetBytes(size)
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						clearParStats(stats)
						var r io.Reader
						var f *os.File
						if input == "file" {
							var err error
							if f, err = os.Open(benchXML); err != nil {
								b.Fatal(err)
							}
							r = f
						} else {
							r = bytes.NewReader(data)
						}
						err := ParseRecords(r, cfg, handle)
						if f != nil {
							f.Close()
						}
						if err != nil {
							b.Fatal(err)
						}
						if rec, _ := sumParStats(stats); rec == 0 {
							b.Fatal("no records parsed")
						}
					}
				})
			}
		}
	}
}

// BenchmarkParallelClean measures record mode over a corpus free of CDATA and
// comments (cmd/genxml -cdata 0 -comments 0), where the speculative fast
// skeleton stays active — the upper end of fastsax's input-dependent range.
// Set FASTSAX_CLEAN_XML to the corpus path to enable it.
func BenchmarkParallelClean(b *testing.B) {
	path := os.Getenv("FASTSAX_CLEAN_XML")
	if path == "" {
		b.Skip("set FASTSAX_CLEAN_XML to a corpus generated with cmd/genxml -cdata 0 -comments 0")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		b.Fatal(err)
	}
	stats := make([]parStat, maxParWorkers)
	handle := trivialRecordHandler(stats)
	for _, w := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", w), func(b *testing.B) {
			cfg := RecordConfig{Tag: []byte("record"), Workers: w}
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				clearParStats(stats)
				if err := ParseRecords(bytes.NewReader(data), cfg, handle); err != nil {
					b.Fatal(err)
				}
				if rec, _ := sumParStats(stats); rec == 0 {
					b.Fatal("no records parsed")
				}
			}
		})
	}
}

// BenchmarkParallelVsSequential is the headline comparison: the identical
// trivial workload (count records/Africa-in-location) through the sequential
// batch Parser vs ParseRecords at GOMAXPROCS workers, from memory and from
// the file.
func BenchmarkParallelVsSequential(b *testing.B) {
	data := loadBenchData(b)
	size := int64(len(data))
	b.Run("sequential/mem", func(b *testing.B) {
		rd := bytes.NewReader(data)
		p, err := NewParser(rd)
		if err != nil {
			b.Fatal(err)
		}
		defer p.Close()
		b.SetBytes(size)
		b.ReportAllocs()
		var pend []byte
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
	b.Run("sequential/file", func(b *testing.B) {
		b.SetBytes(size)
		b.ReportAllocs()
		var pend []byte
		for i := 0; i < b.N; i++ {
			f, err := os.Open(benchXML)
			if err != nil {
				b.Fatal(err)
			}
			p, err := NewParser(f)
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
	stats := make([]parStat, maxParWorkers)
	handle := trivialRecordHandler(stats)
	cfg := RecordConfig{Tag: []byte("record"), Workers: runtime.GOMAXPROCS(0)}
	b.Run("parallel/mem", func(b *testing.B) {
		b.SetBytes(size)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			clearParStats(stats)
			if err := ParseRecords(bytes.NewReader(data), cfg, handle); err != nil {
				b.Fatal(err)
			}
			if _, afr := sumParStats(stats); afr == 0 {
				b.Fatal("no matches")
			}
		}
	})
	b.Run("parallel/file", func(b *testing.B) {
		b.SetBytes(size)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			clearParStats(stats)
			f, err := os.Open(benchXML)
			if err != nil {
				b.Fatal(err)
			}
			err = ParseRecords(f, cfg, handle)
			f.Close()
			if err != nil {
				b.Fatal(err)
			}
			if _, afr := sumParStats(stats); afr == 0 {
				b.Fatal("no matches")
			}
		}
	})
}
