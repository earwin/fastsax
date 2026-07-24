package fastsax

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
)

// TestRecordNameParityAllPaths asserts Batch.Name/Record.Name (the Event.nlen
// tape-name consumer) on records produced through every event-building path
// of the system:
//
//  1. the sequential parser (Parser.Next batches, scanChunk),
//  2. ParseRecords with Workers == 1 (inline parseGroup + predictor),
//  3. ParseRecords with sequential stage 1 (worker subparse via scanRecord),
//  4. ParseRecords with parallel stage 1 (capture replay → worker subparse),
//  5. the predictor hit path in isolation (scanRecord, learn pass then hit pass).
//
// For every tag event the tape name must equal the Name() fallback, and each
// path must actually record nonzero name lengths (proving nlen is filled, not
// silently falling back everywhere). Paths 3-5 additionally assert their
// machinery engaged (s1 hits, predictor hits).
func TestRecordNameParityAllPaths(t *testing.T) {
	// Repetitive records (predictor food) with varied tag shapes: attributes
	// with markup bytes, self-closing with and without attributes, prefixed
	// names, whitespace-guarded close tags, CDATA/comment fields, plus a few
	// degenerate names that must take the Name() fallback.
	var sb strings.Builder
	sb.WriteString("<db>\n")
	for i := range 300 {
		fmt.Fprintf(&sb,
			`<rec id="%d"><name a="x>y" b='<'>n%d</name><ns:v>%d</ns:v><flag/><w s="1"/>`+
				`<blob><![CDATA[fake </rec> ]]></blob><!-- c --><note >t</note ></rec>`+"\n", i, i, i*3)
	}
	sb.WriteString(`<rec><a"q">deg</a"q"></rec>` + "\n") // quote after name: NameLen 0 fallback
	sb.WriteString("</db>\n")
	doc := []byte(sb.String())

	// checkTape verifies one event tape against data; returns how many tag
	// events carried a recorded (nonzero) name length.
	checkTape := func(data []byte, evs []Event) (recorded int, err error) {
		for _, e := range evs {
			isTag := e.Kind == KindStart || e.Kind == KindEnd || e.Kind == KindSelfClose
			if !isTag {
				if e.NameLen() != 0 {
					return 0, fmt.Errorf("non-tag event %v has NameLen %d", e.Kind, e.NameLen())
				}
				continue
			}
			want, _ := Name(data[e.Start:e.End])
			got := eventName(data, e)
			if !bytes.Equal(got, want) {
				return 0, fmt.Errorf("event %v %q: tape name %q, Name() %q",
					e.Kind, data[e.Start:e.End], got, want)
			}
			if e.NameLen() != 0 {
				recorded++
			}
		}
		return recorded, nil
	}

	// Path 1: sequential parser batches.
	t.Run("sequential-parser", func(t *testing.T) {
		for _, geom := range []struct{ chunk, carry int }{{1 << 20, 64 << 10}, {256, 64}} {
			p, err := NewParser(bytes.NewReader(doc), WithPrefetch(false),
				WithChunkSize(geom.chunk), WithCarryCap(geom.carry))
			if err != nil {
				t.Fatal(err)
			}
			recorded := 0
			for {
				b, err := p.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				n, cerr := checkTape(b.Data, b.Events)
				if cerr != nil {
					t.Fatalf("chunk=%d: %v (via Batch)", geom.chunk, cerr)
				}
				recorded += n
			}
			p.Close()
			if recorded == 0 {
				t.Fatalf("chunk=%d: no name lengths recorded on the tape", geom.chunk)
			}
		}
	})

	// Paths 2-4: ParseRecords through Record.Name, with per-mode engagement
	// assertions.
	parseAndCheck := func(t *testing.T, cfg RecordConfig) (nrec int) {
		t.Helper()
		var mu sync.Mutex
		recorded, count := 0, 0
		var firstErr error
		err := ParseRecords(bytes.NewReader(doc), cfg, func(w int, rec Record) error {
			n, cerr := checkTape(rec.Data, rec.Events)
			mu.Lock()
			if cerr != nil && firstErr == nil {
				firstErr = cerr
			}
			recorded += n
			count++
			mu.Unlock()
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if firstErr != nil {
			t.Fatal(firstErr)
		}
		if count != 301 {
			t.Fatalf("got %d records, want 301", count)
		}
		if recorded == 0 {
			t.Fatal("no name lengths recorded on any record tape")
		}
		return count
	}

	t.Run("parse-records-inline", func(t *testing.T) {
		var stats RecordStats
		parseAndCheck(t, RecordConfig{Tag: []byte("rec"), Workers: 1, PredictLayout: true,
			ChunkSize: 512, CarryCap: 64, Stats: &stats})
		if stats.PredictHits.Load() == 0 {
			t.Fatal("inline path never hit the predictor")
		}
	})

	t.Run("parse-records-seq-stage1", func(t *testing.T) {
		s1ForceSeq.Store(true)
		defer s1ForceSeq.Store(false)
		var stats RecordStats
		pre := snapS1()
		parseAndCheck(t, RecordConfig{Tag: []byte("rec"), Workers: 4, PredictLayout: true,
			ChunkSize: 512, CarryCap: 64, Stats: &stats})
		if d := snapS1().delta(pre); d.prewalks != 0 {
			t.Fatalf("forced-sequential run pre-walked: %+v", d)
		}
		if stats.PredictHits.Load() == 0 {
			t.Fatal("worker subparse never hit the predictor")
		}
	})

	t.Run("parse-records-par-stage1", func(t *testing.T) {
		s1ForceParallel.Store(true)
		defer s1ForceParallel.Store(false)
		var stats RecordStats
		pre := snapS1()
		parseAndCheck(t, RecordConfig{Tag: []byte("rec"), Workers: 4, PredictLayout: true,
			ChunkSize: 512, CarryCap: 64, Stats: &stats})
		if d := snapS1().delta(pre); d.hits == 0 {
			t.Fatalf("parallel run consumed no speculation (capture replay untested): %+v", d)
		}
		if stats.PredictHits.Load() == 0 {
			t.Fatal("worker subparse never hit the predictor")
		}
	})

	// Path 5: the predictor hit path in isolation — learn pass, then a hit
	// pass whose events flow through predictNext's cached nl.
	t.Run("predictor-hit", func(t *testing.T) {
		rec := []byte(`<rec id="7"><name a="x>y">n</name><ns:v>21</ns:v><flag/><w s="1"/>` +
			`<blob><![CDATA[fake </rec> ]]></blob><!-- c --><note >t</note ></rec>`)
		var pr fieldPredictor
		var last []Event
		for pass := range 2 {
			tape, cut, err := scanRecord(rec, nil, &pr)
			if err != nil || cut != len(rec) {
				t.Fatalf("pass %d: cut=%d/%d err=%v", pass, cut, len(rec), err)
			}
			recorded, cerr := checkTape(rec, tape)
			if cerr != nil {
				t.Fatalf("pass %d: %v", pass, cerr)
			}
			if recorded == 0 {
				t.Fatalf("pass %d: no name lengths recorded", pass)
			}
			last = tape
		}
		if pr.hits == 0 {
			t.Fatal("second pass produced no predictor hits")
		}
		// The hit-pass tape must be byte-identical to a plain scanChunk tape,
		// nlen included.
		plain, _, _ := scanChunk(rec, nil)
		if len(plain) != len(last) {
			t.Fatalf("tape length: predict=%d plain=%d", len(last), len(plain))
		}
		for i := range plain {
			if plain[i] != last[i] {
				t.Fatalf("event %d: predict %+v, plain %+v", i, last[i], plain[i])
			}
		}
	})
}
