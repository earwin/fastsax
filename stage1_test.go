package fastsax

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// s1Snap is a snapshot of the process-wide parallel-stage-1 counters. Package
// tests run sequentially, so deltas around a ParseRecords call are exact.
type s1Snap struct{ prewalks, hits, misses, disables int64 }

func snapS1() s1Snap {
	return s1Snap{
		prewalks: s1Stats.prewalks.Load(),
		hits:     s1Stats.hits.Load(),
		misses:   s1Stats.misses.Load(),
		disables: s1Stats.disables.Load(),
	}
}

func (s s1Snap) delta(prev s1Snap) s1Snap {
	return s1Snap{s.prewalks - prev.prewalks, s.hits - prev.hits,
		s.misses - prev.misses, s.disables - prev.disables}
}

// collectS1 runs collectParallel with parallel stage 1 forced on or off.
func collectS1(doc []byte, cfg RecordConfig, parallel bool) (map[int64]refRec, []int64, error) {
	if parallel {
		s1ForceParallel.Store(true)
		defer s1ForceParallel.Store(false)
	} else {
		s1ForceSeq.Store(true)
		defer s1ForceSeq.Store(false)
	}
	got, order, _, err := collectParallel(doc, cfg)
	return got, order, err
}

// checkS1Differential is the differential harness: the same input and config through
// sequential stage 1 and forced-parallel stage 1 must produce identical
// records (data, events, indices) and identical errors (message and offset).
// Successful ordered runs must also deliver indices in exact document order.
// Returns the parallel run's counter delta.
func checkS1Differential(t *testing.T, label string, doc []byte, cfg RecordConfig) s1Snap {
	t.Helper()
	seqGot, _, seqErr := collectS1(doc, cfg, false)
	pre := snapS1()
	parGot, parOrder, parErr := collectS1(doc, cfg, true)
	d := snapS1().delta(pre)

	if (seqErr == nil) != (parErr == nil) {
		t.Fatalf("%s: error mismatch:\n seq: %v\n par: %v", label, seqErr, parErr)
	}
	if seqErr != nil {
		if seqErr.Error() != parErr.Error() {
			t.Fatalf("%s: error text mismatch:\n seq: %v\n par: %v", label, seqErr, parErr)
		}
		// Which already-dispatched records reached the handler before the
		// error is timing-dependent in both modes; only the error is
		// contractual.
		return d
	}
	if len(parGot) != len(seqGot) {
		t.Fatalf("%s: got %d records parallel, %d sequential", label, len(parGot), len(seqGot))
	}
	for idx, want := range seqGot {
		g, ok := parGot[idx]
		if !ok {
			t.Fatalf("%s: parallel run missing record %d", label, idx)
		}
		if g.data != want.data {
			t.Fatalf("%s: record %d data mismatch\n par: %q\n seq: %q", label, idx, g.data, want.data)
		}
		if !toksEqual(g.events, want.events) {
			t.Fatalf("%s: record %d events mismatch\n par: %v\n seq: %v", label, idx, g.events, want.events)
		}
	}
	if cfg.Ordered {
		for i, idx := range parOrder {
			if idx != int64(i) {
				t.Fatalf("%s: ordered delivery broken at %d: got index %d", label, i, idx)
			}
		}
	}
	return d
}

// TestParallelStage1Matrix runs the full document matrix through the
// differential harness across geometries, worker counts and delivery modes,
// and also validates the parallel output against the naive reference parser.
func TestParallelStage1Matrix(t *testing.T) {
	geoms := []struct{ chunk, carry int }{
		{1 << 20, 64 << 10}, // defaults: single window
		{64, 16},            // dense borders, spill + jumbo everywhere
		{97, 24},
		{256, 32},
	}
	for _, tc := range recordCorpus() {
		ref := refParse(t, []byte(tc.doc), tc.tag)
		for _, g := range geoms {
			for _, workers := range []int{4, 8} {
				for _, ordered := range []bool{false, true} {
					label := fmt.Sprintf("%s/chunk=%d/w=%d/ordered=%v", tc.name, g.chunk, workers, ordered)
					cfg := RecordConfig{
						Tag: []byte(tc.tag), Workers: workers, Ordered: ordered,
						ChunkSize: g.chunk, CarryCap: g.carry,
					}
					checkS1Differential(t, label, []byte(tc.doc), cfg)
					got, _, err := collectS1([]byte(tc.doc), cfg, true)
					if err != nil {
						t.Fatalf("%s: %v", label, err)
					}
					checkAgainstRef(t, ref, got, label)
				}
			}
		}
	}
}

// TestParallelStage1Adversarial runs the adversarial cases from docs/design.md
// through the differential harness with border-dense geometry.
func TestParallelStage1Adversarial(t *testing.T) {
	long := strings.Repeat("x>", 200) // 400 bytes of '>'-riddled payload
	docs := []struct{ name, tag, doc string }{
		{"stray-cdata-close-in-text", "record",
			`<db><record><a>x ]]> y</a></record><record><b>z</b></record></db>`},
		{"stray-comment-close-in-text", "record",
			`<db><record>a --> b</record><record>c ?> d</record></db>`},
		{"cdata-in-comment", "record",
			`<db><!-- <![CDATA[ </record> --><record><a>1</a></record><record><b>2</b></record></db>`},
		{"comment-in-cdata", "record",
			`<db><record><c><![CDATA[<!-- </record> ]]></c></record><record><d>2</d></record></db>`},
		{"fake-opener-in-own-attr", "record",
			`<db><record a="<!--" b="<![CDATA["><f>v</f></record><record><g>w</g></record></db>`},
		{"fake-opener-in-other-attr", "record",
			`<db><meta q="<!-- <record>"/><record><f>v</f></record><record><g>w</g></record></db>`},
		{"tricky-comment", "record",
			`<db><record><!--->--><f>v</f></record><record><g>w</g></record></db>`},
		{"doctype-internal-subset", "record",
			`<!DOCTYPE db [<!ELEMENT db ANY> <!ENTITY e "<record>fake</record>">]>` +
				`<db><record><f>real</f></record><record><g>2</g></record></db>`},
		{"quote-with-gt-spans-borders", "record",
			`<db><record a="` + long + `"><f>v</f></record><record><g>w</g></record></db>`},
		{"giant-cdata-jumbo", "record",
			`<db><record><c><![CDATA[` + long + `]]></c></record><record><g>w</g></record></db>`},
		{"giant-comment-jumbo", "record",
			`<db><!--` + long + `--><record><g>w</g></record></db>`},
		{"record-straddles-many-borders", "record",
			`<db><record id="big"><f>` + strings.Repeat("ab>", 150) + `</f></record><record><g>w</g></record></db>`},
		{"nested-same-name-straddling", "r",
			`<db><r><r><r>` + strings.Repeat("d>", 100) + `</r></r></r><r>flat</r></db>`},
		{"unknown-bang", "record",
			`<db><record><a>1</a></record><!x><record><b>2</b></record></db>`},
		{"unknown-bang-in-attr", "record",
			`<db><record a="<!x"><f>v</f></record><record><g>w</g></record></db>`},
		{"split-markers", "record",
			`<db><record><![CDATA[q]]><!--c--><?p i?><f/></record><record><g>w</g></record></db>`},
		{"pi-with-angles", "record",
			`<db><?proc <record> "un'bal >> anced?><record><f>v</f></record></db>`},
	}
	for _, tc := range docs {
		for _, geom := range []struct{ chunk, carry int }{{64, 16}, {97, 24}, {1 << 20, 64 << 10}} {
			cfg := RecordConfig{
				Tag: []byte(tc.tag), Workers: 4,
				ChunkSize: geom.chunk, CarryCap: geom.carry,
			}
			label := fmt.Sprintf("%s/chunk=%d", tc.name, geom.chunk)
			checkS1Differential(t, label, []byte(tc.doc), cfg)
		}
	}
}

// TestParallelStage1BorderSweep slides every chunk border across documents
// whose guess-hostile patterns (quotes with '>', construct interiors, split
// markers) then straddle every possible offset. Every placement must be
// byte-identical to the sequential walk.
func TestParallelStage1BorderSweep(t *testing.T) {
	docs := []struct{ tag, doc string }{
		{"record", `<db><record a="x>y>z"><f>` + strings.Repeat("q>", 30) + `</f></record>` +
			`<meta q="</record>"/><record>t</record ><record/><record>z</record></db>`},
		{"r", `<db><r><c><![CDATA[ ]]> </r> >>> ]]></c></r><r a="</r>">q</r><r>t</r></db>`},
		{"r", `<db><!-- <r>no</r> --><r><![CDATA[x>y]]></r><?pi > ?><r>t</r></db>`},
		{"rec", `<db><rec>a<recx></recx>]]&gt;</rec><rec  attr="1>2">b</rec><recs/><rec>c</rec></db>`},
	}
	for di, tc := range docs {
		ref := refParse(t, []byte(tc.doc), tc.tag)
		for cs := 64; cs <= 200; cs++ {
			cfg := RecordConfig{Tag: []byte(tc.tag), Workers: 4, ChunkSize: cs, CarryCap: 16}
			got, _, err := collectS1([]byte(tc.doc), cfg, true)
			if err != nil {
				t.Fatalf("doc %d chunk=%d: %v", di, cs, err)
			}
			checkAgainstRef(t, ref, got, fmt.Sprintf("doc %d chunk=%d", di, cs))
		}
	}
}

// TestParallelStage1IncidentRecovery: single disproved guesses must recover
// (sequential window walk, speculation resumes) without disabling the run.
func TestParallelStage1IncidentRecovery(t *testing.T) {
	// Mostly clean records with an occasional quoted attribute full of '>':
	// borders landing inside those values disprove the guess.
	var sb strings.Builder
	sb.WriteString("<db>")
	for i := range 400 {
		if i%8 == 0 {
			fmt.Fprintf(&sb, `<r a="%s">h%d</r>`, strings.Repeat(">", 24), i)
		} else {
			fmt.Fprintf(&sb, `<r id="%d"><f>%d</f></r>`, i, i)
		}
	}
	sb.WriteString("</db>")
	doc := []byte(sb.String())
	ref := refParse(t, doc, "r")

	var total s1Snap
	for cs := 64; cs <= 96; cs++ {
		cfg := RecordConfig{Tag: []byte("r"), Workers: 4, ChunkSize: cs, CarryCap: 32}
		pre := snapS1()
		got, _, err := collectS1(doc, cfg, true)
		d := snapS1().delta(pre)
		if err != nil {
			t.Fatalf("chunk=%d: %v", cs, err)
		}
		checkAgainstRef(t, ref, got, fmt.Sprintf("chunk=%d", cs))
		total.hits += d.hits
		total.misses += d.misses
		total.disables += d.disables
	}
	if total.misses == 0 {
		t.Fatal("sweep never disproved a guess; incident recovery untested")
	}
	if total.hits == 0 {
		t.Fatal("sweep never proved a guess; speculation untested")
	}
	if total.hits < total.misses {
		t.Fatalf("expected mostly-clean corpus to mostly hit: hits=%d misses=%d", total.hits, total.misses)
	}
}

// TestParallelStage1AdaptiveDisable: an input hostile on every border must
// disable speculation for the run and still produce exact output.
func TestParallelStage1AdaptiveDisable(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("<db>")
	for i := range 300 {
		fmt.Fprintf(&sb, `<r a="%s">v%d</r>`, strings.Repeat(">", 60), i)
	}
	sb.WriteString("</db>")
	doc := []byte(sb.String())
	ref := refParse(t, doc, "r")

	cfg := RecordConfig{Tag: []byte("r"), Workers: 4, ChunkSize: 64, CarryCap: 64}
	pre := snapS1()
	got, _, err := collectS1(doc, cfg, true)
	d := snapS1().delta(pre)
	if err != nil {
		t.Fatal(err)
	}
	checkAgainstRef(t, ref, got, "hostile")
	if d.misses < s1IncidentLimit || d.disables < 1 {
		t.Fatalf("expected adaptive disable after %d misses, counters %+v", s1IncidentLimit, d)
	}
}

// TestParallelStage1Errors: every error class must surface with the exact
// sequential message and offset under parallel stage 1.
func TestParallelStage1Errors(t *testing.T) {
	cases := []struct {
		name, doc, want string
		cfg             RecordConfig
	}{
		{"unterminated record", `<db><record><a>1</a>`, "unterminated record",
			RecordConfig{ChunkSize: 64, CarryCap: 16}},
		{"eof in markup", `<db><record><a`, "unexpected EOF inside markup",
			RecordConfig{ChunkSize: 64, CarryCap: 16}},
		{"eof in markup big", `<db><record><a>1</a></record><record ` + strings.Repeat("x", 100), "unexpected EOF inside markup",
			RecordConfig{ChunkSize: 64, CarryCap: 16}},
		{"max record size", `<db><record>` + strings.Repeat("x", 300) + `</record></db>`, "record exceeds MaxRecordSize",
			RecordConfig{ChunkSize: 64, CarryCap: 16, MaxRecordSize: 128}},
		{"max construct size", `<db><record><!--` + strings.Repeat("y", 300) + `--></record></db>`, "markup construct exceeds MaxRecordSize",
			RecordConfig{ChunkSize: 64, CarryCap: 16, MaxRecordSize: 128}},
		{"bad markup", `<db><record><a>1</a></record><!x></db>`, "unknown markup",
			RecordConfig{ChunkSize: 64, CarryCap: 16}},
		{"bad markup one window", `<db><record><!x></record></db>`, "unknown markup",
			RecordConfig{}},
		{"utf16 bom", "\xFE\xFF\x00<", "UTF-16",
			RecordConfig{ChunkSize: 64, CarryCap: 16}},
	}
	for _, c := range cases {
		for _, workers := range []int{4, 8} {
			c.cfg.Tag = []byte("record")
			c.cfg.Workers = workers
			label := fmt.Sprintf("%s/w=%d", c.name, workers)
			// Differential: parallel error must equal sequential error.
			checkS1Differential(t, label, []byte(c.doc), c.cfg)
			// And it must be the intended error.
			_, _, err := collectS1([]byte(c.doc), c.cfg, true)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("%s: got %v, want containing %q", label, err, c.want)
			}
		}
	}
}

// TestParallelStage1ActiveByDefault guards the activation wiring: a
// Workers >= 4 run over a multi-chunk document must actually consume
// speculation without any test knob.
func TestParallelStage1ActiveByDefault(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("<db>")
	for i := range 3000 {
		fmt.Fprintf(&sb, `<record id="%d"><f>%d</f></record>`, i, i)
	}
	sb.WriteString("</db>")
	doc := []byte(sb.String())

	pre := snapS1()
	got, _, _, err := collectParallel(doc, RecordConfig{
		Tag: []byte("record"), Workers: 4, ChunkSize: 4096, CarryCap: 512,
	})
	d := snapS1().delta(pre)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3000 {
		t.Fatalf("got %d records, want 3000", len(got))
	}
	if d.hits == 0 {
		t.Fatalf("Workers=4 multi-chunk run consumed no speculation: %+v", d)
	}

	// Workers below the threshold must not touch the parallel pipeline.
	pre = snapS1()
	if _, _, _, err := collectParallel(doc, RecordConfig{
		Tag: []byte("record"), Workers: 2, ChunkSize: 4096, CarryCap: 512,
	}); err != nil {
		t.Fatal(err)
	}
	if d := snapS1().delta(pre); d.prewalks != 0 {
		t.Fatalf("Workers=2 run pre-walked: %+v", d)
	}
}

// TestParallelStage1ErraticReader: short and zero-length reads through the
// forced-parallel pipeline.
func TestParallelStage1ErraticReader(t *testing.T) {
	tc := recordCorpus()[1] // "many"
	ref := refParse(t, []byte(tc.doc), tc.tag)
	s1ForceParallel.Store(true)
	defer s1ForceParallel.Store(false)
	var mu sync.Mutex
	got := map[int64]refRec{}
	err := ParseRecords(&erraticReader{data: []byte(tc.doc)},
		RecordConfig{Tag: []byte(tc.tag), Workers: 4, ChunkSize: 64, CarryCap: 16},
		func(w int, rec Record) error {
			var evs []tok
			for _, e := range rec.Events {
				evs = append(evs, tok{e.Kind, string(rec.Bytes(e))})
			}
			mu.Lock()
			got[rec.Index] = refRec{data: string(rec.Data), events: evs}
			mu.Unlock()
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	checkAgainstRef(t, ref, got, "erratic")
}

// failReader yields data then a non-EOF error.
type failReader struct {
	data []byte
	pos  int
	err  error
}

func (r *failReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, r.err
	}
	n := min(7, len(p), len(r.data)-r.pos)
	copy(p, r.data[r.pos:r.pos+n])
	r.pos += n
	return n, nil
}

// TestParallelStage1Shutdown exercises every early-exit path (handler error,
// reader error, syntax error) with parallel stage 1 forced and asserts all
// goroutines are joined afterwards.
func TestParallelStage1Shutdown(t *testing.T) {
	s1ForceParallel.Store(true)
	defer s1ForceParallel.Store(false)
	var sb strings.Builder
	sb.WriteString("<db>")
	for i := range 500 {
		fmt.Fprintf(&sb, `<record id="%d"><f>%d</f></record>`, i, i)
	}
	sb.WriteString("</db>")
	doc := []byte(sb.String())
	sentinel := errors.New("boom")
	before := runtime.NumGoroutine()

	for _, workers := range []int{4, 8} {
		for _, ordered := range []bool{false, true} {
			cfg := RecordConfig{Tag: []byte("record"), Workers: workers, Ordered: ordered,
				ChunkSize: 64, CarryCap: 16}
			// Handler error mid-stream.
			calls := 0
			var mu sync.Mutex
			err := ParseRecords(bytes.NewReader(doc), cfg, func(w int, rec Record) error {
				mu.Lock()
				calls++
				n := calls
				mu.Unlock()
				if n >= 5 {
					return sentinel
				}
				return nil
			})
			if !errors.Is(err, sentinel) {
				t.Fatalf("w=%d ordered=%v handler error: got %v", workers, ordered, err)
			}
			// Reader error mid-stream.
			readerErr := errors.New("pipe burst")
			err = ParseRecords(&failReader{data: doc[:1000], err: readerErr}, cfg,
				func(w int, rec Record) error { return nil })
			if !errors.Is(err, readerErr) {
				t.Fatalf("w=%d ordered=%v reader error: got %v", workers, ordered, err)
			}
			// Syntax error mid-stream.
			bad := append(append([]byte{}, doc[:2000]...), []byte(`<record><!bad></record>`)...)
			err = ParseRecords(bytes.NewReader(bad), cfg, func(w int, rec Record) error { return nil })
			if err == nil || !strings.Contains(err.Error(), "unknown markup") {
				t.Fatalf("w=%d ordered=%v syntax error: got %v", workers, ordered, err)
			}
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("goroutine leak: %d before, %d after", before, n)
	}
}

// TestParallelStage1OrderedSequence: ordered delivery with parallel stage 1
// must remain a strict 0..n-1 index sequence from worker 0.
func TestParallelStage1OrderedSequence(t *testing.T) {
	s1ForceParallel.Store(true)
	defer s1ForceParallel.Store(false)
	var sb strings.Builder
	sb.WriteString("<db>")
	for i := range 2000 {
		fmt.Fprintf(&sb, `<record id="%d"><f>%d</f></record>`, i, i)
	}
	sb.WriteString("</db>")
	var next int64
	err := ParseRecords(strings.NewReader(sb.String()),
		RecordConfig{Tag: []byte("record"), Workers: 8, Ordered: true, ChunkSize: 512, CarryCap: 64},
		func(w int, rec Record) error {
			if w != 0 {
				return fmt.Errorf("ordered mode called handler with worker %d", w)
			}
			if rec.Index != next {
				return fmt.Errorf("out of order: got %d, want %d", rec.Index, next)
			}
			next++
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if next != 2000 {
		t.Fatalf("delivered %d records, want 2000", next)
	}
}

// TestParallelStage1AutoDetect: the Tag == nil auto-detection seam feeds the
// buffered first window back through the parallel pipeline.
func TestParallelStage1AutoDetect(t *testing.T) {
	s1ForceParallel.Store(true)
	defer s1ForceParallel.Store(false)
	var sb strings.Builder
	sb.WriteString("<db>")
	for i := range 300 {
		fmt.Fprintf(&sb, `<record id="%d"><f>%d</f></record>`, i, i)
	}
	sb.WriteString("</db>")
	var detected []byte
	var n int64
	var mu sync.Mutex
	err := ParseRecords(strings.NewReader(sb.String()),
		RecordConfig{Workers: 4, ChunkSize: 256, CarryCap: 32,
			OnDetect: func(tag []byte) { detected = append([]byte(nil), tag...) }},
		func(w int, rec Record) error {
			mu.Lock()
			n++
			mu.Unlock()
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if string(detected) != "record" || n != 300 {
		t.Fatalf("detected %q, %d records", detected, n)
	}
}

// TestParallelStage1SoundnessAttack attacks the docs/design.md soundness
// argument directly: each case targets a listed risk (opaque construct
// interiors full of fake records, orphan closers, nested comment lookalikes,
// quote/marker bytes straddling chunk borders at every alignment, index
// ordering under record floods, records straddling 3+ chunks). Every case must
// be byte-identical across forced-parallel stage 1 (workers 4 and 8, ordered
// and not), forced-sequential stage 1, and the naive reference parser.
func TestParallelStage1SoundnessAttack(t *testing.T) {
	type geo struct{ chunk, carry int }
	verify := func(t *testing.T, label, tag string, doc []byte, geoms []geo) {
		t.Helper()
		ref := refParse(t, doc, tag)
		for _, g := range geoms {
			for _, w := range []int{4, 8} {
				for _, ordered := range []bool{false, true} {
					cfg := RecordConfig{Tag: []byte(tag), Workers: w, Ordered: ordered,
						ChunkSize: g.chunk, CarryCap: g.carry}
					l := fmt.Sprintf("%s/chunk=%d/w=%d/ordered=%v", label, g.chunk, w, ordered)
					checkS1Differential(t, l, doc, cfg)
					got, _, err := collectS1(doc, cfg, true)
					if err != nil {
						t.Fatalf("%s: %v", l, err)
					}
					checkAgainstRef(t, ref, got, l)
				}
			}
		}
	}
	dense := []geo{{64, 16}, {97, 24}, {256, 64}}
	all := []geo{{64, 16}, {97, 24}, {256, 64}, {1 << 20, 64 << 10}}

	t.Run("giant-cdata-fake-records", func(t *testing.T) {
		// One giant CDATA interior packed with thousands of syntactically
		// perfect fake records: every border chunk's pre-walk sees plausible
		// structure, yet none of it may surface. At dense geometry the
		// construct also exercises the jumbo path.
		fakes := strings.Repeat("<record><f>fake</f></record>", 3000) // ~84KB
		top := `<db><![CDATA[` + fakes + `]]><record><g>ok</g></record></db>`
		wrapped := `<db><record><c><![CDATA[` + fakes + `]]></c></record><record><g>ok</g></record></db>`
		verify(t, "top-level", "record", []byte(top), all)
		verify(t, "in-record", "record", []byte(wrapped), all)
	})

	t.Run("orphan-closers", func(t *testing.T) {
		doc := `<db>]]> ]]>]]><record>a]]>b</record>--> ?><record c="]]>" d="-->">x</record>]]></db>`
		verify(t, "orphans", "record", []byte(doc), all)
	})

	t.Run("nested-comment-lookalikes", func(t *testing.T) {
		doc := `<db><!-- <!-- <record>f1</record> --><record>r1</record>--> <record>r2</record>` +
			`<!--->--><record><!-- <!--inner--></record><!----><record>r4</record></db>`
		verify(t, "lookalikes", "record", []byte(doc), all)
	})

	t.Run("quote-first-chunk-byte", func(t *testing.T) {
		// Deterministic construction: at ChunkSize 64 the opening quote of the
		// attribute value is the FIRST byte of chunk 1 (absolute offset 64),
		// so chunk 1's H-guess lands on a '>' inside the quoted value and must
		// be disproved by the head-fragment walk.
		doc := `<db><record ` + strings.Repeat("a", 51) + `=">>v>>">x</record><record>y</record></db>`
		if doc[64] != '"' {
			t.Fatalf("construction drifted: byte 64 is %q", doc[64])
		}
		verify(t, "quote-at-border", "record", []byte(doc), []geo{{64, 16}, {64, 64}})
	})

	t.Run("marker-split-sweep", func(t *testing.T) {
		// Fixed ChunkSize 64 with a growing pad slides every byte of the
		// pattern across every border alignment: each marker ("<!", "<!--",
		// "<![CDATA[", "]]>", "-->", "<?", "?>", "/>") and each quote char is
		// split at every possible border offset, including as the first byte
		// of a chunk.
		pattern := `<record a="1>2>3" b='<!--' c="]]>"><f><![CDATA[x]]y]]></f><!--c--><?pi q?></record><record>t</record ><record/>`
		for pad := 0; pad < 64; pad++ {
			doc := []byte(`<db><m>` + strings.Repeat("p", pad) + `</m>` + pattern + `</db>`)
			ref := refParse(t, doc, "record")
			for _, cfg := range []RecordConfig{
				{Tag: []byte("record"), Workers: 4, ChunkSize: 64, CarryCap: 16},
				{Tag: []byte("record"), Workers: 8, Ordered: true, ChunkSize: 64, CarryCap: 16},
			} {
				l := fmt.Sprintf("pad=%d/w=%d/ordered=%v", pad, cfg.Workers, cfg.Ordered)
				checkS1Differential(t, l, doc, cfg)
				got, _, err := collectS1(doc, cfg, true)
				if err != nil {
					t.Fatalf("%s: %v", l, err)
				}
				checkAgainstRef(t, ref, got, l)
			}
		}
	})

	t.Run("many-tiny-records", func(t *testing.T) {
		// 20000 ten-byte records stress dispatch-order and index bookkeeping:
		// any replay slip (dropped stray, double-counted span) shifts every
		// subsequent index and fails the ordered-delivery check.
		doc := `<db>` + strings.Repeat(`<r i="7"/>`, 20000) + `</db>`
		verify(t, "tiny", "r", []byte(doc), []geo{{64, 16}, {1024, 128}})
	})

	t.Run("record-straddles-3plus-chunks", func(t *testing.T) {
		// At ChunkSize 64: a record body spanning >10 borders (evOpen/evStray
		// chain plus per-window bankSpill), a quoted value spanning ~4 borders
		// (consecutive disproved guesses), then clean records to prove
		// recovery resumes exactly.
		doc := `<db><record>` + strings.Repeat("<f>0123456789</f>", 40) +
			`</record><record a="` + strings.Repeat(">", 200) + `">tail</record><record>fin</record></db>`
		verify(t, "straddler", "record", []byte(doc), dense)
	})

	t.Run("straddler-maxrecordsize-parity", func(t *testing.T) {
		// MaxRecordSize violations discovered while completing a straddler
		// (spill path) and while banking spill must carry sequential-identical
		// offsets; differential-only (the reference has no size limit).
		for _, n := range []int{120, 129, 200, 300} {
			doc := []byte(`<db><record>` + strings.Repeat("z", n) + `</record><record>ok</record></db>`)
			for _, w := range []int{4, 8} {
				cfg := RecordConfig{Tag: []byte("record"), Workers: w,
					ChunkSize: 64, CarryCap: 16, MaxRecordSize: 128}
				checkS1Differential(t, fmt.Sprintf("n=%d/w=%d", n, w), doc, cfg)
			}
		}
	})
}

// FuzzParallelStage1 is the differential fuzz target: arbitrary bytes through
// sequential and parallel stage 1 must yield identical record sets and
// identical errors, at both border-dense and default geometry.
func FuzzParallelStage1(f *testing.F) {
	for _, tc := range recordCorpus() {
		f.Add([]byte(tc.doc))
	}
	f.Add([]byte(`<db><record a="<!--">x</record><record>]]></record></db>`))
	f.Add([]byte(`<db><!-- <![CDATA[ </record> --><record><![CDATA[>>>]]></record></db>`))
	f.Add([]byte(`<db><record><!--->--><f attr="1>2"/></record><record/></db>`))
	f.Add([]byte(`<!DOCTYPE db [<!ENTITY e "<record>f</record>">]><db><record>r</record></db>`))
	f.Add([]byte("<db><record" + strings.Repeat(" ", 100) + "a='" + strings.Repeat(">", 100) + "'>v</record></db>"))

	run := func(data []byte, parallel bool) (map[int64]string, string) {
		if parallel {
			s1ForceParallel.Store(true)
			defer s1ForceParallel.Store(false)
		} else {
			s1ForceSeq.Store(true)
			defer s1ForceSeq.Store(false)
		}
		var mu sync.Mutex
		recs := map[int64]string{}
		err := ParseRecords(bytes.NewReader(data),
			RecordConfig{Tag: []byte("record"), Workers: 4, ChunkSize: 64, CarryCap: 16},
			func(w int, rec Record) error {
				mu.Lock()
				recs[rec.Index] = string(rec.Data)
				mu.Unlock()
				return nil
			})
		if err != nil {
			return nil, err.Error()
		}
		return recs, ""
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		seqRecs, seqErr := run(data, false)
		parRecs, parErr := run(data, true)
		// Force-generic leg: the same parse with the scankern assembly kernel
		// replaced by the portable builder must be byte-identical too (parity
		// gate for the per-arch maskByte64 in full context).
		scankernForceGeneric.Store(true)
		genRecs, genErr := run(data, true)
		scankernForceGeneric.Store(false)
		for _, leg := range []struct {
			name string
			recs map[int64]string
			err  string
		}{{"par", parRecs, parErr}, {"par+generic-kern", genRecs, genErr}} {
			if seqErr != leg.err {
				t.Fatalf("error mismatch (%s):\n seq: %q\n %s: %q\ninput: %q",
					leg.name, seqErr, leg.name, leg.err, data)
			}
			if seqErr != "" {
				continue
			}
			if len(seqRecs) != len(leg.recs) {
				t.Fatalf("record count mismatch (%s): seq=%d got=%d\ninput: %q",
					leg.name, len(seqRecs), len(leg.recs), data)
			}
			for idx, want := range seqRecs {
				if got, ok := leg.recs[idx]; !ok || got != want {
					t.Fatalf("record %d mismatch (%s):\n seq: %q\n got: %q\ninput: %q",
						idx, leg.name, want, got, data)
				}
			}
		}
	})
}

// BenchmarkParallelStage1 is the back-to-back stage-1 comparison: identical
// runs with stage 1 sequential vs parallel, over the default corpus and (via
// FASTSAX_CLEAN_XML) a CDATA/comment-free one. Same-process ratios are the
// meaningful output; absolute numbers depend on machine load.
func BenchmarkParallelStage1(b *testing.B) {
	type corpusEntry struct {
		name string
		data []byte
	}
	var corpora []corpusEntry
	if data, err := os.ReadFile(benchXML); err == nil {
		corpora = append(corpora, corpusEntry{"default", data})
	}
	if path := os.Getenv("FASTSAX_CLEAN_XML"); path != "" {
		if data, err := os.ReadFile(path); err == nil {
			corpora = append(corpora, corpusEntry{"clean", data})
		}
	}
	if len(corpora) == 0 {
		b.Skipf("no benchmark corpora available (%s)", benchXMLHint)
	}
	stats := make([]parStat, maxParWorkers)
	handle := trivialRecordHandler(stats)
	for _, corpus := range corpora {
		for _, mode := range []struct {
			name     string
			parallel bool
		}{{"stage1=seq", false}, {"stage1=par", true}} {
			for _, w := range []int{1, 2, 4, 8} {
				b.Run(fmt.Sprintf("%s/%s/workers=%d", corpus.name, mode.name, w), func(b *testing.B) {
					if mode.parallel {
						s1ForceParallel.Store(true)
						defer s1ForceParallel.Store(false)
					} else {
						s1ForceSeq.Store(true)
						defer s1ForceSeq.Store(false)
					}
					cfg := RecordConfig{Tag: []byte("record"), Workers: w}
					b.SetBytes(int64(len(corpus.data)))
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						clearParStats(stats)
						if err := ParseRecords(bytes.NewReader(corpus.data), cfg, handle); err != nil {
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
