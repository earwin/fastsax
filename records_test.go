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

// refRec is one record as seen by the naive reference parser.
type refRec struct {
	data   string
	events []tok
}

// refParse is the reference: one scanChunk over the whole document, a simple
// depth-counting walk for the tag, one scanChunk per record for its events.
func refParse(t *testing.T, doc []byte, tag string) []refRec {
	t.Helper()
	tape, cut, err := scanChunk(doc, nil)
	if err != nil || cut != len(doc) {
		t.Fatalf("reference scan: cut=%d/%d err=%v", cut, len(doc), err)
	}
	tb := []byte(tag)
	var out []refRec
	depth, start := 0, 0
	add := func(b []byte) {
		rt, rcut, rerr := scanChunk(b, nil)
		if rerr != nil || rcut != len(b) {
			t.Fatalf("reference record scan: %v", rerr)
		}
		var evs []tok
		for _, e := range rt {
			evs = append(evs, tok{e.Kind, string(b[e.Start:e.End])})
		}
		out = append(out, refRec{data: string(b), events: evs})
	}
	for _, e := range tape {
		b := doc[e.Start:e.End]
		switch e.Kind {
		case KindStart:
			if matchOpenTag(b, tb) {
				if depth == 0 {
					start = int(e.Start)
				}
				depth++
			}
		case KindSelfClose:
			if depth == 0 && matchOpenTag(b, tb) {
				add(b)
			}
		case KindEnd:
			if depth > 0 && matchCloseTag(b, tb) {
				depth--
				if depth == 0 {
					add(doc[start:e.End])
				}
			}
		}
	}
	return out
}

// collectParallel runs ParseRecords and returns the records keyed by Index,
// with the raw handle-call order of indices as a second value.
func collectParallel(doc []byte, cfg RecordConfig) (map[int64]refRec, []int64, []int, error) {
	var mu sync.Mutex
	got := map[int64]refRec{}
	var order []int64
	var workers []int
	err := ParseRecords(bytes.NewReader(doc), cfg, func(w int, rec Record) error {
		var evs []tok
		for _, e := range rec.Events {
			if int(e.End) > len(rec.Data) || e.Start > e.End {
				return fmt.Errorf("event out of bounds: %+v in %d-byte record", e, len(rec.Data))
			}
			evs = append(evs, tok{e.Kind, string(rec.Bytes(e))})
		}
		r := refRec{data: string(rec.Data), events: evs}
		mu.Lock()
		if _, dup := got[rec.Index]; dup {
			mu.Unlock()
			return fmt.Errorf("duplicate record index %d", rec.Index)
		}
		got[rec.Index] = r
		order = append(order, rec.Index)
		workers = append(workers, w)
		mu.Unlock()
		return nil
	})
	return got, order, workers, err
}

func checkAgainstRef(t *testing.T, ref []refRec, got map[int64]refRec, label string) {
	t.Helper()
	if len(got) != len(ref) {
		t.Fatalf("%s: got %d records, want %d", label, len(got), len(ref))
	}
	for i, want := range ref {
		g, ok := got[int64(i)]
		if !ok {
			t.Fatalf("%s: missing record index %d", label, i)
		}
		if g.data != want.data {
			t.Fatalf("%s: record %d data mismatch\n got: %q\nwant: %q", label, i, g.data, want.data)
		}
		if !toksEqual(g.events, want.events) {
			t.Fatalf("%s: record %d event mismatch\n got: %v\nwant: %v", label, i, g.events, want.events)
		}
	}
}

// recordCorpus returns documents plus the record tag to extract.
func recordCorpus() []struct{ name, tag, doc string } {
	long := strings.Repeat("0123456789abcdef", 30) // 480 bytes: > tiny chunk => spill
	var many strings.Builder
	many.WriteString("<?xml version=\"1.0\"?>\n<db created=\"now\">\n")
	for i := range 137 {
		switch i % 5 {
		case 0:
			fmt.Fprintf(&many, "<record id=\"%d\"><name>n&amp;%d</name><v>%d</v></record>\n", i, i, i*7)
		case 1:
			fmt.Fprintf(&many, "  <record id=\"%d\"/>\n", i)
		case 2:
			fmt.Fprintf(&many, "<record id=\"%d\" note=\"a &gt; b\"><blob><![CDATA[fake </record> close %d]]></blob></record>", i, i)
		case 3:
			fmt.Fprintf(&many, "<record id=\"%d\"><!-- fake </record> --><record><inner>%d</inner></record></record>\n", i, i)
		case 4:
			fmt.Fprintf(&many, "<recordx skip=\"%d\">not a record</recordx><record attr=\"</record>\" x=\"1>2\">t%d</record>\n", i, i)
		}
	}
	many.WriteString("</db>\n")

	return []struct{ name, tag, doc string }{
		{"sample", "record", sampleDoc},
		{"many", "record", many.String()},
		{"nested", "r", `<top><r><r><r>deep</r></r></r><r>flat</r></top>`},
		{"selfclose", "r", `<top><r/><r a="1"/><r></r><r  /></top>`},
		{"bigrecord", "record", `<db><record id="big"><data>` + long + `</data></record><record id="s"/></db>`},
		{"bigcdata", "record", `<db><record><c><![CDATA[` + long + `]]></c></record><record><x>1</x></record></db>`},
		{"jumbocomment", "record", `<db><!--` + long + `--><record><a>1</a></record></db>`},
		{"prefix", "rec", `<db><record>no</record><rec>yes</rec><recx/><rec  attr="v">also</rec></db>`},
		{"norecords", "record", `<db><other>1</other><more/></db>`},
		{"onlytext", "record", `just text, no markup at all... almost: <db/>`},
	}
}

func TestParseRecordsMatrix(t *testing.T) {
	geoms := []struct{ chunk, carry int }{
		{1 << 20, 64 << 10}, // defaults
		{64, 16},            // forces spill + jumbo everywhere
		{97, 24},
		{256, 32},
	}
	for _, tc := range recordCorpus() {
		ref := refParse(t, []byte(tc.doc), tc.tag)
		for _, g := range geoms {
			for _, workers := range []int{1, 4} {
				for _, ordered := range []bool{false, true} {
					label := fmt.Sprintf("%s/chunk=%d/carry=%d/w=%d/ordered=%v", tc.name, g.chunk, g.carry, workers, ordered)
					cfg := RecordConfig{
						Tag: []byte(tc.tag), Workers: workers, Ordered: ordered,
						ChunkSize: g.chunk, CarryCap: g.carry,
					}
					got, order, wids, err := collectParallel([]byte(tc.doc), cfg)
					if err != nil {
						t.Fatalf("%s: %v", label, err)
					}
					checkAgainstRef(t, ref, got, label)
					if ordered || workers == 1 {
						for i, idx := range order {
							if idx != int64(i) {
								t.Fatalf("%s: delivery order broken at %d: got index %d", label, i, idx)
							}
						}
					}
					for _, w := range wids {
						max := workers
						if ordered {
							max = 1 // ordered delivery always reports worker 0
						}
						if w < 0 || w >= max {
							t.Fatalf("%s: worker id %d out of range [0,%d)", label, w, max)
						}
					}
				}
			}
		}
	}
}

func TestParseRecordsErraticReader(t *testing.T) {
	tc := recordCorpus()[1] // "many"
	ref := refParse(t, []byte(tc.doc), tc.tag)
	cfg := RecordConfig{Tag: []byte(tc.tag), Workers: 3, ChunkSize: 64, CarryCap: 16}
	var mu sync.Mutex
	got := map[int64]refRec{}
	err := ParseRecords(&erraticReader{data: []byte(tc.doc)}, cfg, func(w int, rec Record) error {
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

func TestParseRecordsOrderedSequence(t *testing.T) {
	tc := recordCorpus()[1] // "many": enough records to shuffle workers
	cfg := RecordConfig{Tag: []byte(tc.tag), Workers: 4, Ordered: true, ChunkSize: 128, CarryCap: 32}
	var next int64
	err := ParseRecords(bytes.NewReader([]byte(tc.doc)), cfg, func(w int, rec Record) error {
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
	if next == 0 {
		t.Fatal("no records delivered")
	}
}

func TestParseRecordsHandlerError(t *testing.T) {
	sentinel := errors.New("boom")
	tc := recordCorpus()[1]
	before := runtime.NumGoroutine()
	for _, ordered := range []bool{false, true} {
		for _, workers := range []int{1, 4} {
			calls := 0
			var mu sync.Mutex
			err := ParseRecords(bytes.NewReader([]byte(tc.doc)),
				RecordConfig{Tag: []byte(tc.tag), Workers: workers, Ordered: ordered, ChunkSize: 64, CarryCap: 16},
				func(w int, rec Record) error {
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
				t.Fatalf("workers=%d ordered=%v: got %v, want sentinel", workers, ordered, err)
			}
		}
	}
	// All goroutines must be joined after ParseRecords returns.
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("goroutine leak: %d before, %d after", before, n)
	}
}

func TestParseRecordsErrors(t *testing.T) {
	handle := func(w int, rec Record) error { return nil }

	// nil Tag is rejected until auto-detection exists.
	if err := ParseRecords(strings.NewReader("<a/>"), RecordConfig{}, handle); err == nil ||
		!strings.Contains(err.Error(), "Tag is required") {
		t.Fatalf("nil tag: %v", err)
	}
	// nil handler.
	if err := ParseRecords(strings.NewReader("<a/>"), RecordConfig{Tag: []byte("a")}, nil); err == nil {
		t.Fatal("nil handler accepted")
	}

	cases := []struct {
		name, doc, want string
		cfg             RecordConfig
	}{
		{"unterminated record", `<db><record><a>1</a>`, "unterminated record",
			RecordConfig{Tag: []byte("record")}},
		{"eof in markup", `<db><record><a`, "unexpected EOF inside markup",
			RecordConfig{Tag: []byte("record")}},
		{"eof in markup tiny", `<db><record><a`, "unexpected EOF inside markup",
			RecordConfig{Tag: []byte("record"), ChunkSize: 64, CarryCap: 16}},
		{"max record size", `<db><record>` + strings.Repeat("x", 300) + `</record></db>`, "MaxRecordSize",
			RecordConfig{Tag: []byte("record"), ChunkSize: 64, CarryCap: 16, MaxRecordSize: 128}},
		{"max record inline", `<db><record>` + strings.Repeat("x", 300) + `</record></db>`, "MaxRecordSize",
			RecordConfig{Tag: []byte("record"), MaxRecordSize: 128}},
		{"bad markup", `<db><record><!x></record></db>`, "unknown markup",
			RecordConfig{Tag: []byte("record")}},
		{"utf16 bom", "\xFE\xFF\x00<", "UTF-16",
			RecordConfig{Tag: []byte("record")}},
	}
	for _, c := range cases {
		for _, workers := range []int{1, 4} {
			c.cfg.Workers = workers
			err := ParseRecords(strings.NewReader(c.doc), c.cfg, handle)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("%s (workers=%d): got %v, want containing %q", c.name, workers, err, c.want)
			}
		}
	}
}

// TestParseRecordsSpillBoundaries sweeps chunk sizes so a record straddles
// every possible window offset at least once.
func TestParseRecordsSpillBoundaries(t *testing.T) {
	doc := `<db><record id="1"><f>` + strings.Repeat("ab", 40) + `</f></record>` +
		`<record id="2"><g><![CDATA[x]]></g></record><record id="3"/></db>`
	ref := refParse(t, []byte(doc), "record")
	for cs := 64; cs <= 200; cs++ {
		got, _, _, err := collectParallel([]byte(doc),
			RecordConfig{Tag: []byte("record"), Workers: 2, ChunkSize: cs, CarryCap: 16})
		if err != nil {
			t.Fatalf("chunk=%d: %v", cs, err)
		}
		checkAgainstRef(t, ref, got, fmt.Sprintf("chunk=%d", cs))
	}
}

func BenchmarkParseRecords(b *testing.B) {
	data, err := os.ReadFile(benchXML)
	if err != nil {
		b.Skipf("bench corpus unavailable: %v (%s)", err, benchXMLHint)
	}
	counts := make([]struct {
		n int64
		_ [56]byte // avoid false sharing between worker counters
	}, 64)
	handle := func(w int, rec Record) error {
		counts[w].n += int64(len(rec.Events))
		return nil
	}
	for _, w := range []int{1, 2, 4, 8, runtime.GOMAXPROCS(0)} {
		b.Run(fmt.Sprintf("workers=%d", w), func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			for b.Loop() {
				if err := ParseRecords(bytes.NewReader(data),
					RecordConfig{Tag: []byte("record"), Workers: w}, handle); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.Run("ordered", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for b.Loop() {
			if err := ParseRecords(bytes.NewReader(data),
				RecordConfig{Tag: []byte("record"), Ordered: true}, handle); err != nil {
				b.Fatal(err)
			}
		}
	})
}
