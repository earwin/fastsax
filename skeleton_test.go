package fastsax

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// skelSnap is a snapshot of the process-wide fast-skeleton counters. Package
// tests run sequentially, so deltas around a ParseRecords call are exact.
type skelSnap struct{ fast, full, cons, walks, fakes, disables int64 }

func snapSkel() skelSnap {
	return skelSnap{
		fast:     skelStats.fastWindows.Load(),
		full:     skelStats.fullWindows.Load(),
		cons:     skelStats.constructs.Load(),
		walks:    skelStats.gapWalks.Load(),
		fakes:    skelStats.fakeHits.Load(),
		disables: skelStats.disables.Load(),
	}
}

func (s skelSnap) delta(prev skelSnap) skelSnap {
	return skelSnap{s.fast - prev.fast, s.full - prev.full, s.cons - prev.cons,
		s.walks - prev.walks, s.fakes - prev.fakes, s.disables - prev.disables}
}

// checkFastVsFull parses doc twice — fast path allowed, then full walker
// forced via the test knob — checks both against the naive reference and
// returns the fast run's counter delta.
func checkFastVsFull(t *testing.T, label, doc, tag string, cfg RecordConfig) skelSnap {
	t.Helper()
	cfg.Tag = []byte(tag)
	ref := refParse(t, []byte(doc), tag)

	pre := snapSkel()
	got, _, _, err := collectParallel([]byte(doc), cfg)
	d := snapSkel().delta(pre)
	if err != nil {
		t.Fatalf("%s: fast run: %v", label, err)
	}
	checkAgainstRef(t, ref, got, label+"/fast")

	skelForceFull.Store(true)
	defer skelForceFull.Store(false)
	full, _, _, err := collectParallel([]byte(doc), cfg)
	if err != nil {
		t.Fatalf("%s: forced-full run: %v", label, err)
	}
	checkAgainstRef(t, ref, full, label+"/full")
	return d
}

// TestFastSkeletonAdversarial feeds documents where naive pattern hopping
// would find fake record boundaries and asserts both correct output and that
// the intended defense actually fired (atomic construct consumption for
// "<!"/"<?" constructs, gap resolution for quoted attributes). Fakes inside
// construct interiors must never be seen at all: those documents stay on the
// fast path with zero fake hits and their constructs counted.
func TestFastSkeletonAdversarial(t *testing.T) {
	docs := []struct {
		name, tag, doc               string
		wantCons, wantFake, wantWalk bool
	}{
		{"fake-close-in-cdata", "record",
			`<db><record><c><![CDATA[</record> fake]]></c></record><record><x>1</x></record></db>`,
			true, false, false},
		{"fake-close-in-comment", "record",
			`<db><!-- </record> --><record><x>1</x></record><record><y>2</y></record></db>`,
			true, false, false},
		{"fake-close-in-pi", "record",
			`<db><?p </record> ?><record><f>v</f></record></db>`,
			true, false, false},
		// Fake close in the record's own attributes: consumed while scanning
		// the start tag's quoted values, no fallback needed at all.
		{"fake-close-in-own-attr", "record",
			`<db><record attr="</record>" x="1>2"><f>v</f></record><record><g>w</g></record></db>`,
			false, false, false},
		// Fake close/open in another element's attributes: exposed by the
		// quoted-gap construct walk.
		{"fake-close-in-other-attr", "record",
			`<db><meta q="</record>"/><record><f>v</f></record><record><g>w</g></record></db>`,
			false, true, true},
		{"fake-open-in-other-attr", "record",
			`<db><meta q="<record>"/><record><f>v</f></record><record><g>w</g></record></db>`,
			false, true, true},
		// Lenient raw '<' inside a tag: the quote-free last-'<'/'>' proof must
		// still reject the swallowed pattern.
		{"raw-lt-in-tag", "record",
			`<db><record>a<f </record>b</record><record>c</record></db>`,
			false, true, false},
		{"ws-close-guard", "record",
			`<db><record><f>v</f></record ><record><g>w</g></record></db>`,
			false, false, false},
		{"quote-in-text", "record",
			`<db><record><q>say "hi" now</q></record><record><x>2</x></record></db>`,
			false, false, true},
		{"nested-and-prefix", "rec",
			`<db><rec><rec>i</rec><recx a="</rec>">z</recx></rec><rec>f</rec></db>`,
			false, true, true},
	}
	for _, tc := range docs {
		cfg := RecordConfig{Workers: 2}
		d := checkFastVsFull(t, tc.name, tc.doc, tc.tag, cfg)
		if d.fast < 1 || d.full != 0 {
			t.Errorf("%s: expected fast-path window(s) only, counters %+v", tc.name, d)
		}
		if tc.wantCons && d.cons < 1 {
			t.Errorf("%s: expected inline construct consumption, counters %+v", tc.name, d)
		}
		if !tc.wantCons && d.cons != 0 {
			t.Errorf("%s: unexpected constructs, counters %+v", tc.name, d)
		}
		if tc.wantFake && (d.fakes < 1 || d.disables < 1) {
			t.Errorf("%s: expected fake-hit detection+disable, counters %+v", tc.name, d)
		}
		if !tc.wantFake && d.fakes != 0 {
			t.Errorf("%s: unexpected fake hits, counters %+v", tc.name, d)
		}
		if tc.wantWalk && d.walks < 1 {
			t.Errorf("%s: expected quoted-gap walk, counters %+v", tc.name, d)
		}
	}
}

// TestFastSkeletonFakeMultiWindow puts the speculation hazard in the first of
// many windows: the fake must be detected, the fast path disabled for the
// rest of the stream, and every later record still delivered exactly.
func TestFastSkeletonFakeMultiWindow(t *testing.T) {
	var sb strings.Builder
	sb.WriteString(`<db><meta q="</record>"/>`)
	for i := range 200 {
		fmt.Fprintf(&sb, `<record id="%d"><f>%d</f></record>`, i, i)
	}
	sb.WriteString(`</db>`)
	doc := sb.String()

	ref := refParse(t, []byte(doc), "record")
	pre := snapSkel()
	got, _, _, err := collectParallel([]byte(doc),
		RecordConfig{Tag: []byte("record"), Workers: 4, ChunkSize: 256, CarryCap: 32})
	d := snapSkel().delta(pre)
	if err != nil {
		t.Fatal(err)
	}
	checkAgainstRef(t, ref, got, "multiwindow")
	if d.fakes < 1 || d.disables < 1 {
		t.Fatalf("fake in window 1 not detected: counters %+v", d)
	}
	if d.fast < 1 || d.full < 1 {
		t.Fatalf("expected fast window(s) before the disable and full after: %+v", d)
	}
}

// TestFastSkeletonBoundarySweep slides every window boundary across
// fast-path documents (clean, quoted-fake, ws-close, nested, self-close,
// prefix-collision, CDATA fallback) so pattern hits, guards and quoted gaps
// get truncated at every possible offset. Tiny carry also forces the
// spill/jumbo interplay through the fast walker.
func TestFastSkeletonBoundarySweep(t *testing.T) {
	docs := []struct{ tag, doc string }{
		{"rec", `<db><rec a="x>y"><f>` + strings.Repeat("ab ", 30) + `"q"</f><rec>n</rec></rec>` +
			`<meta q="</rec>"/><rec>t</rec ><rec/><record>not</record><rec>z</rec></db>`},
		{"record", `<db><record id="1"><f>v</f></record>plain text > here` +
			`<record id="2"/><record>a<x </record>b</record></db>`},
		{"r", `<db><r><c><![CDATA[</r>]]></c></r><r a="</r>">q</r><r>t</r></db>`},
	}
	for di, tc := range docs {
		ref := refParse(t, []byte(tc.doc), tc.tag)
		for cs := 64; cs <= 220; cs++ {
			got, _, _, err := collectParallel([]byte(tc.doc),
				RecordConfig{Tag: []byte(tc.tag), Workers: 2, ChunkSize: cs, CarryCap: 16})
			if err != nil {
				t.Fatalf("doc %d chunk=%d: %v", di, cs, err)
			}
			checkAgainstRef(t, ref, got, fmt.Sprintf("doc %d chunk=%d", di, cs))
		}
	}
}

// TestFastSkeletonParity runs every corpus document through the fast path and
// the forced-full walker and requires identical record streams.
func TestFastSkeletonParity(t *testing.T) {
	geoms := []struct{ chunk, carry int }{{1 << 20, 64 << 10}, {97, 24}}
	for _, tc := range recordCorpus() {
		for _, g := range geoms {
			label := fmt.Sprintf("%s/chunk=%d", tc.name, g.chunk)
			checkFastVsFull(t, label, tc.doc, tc.tag,
				RecordConfig{Workers: 4, ChunkSize: g.chunk, CarryCap: g.carry})
		}
	}
	for di, doc := range corpus() {
		for _, tag := range []string{"a", "r", "rec"} {
			label := fmt.Sprintf("corpus %d tag=%s", di, tag)
			checkFastVsFull(t, label, doc, tag, RecordConfig{Workers: 2, ChunkSize: 96, CarryCap: 24})
		}
	}
}

// TestFastSkeletonConstructHeavy runs construct-dense documents — every field
// CDATA, comments everywhere, PIs between records — across many small windows
// and asserts the whole stream stays on the fast path: constructs consumed
// inline, no full-walker windows, no fake hits, no adaptive disable, and
// byte-exact parity with the forced-full walker.
func TestFastSkeletonConstructHeavy(t *testing.T) {
	var cdata, comment, pi strings.Builder
	cdata.WriteString(`<db>`)
	for i := range 80 {
		fmt.Fprintf(&cdata,
			`<record id="%d"><a><![CDATA[fake </record> "q" %d]]></a><b><![CDATA[<record> 'x' &amp;]]></b></record>`,
			i, i)
	}
	cdata.WriteString(`</db>`)
	comment.WriteString(`<db><!-- head "</record>" -->`)
	for i := range 80 {
		fmt.Fprintf(&comment,
			`<record id="%d"><!-- fake </record> --><f>v%d</f></record><!-- gap <record> "q" -->`,
			i, i)
	}
	comment.WriteString(`</db>`)
	pi.WriteString(`<?xml version="1.0"?><db><?head </record> "q"?>`)
	for i := range 80 {
		fmt.Fprintf(&pi, `<record id="%d"><f>v%d</f></record><?proc <record> data?>`, i, i)
	}
	pi.WriteString(`</db>`)

	docs := []struct {
		name, doc string
		minCons   int64
	}{
		{"cdata-every-field", cdata.String(), 160}, // two CDATA per record
		{"comment-heavy", comment.String(), 160},   // in-record + between-record comments
		{"pi-between-records", pi.String(), 80},    // prologue + head + per-record PIs
	}
	for _, tc := range docs {
		d := checkFastVsFull(t, tc.name, tc.doc, "record",
			RecordConfig{Workers: 4, ChunkSize: 512, CarryCap: 64})
		if d.full != 0 || d.fast < 2 {
			t.Errorf("%s: expected multi-window fast-only scan, counters %+v", tc.name, d)
		}
		if d.cons < tc.minCons {
			t.Errorf("%s: constructs %d < %d, counters %+v", tc.name, d.cons, tc.minCons, d)
		}
		if d.fakes != 0 || d.disables != 0 {
			t.Errorf("%s: construct interiors leaked into speculation, counters %+v", tc.name, d)
		}
	}
}

// TestFastSkeletonConstructSweep slides every window boundary (chunk sizes
// 64..220, tiny carry) across a document interleaving PIs, comments, CDATA
// and records, so construct openers, interiors and terminators are truncated
// at every possible offset — including truncations whose tail exceeds the
// carry and must escalate to the jumbo path mid-construct. Every geometry
// must match the reference and the forced-full walker byte-exactly.
func TestFastSkeletonConstructSweep(t *testing.T) {
	doc := `<?xml version="1.0"?><db><?pi </rec> "q" ?><!-- x </rec> ' -->` +
		`<rec><c><![CDATA[</rec> "q" <rec>]]></c></rec>` +
		`<rec a="b>c"><d><![CDATA[z]]></d><!-- </rec> --></rec>` +
		`text > here<rec/><!--tail "--><rec><e>f</e></rec></db>`
	ref := refParse(t, []byte(doc), "rec")
	for cs := 64; cs <= 220; cs++ {
		got, _, _, err := collectParallel([]byte(doc),
			RecordConfig{Tag: []byte("rec"), Workers: 2, ChunkSize: cs, CarryCap: 16})
		if err != nil {
			t.Fatalf("chunk=%d: %v", cs, err)
		}
		checkAgainstRef(t, ref, got, fmt.Sprintf("chunk=%d", cs))
	}
	skelForceFull.Store(true)
	defer skelForceFull.Store(false)
	for cs := 64; cs <= 220; cs++ {
		got, _, _, err := collectParallel([]byte(doc),
			RecordConfig{Tag: []byte("rec"), Workers: 2, ChunkSize: cs, CarryCap: 16})
		if err != nil {
			t.Fatalf("full chunk=%d: %v", cs, err)
		}
		checkAgainstRef(t, ref, got, fmt.Sprintf("full chunk=%d", cs))
	}
}

// TestFastSkeletonJumboConstruct drives constructs far larger than the carry
// (and the whole window) through the fast path: a jumbo CDATA inside a record
// and a jumbo comment between records, both stuffed with fake boundaries. The
// fast walker must cut at the opener, ride the jumbo assembly loop to the
// construct's end, and never see the interior fakes.
func TestFastSkeletonJumboConstruct(t *testing.T) {
	big := strings.Repeat(`fake </record> "q" <record> ]] > `, 64) // ~2KB per construct
	doc := `<db><record><x><![CDATA[` + big + `]]></x></record>` +
		`<!--` + big + `--><record><y>ok</y></record></db>`
	ref := refParse(t, []byte(doc), "record")
	pre := snapSkel()
	got, _, _, err := collectParallel([]byte(doc),
		RecordConfig{Tag: []byte("record"), Workers: 2, ChunkSize: 128, CarryCap: 16})
	d := snapSkel().delta(pre)
	if err != nil {
		t.Fatal(err)
	}
	checkAgainstRef(t, ref, got, "jumbo-construct")
	if d.full != 0 || d.cons < 2 {
		t.Errorf("expected fast-only scan with jumbo constructs consumed, counters %+v", d)
	}
	if d.fakes != 0 || d.disables != 0 {
		t.Errorf("jumbo construct interiors leaked into speculation, counters %+v", d)
	}
}

func TestFastSkeletonErraticReader(t *testing.T) {
	var sb strings.Builder
	sb.WriteString(`<db>`)
	for i := range 50 {
		fmt.Fprintf(&sb, `<record id="%d"><f>%d</f><g/></record>`, i, i)
	}
	sb.WriteString(`</db>`)
	doc := sb.String()
	ref := refParse(t, []byte(doc), "record")
	var mu sync.Mutex
	got := map[int64]refRec{}
	err := ParseRecords(&erraticReader{data: []byte(doc)},
		RecordConfig{Tag: []byte("record"), Workers: 3, ChunkSize: 64, CarryCap: 16},
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

// genBenchXML writes size bytes of genxml output with the given extra flags
// and returns the file contents.
func genBenchXML(b *testing.B, size string, extra ...string) []byte {
	b.Helper()
	path := filepath.Join(b.TempDir(), "bench.xml")
	args := append([]string{"run", "./cmd/genxml", "-size", size, "-out", path}, extra...)
	if out, err := exec.Command("go", args...).CombinedOutput(); err != nil {
		b.Skipf("genxml failed: %v\n%s", err, out)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		b.Skip(err)
	}
	return data
}

// BenchmarkSkeletonStage1 measures the two stage-1 walkers in isolation over
// one large in-memory window — a raw skeleton GB/s number, free of
// worker/pipeline effects.
func BenchmarkSkeletonStage1(b *testing.B) {
	data := genBenchXML(b, "50MB", "-cdata", "0", "-comments", "0")
	data = data[bytes.IndexByte(data, '\n')+1:] // drop the <?xml?> prologue: record-only bytes for both walkers
	for _, mode := range []string{"fast", "full"} {
		b.Run(mode, func(b *testing.B) {
			run := &recordRun{
				cfg:         RecordConfig{Tag: []byte("record"), MaxRecordSize: 1 << 30},
				walkState:   walkState{tag: []byte("record"), recStart: -1},
				groupPool:   make(chan *recGroup, 4),
				spillPool:   make(chan []byte, 4),
				groupTarget: 1 << 40, // one group per window: recycled inline below
			}
			run.hooks = run
			for range cap(run.groupPool) {
				run.groupPool <- &recGroup{}
			}
			for range cap(run.spillPool) {
				run.spillPool <- nil
			}
			ck := &rchunk{}
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			for b.Loop() {
				run.depth, run.recStart = 0, -1
				var cut int
				var err error
				if mode == "fast" {
					cut, err = run.skeletonFast(data, ck, 0)
				} else {
					cut, err = run.skeletonFull(data, ck, 0)
				}
				if err != nil || cut != len(data) {
					b.Fatalf("cut=%d/%d err=%v", cut, len(data), err)
				}
				run.flushCur()
				for _, g := range run.pending {
					g.bounds, g.chunk = g.bounds[:0], nil
					run.groupPool <- g
				}
				run.pending = run.pending[:0]
			}
		})
	}
}

// BenchmarkSkeletonFastPath measures ParseRecords with the speculative
// skeleton on versus forced off. "clean" is CDATA/comment-free (pure
// candidate hopping); "mixed" uses genxml defaults (2% CDATA fields), where
// the fast walker consumes the interleaved constructs inline.
func BenchmarkSkeletonFastPath(b *testing.B) {
	sets := []struct {
		name  string
		extra []string
	}{
		{"clean", []string{"-cdata", "0", "-comments", "0"}},
		{"mixed", nil},
	}
	for _, set := range sets {
		data := genBenchXML(b, "50MB", set.extra...)
		counts := make([]struct {
			n int64
			_ [56]byte // avoid false sharing between worker counters
		}, 64)
		handle := func(w int, rec Record) error {
			counts[w].n += int64(len(rec.Events))
			return nil
		}
		for _, mode := range []struct {
			name  string
			force bool
		}{{"fast", false}, {"full", true}} {
			b.Run(set.name+"/"+mode.name, func(b *testing.B) {
				skelForceFull.Store(mode.force)
				defer skelForceFull.Store(false)
				b.SetBytes(int64(len(data)))
				b.ReportAllocs()
				for b.Loop() {
					if err := ParseRecords(bytes.NewReader(data),
						RecordConfig{Tag: []byte("record")}, handle); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
