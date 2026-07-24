package fastsax

import (
	"fmt"
	"strings"
	"testing"
)

// This file adversarially targets the composition of per-construct speculation
// (skeleton.go's fast walker) with parallel stage 1 (stage1.go): the construct
// fast path runs INSIDE the pre-walk capture hooks, so nextMark's window-local
// candidate caches, atomic scanMarkup consumption at the H-guess fragment
// boundary and the p0 resume parameter all interact here. Every case is
// verified three ways: forced-parallel vs forced-sequential stage 1
// (checkS1Differential) and forced-parallel vs the naive reference parser.

// s1verify runs one document through the parallel differential harness across
// workers {4,8} x ordered x the given geometries, plus the naive reference.
// Returns the accumulated parallel-run counter delta.
func s1verify(t *testing.T, label, tag string, doc []byte, geoms []struct{ chunk, carry int }) s1Snap {
	t.Helper()
	ref := refParse(t, doc, tag)
	var total s1Snap
	for _, g := range geoms {
		for _, w := range []int{4, 8} {
			for _, ordered := range []bool{false, true} {
				cfg := RecordConfig{Tag: []byte(tag), Workers: w, Ordered: ordered,
					ChunkSize: g.chunk, CarryCap: g.carry}
				l := fmt.Sprintf("%s/chunk=%d/w=%d/ordered=%v", label, g.chunk, w, ordered)
				d := checkS1Differential(t, l, doc, cfg)
				total.prewalks += d.prewalks
				total.hits += d.hits
				total.misses += d.misses
				total.disables += d.disables
				got, _, err := collectS1(doc, cfg, true)
				if err != nil {
					t.Fatalf("%s: %v", l, err)
				}
				checkAgainstRef(t, ref, got, l)
			}
		}
	}
	return total
}

// TestParallelStage1ConstructAdversarial feeds the construct-speculation
// adversarial corpus (skeleton_adv_test.go's advDocs: fake boundaries hugging
// construct delimiters, openers in attributes, DOCTYPE subsets, PI quotes)
// through the parallel stage-1 harness. These documents were built against the
// fast walker in isolation; here the same walker runs under capture hooks with
// H-guesses landing anywhere inside them.
func TestParallelStage1ConstructAdversarial(t *testing.T) {
	geoms := []struct{ chunk, carry int }{{64, 16}, {97, 24}, {1 << 20, 64 << 10}}
	for _, tc := range advDocs {
		s1verify(t, tc.name, tc.tag, []byte(tc.doc), geoms)
	}
}

// TestParallelStage1ConstructHeavy runs construct-dense documents (every field
// CDATA, comments everywhere, PIs between records — the shapes from
// TestFastSkeletonConstructHeavy) through parallel stage 1 and asserts the
// composition actually engaged: constructs consumed inline by the fast walker
// (in pre-walks and reducer walks alike) AND proven H-guesses replayed, in the
// same runs, with byte-exact output.
func TestParallelStage1ConstructHeavy(t *testing.T) {
	var cdata, comment, pi strings.Builder
	cdata.WriteString(`<db>`)
	for i := range 120 {
		fmt.Fprintf(&cdata,
			`<record id="%d"><a><![CDATA[fake </record> "q" %d]]></a><b><![CDATA[<record> 'x' &amp;]]></b></record>`,
			i, i)
	}
	cdata.WriteString(`</db>`)
	comment.WriteString(`<db><!-- head "</record>" -->`)
	for i := range 120 {
		fmt.Fprintf(&comment,
			`<record id="%d"><!-- fake </record> --><f>v%d</f></record><!-- gap <record> "q" -->`,
			i, i)
	}
	comment.WriteString(`</db>`)
	pi.WriteString(`<?xml version="1.0"?><db><?head </record> "q"?>`)
	for i := range 120 {
		fmt.Fprintf(&pi, `<record id="%d"><f>v%d</f></record><?proc <record> data?>`, i, i)
	}
	pi.WriteString(`</db>`)

	docs := []struct{ name, doc string }{
		{"cdata-every-field", cdata.String()},
		{"comment-heavy", comment.String()},
		{"pi-between-records", pi.String()},
	}
	geoms := []struct{ chunk, carry int }{{512, 64}, {1024, 128}}
	for _, tc := range docs {
		skelPre := snapSkel()
		d := s1verify(t, tc.name, "record", []byte(tc.doc), geoms)
		sd := snapSkel().delta(skelPre)
		if d.hits == 0 {
			t.Errorf("%s: no proven H-guesses over a construct-dense document: %+v", tc.name, d)
		}
		if sd.cons == 0 {
			t.Errorf("%s: fast walker consumed no constructs: %+v", tc.name, sd)
		}
		if sd.fakes != 0 {
			t.Errorf("%s: construct interiors leaked into speculation: %+v", tc.name, sd)
		}
	}
}

// TestParallelStage1ConstructAtGuess slides a comment/PI/CDATA cluster across
// every alignment of a 64-byte chunk grid, so construct openers land exactly
// at chunk borders and exactly at H-guess points (the byte after a chunk's
// first '>'). This is the never-before-exercised corner: the pre-walk's fast
// walker starting with a construct-opener candidate at slice position 0, or
// with the opener truncated by the guess fragment boundary.
func TestParallelStage1ConstructAtGuess(t *testing.T) {
	pattern := `</m><!--x<record>y</record>--><record a="1>2">v</record>` +
		`<?pi </record> 'q'?><record><![CDATA[z>z</record>]]></record><record/>`
	for pad := 0; pad < 70; pad++ {
		doc := []byte(`<db><m>` + strings.Repeat("p", pad) + pattern + `</db>`)
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
}

// TestParallelStage1ConstructBorderSweep sweeps chunk sizes across a fixed
// construct sandwich so every construct opener, interior byte and terminator
// is cut by a chunk border (and by the H-guess fragment) at every offset.
func TestParallelStage1ConstructBorderSweep(t *testing.T) {
	doc := []byte(`<?xml version="1.0"?><db><?pi </rec> "q" ?><!-- x </rec> ' -->` +
		`<rec><c><![CDATA[</rec> "q" <rec>]]></c></rec>` +
		`<rec a="b>c"><d><![CDATA[z]]></d><!-- </rec> --></rec>` +
		`text > here<rec/><!--tail "--><rec><e>f</e></rec></db>`)
	ref := refParse(t, doc, "rec")
	for cs := 48; cs <= 160; cs++ {
		cfg := RecordConfig{Tag: []byte("rec"), Workers: 4, ChunkSize: cs, CarryCap: 16}
		l := fmt.Sprintf("chunk=%d", cs)
		checkS1Differential(t, l, doc, cfg)
		got, _, err := collectS1(doc, cfg, true)
		if err != nil {
			t.Fatalf("%s: %v", l, err)
		}
		checkAgainstRef(t, ref, got, l)
	}
}

// TestParallelStage1JumboConstruct drives constructs far larger than the whole
// chunk through parallel stage 1: pre-walks land inside opaque interiors full
// of fake records (their guesses must be disproved or their captures never
// consumed), the reducer escalates to the jumbo path and retires the tasks,
// and speculation must resume exactly on the clean records after.
func TestParallelStage1JumboConstruct(t *testing.T) {
	big := strings.Repeat(`fake </record> "q" <record> ]] > `, 64) // ~2KB per construct
	doc := `<db><record><x><![CDATA[` + big + `]]></x></record>` +
		`<!--` + big + `--><record><y>ok</y></record>` +
		strings.Repeat(`<record><f>tail</f></record>`, 20) + `</db>`
	d := s1verify(t, "jumbo", "record", []byte(doc),
		[]struct{ chunk, carry int }{{128, 16}, {256, 32}})
	if d.prewalks == 0 {
		t.Fatalf("jumbo document never pre-walked: %+v", d)
	}
}
