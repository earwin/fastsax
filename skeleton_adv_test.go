package fastsax

import (
	"fmt"
	"strings"
	"testing"
)

// advDocs are adversarial documents aimed at the construct-level speculation:
// fake record boundaries pressed directly against construct delimiters, fake
// delimiters of one construct kind inside another, quotes and brackets inside
// PI/DOCTYPE interiors, and construct openers hiding in attribute values.
var advDocs = []struct{ name, tag, doc string }{
	{"close-then-cdata-opener", "record",
		`<db><record><a>x</a></record><![CDATA[</record> tail]]><record><b>y</b></record></db>`},
	{"cdata-closer-then-close", "record",
		`<db><record><a><![CDATA[fake </record>]]></a></record><record><b>z</b></record></db>`},
	{"open-right-after-cdata", "record",
		`<db><![CDATA[<record>]]><record><a>1</a></record></db>`},
	{"comment-with-cdata-closer", "record",
		`<db><!-- ]]> </record> --><record><a>1</a></record><record><b>2</b></record></db>`},
	{"cdata-with-comment-opener", "record",
		`<db><record><a><![CDATA[ <!-- </record> not a comment ]]></a></record><record><b>2</b></record></db>`},
	{"cdata-with-comment-closer", "record",
		`<db><record><a><![CDATA[ --> </record> ]]></a></record><record><b>2</b></record></db>`},
	{"pi-with-quotes-and-fakes", "record",
		`<db><?p "</record>" '</record>' <record> ?><record><a>1</a></record></db>`},
	{"pi-adjacent-to-boundaries", "record",
		`<db><record><a>1</a></record><?p </record> ?><record><b>2</b></record><?tail?></db>`},
	{"doctype-nested-fakes", "record",
		`<!DOCTYPE db [<!ENTITY r "</record>"><!ELEMENT record ANY><!-- </record> -->]><db><record><a>1</a></record></db>`},
	{"record-attr-cdata-opener", "record",
		`<db><record a="<![CDATA[" b="]]>"><f>v</f></record><record><g>w</g></record></db>`},
	{"record-attr-comment-opener", "record",
		"<db><record\ta=\"<!-- fake\" b='</record>'><f>v</f></record><record><g>w</g></record></db>"},
	{"other-attr-cdata-opener", "record",
		`<db><meta a="<![CDATA[ </record>"/><record><f>v</f></record><record><g>w</g></record></db>`},
	{"other-attr-bad-bang", "record",
		`<db><meta a="<!bogus"/><record><f>v</f></record><record><g>w</g></record></db>`},
	{"construct-between-nested", "record",
		`<db><record><record><![CDATA[</record>]]></record><!-- </record> --></record><record><b>2</b></record></db>`},
	{"back-to-back-constructs", "record",
		`<db><!--a--><![CDATA[</record>]]><?p </record>?><!--b--><record><a>1</a></record></db>`},
	{"stray-closers-in-text", "record",
		`<db><record><a>x ]]&gt; y ]]> z --&gt; w --> v ?&gt;</a></record><record><b>2</b></record></db>`},
	{"ws-guards-by-constructs", "record",
		`<db><record ><a>1</a></record ><!--x--><record
><b>2</b></record	></db>`},
	{"self-close-by-constructs", "record",
		`<db><!--x--><record/><![CDATA[</record>]]><record a="b"/><?p?></db>`},
	{"comment-dashes", "record",
		`<db><!-----></record>--><record><a>1</a></record></db>`},
	{"doctype-then-records", "record",
		`<?xml version="1.0"?><!DOCTYPE db [<!ENTITY a "<record><f>1</f></record>">]><db><record><f>2</f></record></db>`},
}

// TestFastSkeletonConstructAdjacent checks every adversarial document at a
// window geometry that keeps the whole document in one window, requiring
// byte-exact parity between the fast path, the forced-full walker and the
// naive reference.
func TestFastSkeletonConstructAdjacent(t *testing.T) {
	for _, tc := range advDocs {
		checkFastVsFull(t, tc.name, tc.doc, tc.tag, RecordConfig{Workers: 2})
	}
}

// TestFastSkeletonConstructAdjacentSweep slides window boundaries across the
// same adversarial corpus so every construct delimiter, fake boundary, guard
// byte and attribute quote is truncated at every possible offset, on both
// paths.
func TestFastSkeletonConstructAdjacentSweep(t *testing.T) {
	for _, force := range []bool{false, true} {
		skelForceFull.Store(force)
		for _, tc := range advDocs {
			ref := refParse(t, []byte(tc.doc), tc.tag)
			for cs := 32; cs <= 200; cs++ {
				got, _, _, err := collectParallel([]byte(tc.doc),
					RecordConfig{Tag: []byte(tc.tag), Workers: 2, ChunkSize: cs, CarryCap: 16})
				if err != nil {
					t.Fatalf("%s force=%v chunk=%d: %v", tc.name, force, cs, err)
				}
				checkAgainstRef(t, ref, got, fmt.Sprintf("%s force=%v chunk=%d", tc.name, force, cs))
			}
		}
	}
	skelForceFull.Store(false)
}

// TestFastSkeletonErrorParity feeds malformed documents (bad "<!x", EOF inside
// a construct, unterminated quote, unterminated record) through both paths at
// several geometries and requires the fast path to fail exactly like the full
// walker: same error type, offset and message.
func TestFastSkeletonErrorParity(t *testing.T) {
	docs := []struct{ name, doc string }{
		{"bad-bang-in-text", `<db><record><a>1</a></record>x<!bogus><record><b>2</b></record></db>`},
		{"eof-in-cdata", `<db><record><a><![CDATA[never closed`},
		{"eof-in-comment", `<db><record><a>1</a></record><!-- never closed`},
		{"eof-in-pi", `<db><record><a>1</a></record><?p never closed`},
		{"eof-in-doctype", `<!DOCTYPE db [<!ENTITY a "x">`},
		{"eof-in-quote", `<db><record a="never closed`},
		{"eof-in-record", `<db><record><a>1</a>`},
		{"eof-in-close", `<db><record><a>1</a></record`},
	}
	geoms := []struct{ chunk, carry int }{{1 << 20, 64 << 10}, {64, 16}, {33, 16}}
	for _, tc := range docs {
		for _, g := range geoms {
			cfg := RecordConfig{Tag: []byte("record"), Workers: 2, ChunkSize: g.chunk, CarryCap: g.carry}
			_, _, _, fastErr := collectParallel([]byte(tc.doc), cfg)
			skelForceFull.Store(true)
			_, _, _, fullErr := collectParallel([]byte(tc.doc), cfg)
			skelForceFull.Store(false)
			if (fastErr == nil) != (fullErr == nil) {
				t.Fatalf("%s chunk=%d: fast err %v, full err %v", tc.name, g.chunk, fastErr, fullErr)
			}
			if fastErr != nil && fastErr.Error() != fullErr.Error() {
				t.Errorf("%s chunk=%d: fast err %q, full err %q", tc.name, g.chunk, fastErr, fullErr)
			}
		}
	}
}

// TestFastSkeletonConstructTorture interleaves every hazard in one long
// multi-window document — records with construct-opener attributes, jumbo
// CDATA, comments hugging record boundaries, PIs with quotes — and sweeps a
// range of geometries including ones that force mid-construct jumbo
// escalation on both paths.
func TestFastSkeletonConstructTorture(t *testing.T) {
	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0"?><!DOCTYPE db [<!ENTITY r "</record>">]><db>`)
	big := strings.Repeat(`</record> <record> "q" ]] > -- > `, 20)
	for i := range 40 {
		switch i % 5 {
		case 0:
			fmt.Fprintf(&sb, `<record id="%d"><a><![CDATA[%s]]></a></record>`, i, big)
		case 1:
			fmt.Fprintf(&sb, `<!-- </record> --><record id="%d"><f>v</f></record><!-- <record> -->`, i)
		case 2:
			fmt.Fprintf(&sb, `<record a="<![CDATA[" b="</record>" id="%d"><f>v</f></record><?p "</record>"?>`, i)
		case 3:
			fmt.Fprintf(&sb, `<record id="%d"><record><![CDATA[</record>]]></record><g/></record>`, i)
		case 4:
			fmt.Fprintf(&sb, `text ]]> here<record id="%d"/><!--%s-->`, i, big)
		}
	}
	sb.WriteString(`</db>`)
	doc := sb.String()
	ref := refParse(t, []byte(doc), "record")
	for _, force := range []bool{false, true} {
		skelForceFull.Store(force)
		for _, g := range []struct{ chunk, carry int }{
			{96, 16}, {128, 32}, {257, 24}, {1 << 20, 64 << 10},
		} {
			got, _, _, err := collectParallel([]byte(doc),
				RecordConfig{Tag: []byte("record"), Workers: 4, ChunkSize: g.chunk, CarryCap: g.carry})
			if err != nil {
				t.Fatalf("force=%v chunk=%d: %v", force, g.chunk, err)
			}
			checkAgainstRef(t, ref, got, fmt.Sprintf("force=%v chunk=%d", force, g.chunk))
		}
	}
	skelForceFull.Store(false)
}
