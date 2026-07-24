package fastsax

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
	"unsafe"
)

// TestEventSize guards the deliberate 12-byte pointer-free Event layout
// (Kind + 3-byte nlen + Start + End). If this fails, a field change has
// grown the tape; see the Event docs before touching it.
func TestEventSize(t *testing.T) {
	if s := unsafe.Sizeof(Event{}); s != 12 {
		t.Fatalf("Event size = %d, want 12", s)
	}
}

type tok struct {
	k Kind
	v string
}

// collect parses data with the given options and returns the raw token
// stream with adjacent Text events coalesced (Text may legally split at
// window boundaries).
func collect(data []byte, opts ...Option) ([]tok, error) {
	p, err := NewParser(bytes.NewReader(data), opts...)
	if err != nil {
		return nil, err
	}
	defer p.Close()
	var out []tok
	for {
		b, err := p.Next()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		for _, e := range b.Events {
			raw := string(b.Bytes(e))
			// Invariant piggybacked on every collect-based test and fuzz
			// input: the tape-recorded name (Event.nlen) must agree with
			// re-scanning the tag bytes. Panic, not error: a mismatch is
			// tape corruption, never a legitimate parse outcome.
			if e.Kind == KindStart || e.Kind == KindEnd || e.Kind == KindSelfClose {
				if want, _ := Name(b.Bytes(e)); !bytes.Equal(b.Name(e), want) {
					panic(fmt.Sprintf("tape name %q != Name() %q for %v %q",
						b.Name(e), want, e.Kind, raw))
				}
			}
			if e.Kind == KindText && len(out) > 0 && out[len(out)-1].k == KindText {
				out[len(out)-1].v += raw
				continue
			}
			out = append(out, tok{e.Kind, raw})
		}
	}
}

func toksEqual(a, b []tok) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

const sampleDoc = `<?xml version="1.0" encoding="UTF-8"?>
<dataset a="1" b='two'>
  <!-- a comment with "quotes" and > signs -->
  <record id="1" note="a &gt; b">
    <name>Hello &amp; goodbye</name>
    <location>Africa/Abidjan</location>
    <blob><![CDATA[raw <stuff> & more ]] here]]></blob>
    <empty/>
    <weird attr="value with > inside"/>
  </record>
  <record id="2"><name>x</name></record>
</dataset>
`

func TestScanBasic(t *testing.T) {
	toks, err := collect([]byte(sampleDoc), WithPrefetch(false))
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, tk := range toks {
		kinds = append(kinds, tk.k.String())
	}
	got := strings.Join(kinds, " ")
	want := "PI Text Start Text Comment Text Start Text Start Text End Text Start Text End Text Start CData End Text SelfClose Text SelfClose Text End Text Start Start Text End End Text End Text"
	if got != want {
		t.Fatalf("kind sequence mismatch:\n got: %s\nwant: %s", got, want)
	}
	// spot checks
	if toks[0].v != `<?xml version="1.0" encoding="UTF-8"?>` {
		t.Errorf("PI bytes: %q", toks[0].v)
	}
	for _, tk := range toks {
		if tk.k == KindCData && tk.v != `<![CDATA[raw <stuff> & more ]] here]]>` {
			t.Errorf("CData bytes: %q", tk.v)
		}
		if tk.k == KindSelfClose && strings.HasPrefix(tk.v, "<weird") {
			if tk.v != `<weird attr="value with > inside"/>` {
				t.Errorf("quoted > self-close: %q", tk.v)
			}
		}
	}
}

// TestEventNameConsistency: for every tag event, the tape-recorded name
// (Batch.Name / eventName, which uses Event.NameLen when set) must equal the
// name half of the Name helper — across normal, degenerate and
// window-boundary-split inputs, and through the scanRecord/predictor path.
func TestEventNameConsistency(t *testing.T) {
	docs := []string{
		sampleDoc,
		"<a><b/></a>",
		"<a />",
		"<a/ >",
		"<name >spaced</name ></name\t\n>",
		"<a//>",
		"<a\"x\">t</a>",          // quote directly after the name: falls back
		"<a b=\"x>y\" c='<'/>",   // quoted markup characters
		"<x:y ns=\"1\"></ x:y >", // prefixed names, space after '</'
		"<r><![CDATA[<f>]]><!-- <g> --><?pi <h>?></r>",
		"<>t</>",     // nameless tags: NameLen 0, fall back to Name
		"</>",        // nameless end tag
		"< a>t</ a>", // whitespace right after '<': empty name, falls back
	}
	check := func(t *testing.T, data []byte, tape []Event) {
		t.Helper()
		for _, e := range tape {
			if e.Kind != KindStart && e.Kind != KindEnd && e.Kind != KindSelfClose {
				if e.NameLen() != 0 {
					t.Fatalf("non-tag event %v has NameLen %d", e.Kind, e.NameLen())
				}
				continue
			}
			want, _ := Name(data[e.Start:e.End])
			got := eventName(data, e)
			if string(got) != string(want) {
				t.Fatalf("event %v %q: tape name %q, Name() %q",
					e.Kind, data[e.Start:e.End], got, want)
			}
		}
	}
	for _, doc := range docs {
		data := []byte(doc)
		// Direct scanChunk pass.
		tape, _, _ := scanChunk(data, nil)
		check(t, data, tape)
		// scanRecord with a predictor, twice: learn pass then hit pass.
		var pr fieldPredictor
		for range 2 {
			rt, _, _ := scanRecord(data, nil, &pr)
			check(t, data, rt)
		}
		// Through the parser at tiny windows (carry/split geometries).
		p, err := NewParser(bytes.NewReader(data), WithPrefetch(false),
			WithChunkSize(64), WithCarryCap(16))
		if err != nil {
			t.Fatal(err)
		}
		for {
			b, err := p.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				break // malformed docs may error; consistency is checked per batch
			}
			check(t, b.Data, b.Events)
		}
		p.Close()
	}
}

// TestEventNamePackingLimit: name lengths at the 3-byte Event.nlen limit.
// 2^24-1 is the largest recordable length; 2^24 must be recorded as 0 and
// fall back to Name, still yielding the correct name.
func TestEventNamePackingLimit(t *testing.T) {
	for _, nl := range []int{1<<24 - 1, 1 << 24} {
		name := bytes.Repeat([]byte{'n'}, nl)
		data := make([]byte, 0, nl+8)
		data = append(data, '<')
		data = append(data, name...)
		data = append(data, '>', '<', '/', 'x', '>')
		tape, _, err := scanChunk(data, nil)
		if err != nil || len(tape) == 0 {
			t.Fatalf("nl=%d: tape=%d err=%v", nl, len(tape), err)
		}
		e := tape[0]
		if e.Kind != KindStart {
			t.Fatalf("nl=%d: kind=%v", nl, e.Kind)
		}
		wantRec := nl
		if nl >= 1<<24 {
			wantRec = 0
		}
		if e.NameLen() != wantRec {
			t.Fatalf("nl=%d: NameLen=%d, want %d", nl, e.NameLen(), wantRec)
		}
		if got := eventName(data, e); !bytes.Equal(got, name) {
			t.Fatalf("nl=%d: eventName len=%d, want %d", nl, len(got), nl)
		}
	}
}

// TestCountAfricaTapeParity: the tape-name consumer must agree with the
// Name()-based one over the shared corpus.
func TestCountAfricaTapeParity(t *testing.T) {
	data := loadBenchData(t)
	p, err := NewParser(bytes.NewReader(data), WithPrefetch(false))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	want, _, err := countAfrica(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	rd := bytes.NewReader(data)
	p.Reset(rd)
	got, _, err := countAfricaTape(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != want || got == 0 {
		t.Fatalf("countAfricaTape=%d countAfrica=%d", got, want)
	}
}

func TestScanQuotedGT(t *testing.T) {
	toks, err := collect([]byte(`<a x="1>2" y='3>4'>text</a>`), WithPrefetch(false))
	if err != nil {
		t.Fatal(err)
	}
	if len(toks) != 3 || toks[0].v != `<a x="1>2" y='3>4'>` || toks[0].k != KindStart {
		t.Fatalf("got %+v", toks)
	}
}

func TestScanDoctype(t *testing.T) {
	doc := `<!DOCTYPE note [<!ELEMENT note (#PCDATA)> <!ENTITY x "y">]><note>hi</note>`
	toks, err := collect([]byte(doc), WithPrefetch(false))
	if err != nil {
		t.Fatal(err)
	}
	if toks[0].k != KindDocType || !strings.HasSuffix(toks[0].v, "]>") {
		t.Fatalf("doctype: %+v", toks[0])
	}
}

// corpus returns documents used for the boundary sweep and fuzz seeds.
func corpus() []string {
	long := strings.Repeat("abcdefgh", 40) // 320 bytes: bigger than small carryCap => jumbo
	return []string{
		sampleDoc,
		`<a/>`,
		`<a></a>`,
		`<a>` + long + `</a>`,
		`<a><!--` + long + `--></a>`,
		`<a><![CDATA[` + long + `]]></a>`,
		`<a b="` + long + `">x</a>`,
		`<r><a>1</a><b>2</b><c/></r><r><a>3</a></r>`,
		"text before <a>in</a> text after",
		`<a>&lt;&gt;&amp;&apos;&quot;&#65;&#x42;</a>`,
		"<a>line\rbreak\r\nmix</a>",
		`<a><![CDATA[</a> fake close]]></a>`,
		`<a><!-- </a> --><b attr="</a>">t</b></a>`,
		`<?pi with <angle> stuff?><a/>`,
		`<!DOCTYPE d [<!ENTITY e "v">]><a/>`,
		`<ns:tag xmlns:ns="urn:x" ns:attr="v">t</ns:tag>`,
		`<a>]]></a>`,
		`<a  spaced = "v"   flag  ><b/></a>`,
		"\xEF\xBB\xBF<a>bom</a>",
	}
}

// TestBoundarySweep is the core correctness test: the same document parsed
// with many window geometries must produce the identical coalesced stream.
// Tiny carryCap forces the jumbo path; tiny chunks hit every straddle offset.
func TestBoundarySweep(t *testing.T) {
	for di, doc := range corpus() {
		ref, err := collect([]byte(doc), WithPrefetch(false), WithChunkSize(1<<20))
		if err != nil {
			t.Fatalf("doc %d: reference parse failed: %v", di, err)
		}
		for _, carry := range []int{16, 17, 24, 33, 64} {
			for cs := 64; cs <= 256; cs++ {
				got, err := collect([]byte(doc), WithPrefetch(false), WithChunkSize(cs), WithCarryCap(carry))
				if err != nil {
					t.Fatalf("doc %d chunk=%d carry=%d: %v", di, cs, carry, err)
				}
				if !toksEqual(ref, got) {
					t.Fatalf("doc %d chunk=%d carry=%d: stream mismatch\n ref: %v\n got: %v", di, cs, carry, ref, got)
				}
			}
		}
		// prefetch path must agree as well
		got, err := collect([]byte(doc), WithPrefetch(true), WithChunkSize(96), WithCarryCap(24))
		if err != nil {
			t.Fatalf("doc %d prefetch: %v", di, err)
		}
		if !toksEqual(ref, got) {
			t.Fatalf("doc %d prefetch: stream mismatch\n ref: %v\n got: %v", di, ref, got)
		}
	}
}

// erraticReader returns data in a repeating pattern of tiny read sizes,
// including (0, nil) reads.
type erraticReader struct {
	data []byte
	pos  int
	step int
}

func (r *erraticReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	r.step++
	n := min(r.step%5, len(p), len(r.data)-r.pos) // 0..4 bytes
	copy(p, r.data[r.pos:r.pos+n])
	r.pos += n
	return n, nil
}

func TestErraticReader(t *testing.T) {
	for di, doc := range corpus() {
		ref, err := collect([]byte(doc), WithPrefetch(false))
		if err != nil {
			t.Fatalf("doc %d: %v", di, err)
		}
		p, err := NewParser(&erraticReader{data: []byte(doc)}, WithPrefetch(false), WithChunkSize(64), WithCarryCap(16))
		if err != nil {
			t.Fatal(err)
		}
		var got []tok
		for {
			b, err := p.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("doc %d: %v", di, err)
			}
			for _, e := range b.Events {
				raw := string(b.Bytes(e))
				if e.Kind == KindText && len(got) > 0 && got[len(got)-1].k == KindText {
					got[len(got)-1].v += raw
					continue
				}
				got = append(got, tok{e.Kind, raw})
			}
		}
		if !toksEqual(ref, got) {
			t.Fatalf("doc %d erratic: mismatch\n ref: %v\n got: %v", di, ref, got)
		}
	}
}

func TestErrors(t *testing.T) {
	cases := []struct {
		doc  string
		want string
	}{
		{"<a>unclosed <ta", "unexpected EOF"},
		{"<a><!-- unterminated", "unexpected EOF"},
		{"<a><![CDATA[ unterminated", "unexpected EOF"},
		{"<!x>", "unknown markup"},
		{"\xFE\xFF\x00<", "UTF-16"},
	}
	for _, c := range cases {
		_, err := collect([]byte(c.doc), WithPrefetch(false))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("doc %q: got err %v, want containing %q", c.doc, err, c.want)
		}
	}
}

func TestErrorAfterFinalBatch(t *testing.T) {
	// Valid events before an EOF-truncated tag must still be delivered.
	p, err := NewParser(bytes.NewReader([]byte("<a>hello<b")), WithPrefetch(false))
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Next()
	if err != nil {
		t.Fatalf("first Next should deliver batch, got %v", err)
	}
	if len(b.Events) != 2 { // <a> + text
		t.Fatalf("events: %d", len(b.Events))
	}
	if _, err = p.Next(); err == nil || !strings.Contains(err.Error(), "unexpected EOF") {
		t.Fatalf("second Next: %v", err)
	}
}

func TestHelpers(t *testing.T) {
	name, rest := Name([]byte(`<record id="1" note="n">`))
	if string(name) != "record" {
		t.Fatalf("name %q", name)
	}
	a1, rest, err := NextAttribute(rest)
	if err != nil || string(a1.Key) != "id" || string(a1.Value) != "1" {
		t.Fatalf("attr1 %v %v", a1, err)
	}
	a2, _, err := NextAttribute(rest)
	if err != nil || string(a2.Key) != "note" || string(a2.Value) != "n" {
		t.Fatalf("attr2 %v %v", a2, err)
	}
	name, _ = Name([]byte(`</record>`))
	if string(name) != "record" {
		t.Fatalf("end name %q", name)
	}
	name, _ = Name([]byte(`<empty/>`))
	if string(name) != "empty" {
		t.Fatalf("self name %q", name)
	}

	got, err := Unescape([]byte("a &lt;b&gt; &amp; &#65;&#x42; c\r\nd\re"))
	if err != nil || string(got) != "a <b> & AB c\nd\ne" {
		t.Fatalf("unescape %q %v", got, err)
	}
	out, err := UnescapeAppend(nil, []byte("x &amp; y"))
	if err != nil || string(out) != "x & y" {
		t.Fatalf("unescapeAppend %q %v", out, err)
	}
}

func TestInputOffset(t *testing.T) {
	doc := []byte(`<a>` + strings.Repeat("x", 200) + `</a>`)
	p, err := NewParser(bytes.NewReader(doc), WithPrefetch(false), WithChunkSize(64), WithCarryCap(16))
	if err != nil {
		t.Fatal(err)
	}
	var reassembled []byte
	for {
		b, err := p.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		off := p.InputOffset()
		if int(off) != len(reassembled) {
			t.Fatalf("offset %d, want %d", off, len(reassembled))
		}
		reassembled = append(reassembled, b.Data...)
	}
	if !bytes.Equal(reassembled, doc) {
		t.Fatalf("reassembly mismatch: %d vs %d bytes", len(reassembled), len(doc))
	}
}

func ExampleParser() {
	doc := `<root><item id="1">first</item><item id="2">second</item></root>`
	p, _ := NewParser(strings.NewReader(doc), WithPrefetch(false))
	defer p.Close()
	for {
		b, err := p.Next()
		if err != nil {
			break
		}
		for _, e := range b.Events {
			if e.Kind == KindStart {
				name, _ := Name(b.Bytes(e))
				fmt.Println(string(name))
			}
		}
	}
	// Output:
	// root
	// item
	// item
}
