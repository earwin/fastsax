package fastsax

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
)

// FuzzDifferential cross-checks the parser two ways on every input:
//
//  1. Self-consistency: a big-window parse and a tiny-window parse (which
//     exercises carry and jumbo on nearly every token) must agree — same
//     coalesced event stream on success, or both must fail. Neither may panic.
//  2. Differential vs encoding/xml, restricted to inputs where semantics
//     match by design: DOCTYPE ("<!D"/"<!d") is excluded (quote/bracket
//     handling differs deliberately) and every "<!" must open a comment or
//     CDATA section (our bang handling is lenient on garbage). Both streams
//     are folded to normalized tokens before comparison.
func FuzzDifferential(f *testing.F) {
	for _, s := range corpus() {
		f.Add([]byte(s))
	}
	deep := strings.Repeat("<d>", 100) + "x" + strings.Repeat("</d>", 100)
	for _, s := range []string{
		`<r><![CDATA[</r> fake close]]></r>`,
		`<a><![CDATA[--> not a comment end]]></a>`,
		`<a><!-- "quotes" and 'more' > here --></a>`,
		`<a>text with ]]> inside</a>`,
		`<a k="&lt;&amp;&#65;">&quot;&apos;&#x1F600; &gt;</a>`,
		"\xEF\xBB\xBF<a>bom</a>",
		"",
		"<",
		"<a",
		deep,
	} {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		big, errBig := collect(data, WithPrefetch(false), WithChunkSize(1<<20))
		small, errSmall := collect(data, WithChunkSize(64), WithCarryCap(16))
		if (errBig == nil) != (errSmall == nil) {
			t.Fatalf("window geometry changes success: big=%v small=%v\ninput: %q", errBig, errSmall, data)
		}
		if errBig != nil {
			return
		}
		if !toksEqual(big, small) {
			t.Fatalf("window geometry changes stream:\ninput: %q\n big:   %v\n small: %v", data, big, small)
		}

		// Differential comparison against encoding/xml.
		if len(data) > 1<<16 {
			// Window-split text would desync per-event entity decoding from
			// encoding/xml's per-token decoding; self-consistency above still
			// covers large inputs.
			return
		}
		if bytes.Contains(data, []byte("<!D")) || bytes.Contains(data, []byte("<!d")) {
			return // DOCTYPE semantics differ by design
		}
		if !bangOK(data) {
			return // lenient "<!" garbage handling differs by design
		}
		want, xmlOK := foldXML(bytes.TrimPrefix(data, utf8BOM))
		if !xmlOK {
			return // encoding/xml rejects the input; nothing to compare
		}
		got, err, unescape := foldOurs(big)
		if err != nil {
			if unescape {
				t.Errorf("Unescape rejected input encoding/xml accepts: %v\ninput: %q", err, data)
			}
			return // by-design attribute leniency gap; skip
		}
		if !slices.Equal(got, want) {
			t.Errorf("stream differs from encoding/xml:\ninput: %q\n ours: %q\n  xml: %q", data, got, want)
		}
	})
}

// bangOK reports whether every "<!" in data opens a comment or CDATA section.
// Anything else ("<!ENTITY", "<!x", a truncated "<!") is treated leniently by
// fastsax but as a directive or error by encoding/xml.
func bangOK(data []byte) bool {
	for {
		i := bytes.Index(data, []byte("<!"))
		if i < 0 {
			return true
		}
		rest := data[i+2:]
		if !bytes.HasPrefix(rest, []byte("--")) && !bytes.HasPrefix(rest, []byte("[CDATA[")) {
			return false
		}
		data = rest
	}
}

var errValuelessAttr = errors.New("attribute without value")

// foldOurs converts a coalesced fastsax token stream into normalized
// comparison tokens: "start <local>", "attr <local>=<decoded>", "end <local>",
// "text <decoded>" (Text and CData coalesced, empties dropped), "comment
// <payload>", "pi <whitespace-stripped target+inst>". SelfClose expands to
// start+end; xmlns declarations are dropped. unescape reports whether the
// returned error came from entity decoding — a real finding when encoding/xml
// accepted the same input; other errors are by-design leniency gaps.
func foldOurs(toks []tok) (out []string, err error, unescape bool) {
	var text []byte
	flush := func() {
		if s := normNL(string(text)); s != "" {
			out = append(out, "text "+s)
		}
		text = text[:0]
	}
	for _, tk := range toks {
		switch tk.k {
		case KindText:
			// Copy before Unescape: it mutates its argument in place.
			u, uerr := Unescape([]byte(tk.v))
			if uerr != nil {
				return nil, uerr, true
			}
			text = append(text, u...)
		case KindCData:
			text = append(text, CData([]byte(tk.v))...)
		case KindStart, KindSelfClose:
			flush()
			name, rest := Name([]byte(tk.v))
			local := localName(string(name))
			out = append(out, "start "+local)
			for len(rest) > 0 {
				a, r, aerr := NextAttribute(rest)
				if aerr != nil {
					return nil, aerr, false
				}
				if len(a.Key) == 0 {
					break
				}
				if a.Value == nil {
					// Valueless attribute: encoding/xml rejects these, so if
					// we got here the tag boundaries disagree anyway; skip.
					return nil, errValuelessAttr, false
				}
				rest = r
				key := string(a.Key)
				if isXMLNSDecl(key) {
					continue
				}
				val, uerr := UnescapeAppend(nil, a.Value)
				if uerr != nil {
					return nil, uerr, true
				}
				out = append(out, "attr "+localName(key)+"="+string(val))
			}
			if tk.k == KindSelfClose {
				out = append(out, "end "+local)
			}
		case KindEnd:
			flush()
			name, _ := Name([]byte(tk.v))
			out = append(out, "end "+localName(string(name)))
		case KindComment:
			flush()
			out = append(out, "comment "+string(Comment([]byte(tk.v))))
		case KindPI:
			flush()
			out = append(out, "pi "+stripWS(tk.v[2:len(tk.v)-2]))
		case KindDocType:
			flush() // excluded by the caller; skip content if ever reached
		}
	}
	flush()
	return out, nil, false
}

// foldXML runs a full encoding/xml token walk and folds it into the same
// normalized token form as foldOurs. ok is false if the decoder errored.
func foldXML(data []byte) (out []string, ok bool) {
	dec := xml.NewDecoder(bytes.NewReader(data))
	var text []byte
	flush := func() {
		if s := normNL(string(text)); s != "" {
			out = append(out, "text "+s)
		}
		text = text[:0]
	}
	for {
		token, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, false
		}
		switch t := token.(type) {
		case xml.StartElement:
			flush()
			out = append(out, "start "+localName(t.Name.Local))
			for _, a := range t.Attr {
				// Skip namespace declarations: xmlns="..." keeps Local
				// "xmlns", xmlns:p="..." keeps the literal Space "xmlns".
				if a.Name.Space == "xmlns" || (a.Name.Space == "" && a.Name.Local == "xmlns") {
					continue
				}
				out = append(out, "attr "+localName(a.Name.Local)+"="+a.Value)
			}
		case xml.EndElement:
			flush()
			out = append(out, "end "+localName(t.Name.Local))
		case xml.CharData:
			text = append(text, t...)
		case xml.Comment:
			flush()
			out = append(out, "comment "+string(t))
		case xml.ProcInst:
			flush()
			out = append(out, "pi "+stripWS(t.Target+string(t.Inst)))
		case xml.Directive:
			flush() // excluded by the caller; skip content
		}
	}
	flush()
	return out, true
}

// isXMLNSDecl mirrors encoding/xml's QName splitting for attribute keys
// (nsname: strings.Cut at the first colon; an empty prefix or local part keeps
// the whole name un-split): only "xmlns" itself and "xmlns:<local>" with a
// non-empty local are namespace declarations. A degenerate key like "xmlns:"
// is an ordinary attribute to encoding/xml, so foldOurs must keep it too
// (found by FuzzDifferential on the merged tree,
// testdata/fuzz/.../60cedab71980e0dd). Keys with a second colon never reach
// the comparison: encoding/xml rejects them, so foldXML reports !ok.
func isXMLNSDecl(key string) bool {
	if key == "xmlns" {
		return true
	}
	rest, ok := strings.CutPrefix(key, "xmlns:")
	return ok && rest != ""
}

// localName returns the part of an element or attribute name after the last
// colon. encoding/xml splits at the first colon, so both sides are reduced
// with the same rule before comparison.
func localName(s string) string {
	if i := strings.LastIndexByte(s, ':'); i >= 0 {
		return s[i+1:]
	}
	return s
}

// normNL rewrites \r\n and bare \r to \n. Applied to coalesced text on both
// sides: encoding/xml does not renormalize CDATA payloads the way it does
// character data, and fastsax hands CDATA out raw.
func normNL(s string) string {
	if !strings.Contains(s, "\r") {
		return s
	}
	return strings.ReplaceAll(strings.ReplaceAll(s, "\r\n", "\n"), "\r", "\n")
}

// stripWS drops all XML whitespace. Used for PI comparison: encoding/xml
// splits target from instruction and eats the separating space, fastsax keeps
// the raw payload.
func stripWS(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\t', '\r', '\n':
			return -1
		}
		return r
	}, s)
}
