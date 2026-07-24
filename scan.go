// scanMarkup's markup dispatch (comment/CDATA/DOCTYPE/PI handling) is derived
// from gosax (https://github.com/orisano/gosax), Copyright (c) 2024
// Nao Yonashiro, licensed under the BSD 2-Clause License. See
// THIRD_PARTY_NOTICES.md.

package fastsax

import (
	"bytes"
	"encoding/binary"
	"math/bits"
)

// startNameStop ends a start-tag element name ('>', quotes, whitespace): one
// L1 table load per byte instead of four compares, so recording the name
// length costs the start-tag loop (almost) nothing. Whitespace entries must
// mirror the whitespace table so recorded name lengths stay byte-exact with
// the Name helper. End tags deliberately have no equivalent: adding a
// whitespace stop to their plain '>' loop measured ~2% on BenchmarkSequentialMem,
// and consumers rarely need end-tag names — KindEnd events record 0 and
// Batch.Name/Record.Name fall back to Name for them.
var startNameStop = [256]bool{
	'>': true, '"': true, '\'': true,
	' ': true, '\r': true, '\n': true, '\t': true,
}

// scanChunk scans data from the beginning, appending complete events to tape.
// It returns the extended tape, cut — the offset just past the last complete
// event — and an error for constructs that can never become valid.
//
// data[cut:] is always the (possibly empty) prefix of an incomplete markup
// construct; the caller carries it into the next window. Trailing text is
// never carried: it is emitted immediately, which is why KindText events may
// be split across windows.
//
// scanChunk is stateless across calls: because incomplete markup is carried,
// every window starts either at text or at a '<'.
//
// The common constructs (text, end tags, start tags) are scanned inline with
// plain byte loops: typical spans are 5-30 bytes, where the call overhead of
// bytes.IndexByte dominates its SIMD advantage. IndexByte is still used for
// long runs (text > 8 bytes probes, quoted attribute values). The inline
// paths are byte-exact with scanMarkup, which remains the reference
// implementation and handles the rare '!'/'?' constructs.
func scanChunk(data []byte, tape []Event) ([]Event, int, error) {
	p := 0
	for p < len(data) {
		if data[p] != '<' {
			// Text: tiny runs (single-digit field values) are common, so
			// probe 16 bytes inline — two unaligned SWAR words — before
			// paying the IndexByte call. Measured +10% isolated / +5% e2e w8
			// over the previous 8-byte byte-loop probe (docs/performance.md);
			// unlike per-span SIMD calls, the probe stays inline so short
			// spans pay no call overhead.
			q := p
			if q+16 <= len(data) {
				x := binary.LittleEndian.Uint64(data[q:]) ^ swarLT
				m := (x - swarLo) &^ x & swarHi
				if m != 0 {
					q += bits.TrailingZeros64(m) >> 3
				} else {
					x = binary.LittleEndian.Uint64(data[q+8:]) ^ swarLT
					m = (x - swarLo) &^ x & swarHi
					if m != 0 {
						q += 8 + bits.TrailingZeros64(m)>>3
					} else if r := bytes.IndexByte(data[q+16:], '<'); r < 0 {
						q = len(data)
					} else {
						q += 16 + r
					}
				}
			} else {
				for q < len(data) && data[q] != '<' {
					q++
				}
			}
			tape = append(tape, Event{Kind: KindText, Start: uint32(p), End: uint32(q)})
			p = q
			if p == len(data) {
				return tape, p, nil
			}
		}
		if p+1 == len(data) {
			return tape, p, nil // lone '<': needs more data
		}
		switch data[p+1] {
		case '/':
			// End tag: no quoting rules apply, a plain scan to '>' is exact.
			// No name length is recorded (see startNameStop).
			i := p + 2
			for i < len(data) && data[i] != '>' {
				i++
			}
			if i == len(data) {
				return tape, p, nil
			}
			tape = append(tape, Event{Kind: KindEnd, Start: uint32(p), End: uint32(i + 1)})
			p = i + 1
		case '!', '?':
			// Comments, CDATA, DOCTYPE, PIs: rare — defer to scanMarkup.
			n, kind, err := scanMarkup(data[p:])
			if err != nil {
				return tape, p, err
			}
			if n == 0 { // incomplete: needs more data
				return tape, p, nil
			}
			tape = append(tape, Event{Kind: kind, Start: uint32(p), End: uint32(p + n)})
			p += n
		default:
			// Start (or self-closing) tag: single pass to '>', skipping quoted
			// attribute values so '>' (or '<') inside them never terminates
			// the tag. The first loop also stops at whitespace — the end of
			// the element name — then falls into the plain tag loop for the
			// attribute region.
			i := p + 1
			for i < len(data) && !startNameStop[data[i]] {
				i++
			}
			nameEnd := i
			for {
				for i < len(data) && data[i] != '>' && data[i] != '"' && data[i] != '\'' {
					i++
				}
				if i == len(data) {
					return tape, p, nil
				}
				if data[i] == '>' {
					break
				}
				j := bytes.IndexByte(data[i+1:], data[i]) // skip quoted value
				if j < 0 {
					return tape, p, nil // unterminated quote: needs more data
				}
				i += j + 2
			}
			nl := nameEnd - (p + 1)
			if nameEnd < len(data) && (data[nameEnd] == '"' || data[nameEnd] == '\'') {
				nl = 0 // degenerate quote right after the name: leave it to Name
			}
			kind := KindStart
			if data[i-1] == '/' {
				kind = KindSelfClose
				if nameEnd == i {
					nl-- // attribute-free "<a/>": shed the trailing '/'
				}
			}
			tape = append(tape, Event{Kind: kind, nlen: packNameLen(nl),
				Start: uint32(p), End: uint32(i + 1)})
			p = i + 1
		}
	}
	return tape, p, nil
}

// nameLenTok computes the element-name length of one complete tag token,
// byte-exact with the inline computation in scanChunk (same stop set, same
// degenerate cases → 0). Used by scanRecord to fill Event.nlen for events
// that come from scanMarkup or the layout predictor.
func nameLenTok(tok []byte, kind Kind) int {
	switch kind {
	case KindStart, KindSelfClose:
		i := 1
		for i < len(tok) && tok[i] != '>' && tok[i] != '"' && tok[i] != '\'' && !whitespace[tok[i]] {
			i++
		}
		if i < len(tok) && (tok[i] == '"' || tok[i] == '\'') {
			return 0 // degenerate quote right after the name: leave it to Name
		}
		nl := i - 1
		if kind == KindSelfClose && i == len(tok)-1 {
			nl-- // attribute-free "<a/>": shed the trailing '/'
		}
		return nl
	}
	return 0
}

// scanMarkup scans one markup construct at data[0] == '<'.
// It returns the construct's length in bytes and its kind, or n == 0 if data
// does not yet contain the complete construct.
func scanMarkup(data []byte) (n int, kind Kind, err error) {
	if len(data) < 2 {
		return 0, 0, nil
	}
	switch data[1] {
	case '/':
		if i := bytes.IndexByte(data[2:], '>'); i >= 0 {
			return i + 3, KindEnd, nil
		}
		return 0, 0, nil
	case '?':
		if i := bytes.Index(data[2:], []byte("?>")); i >= 0 {
			return i + 4, KindPI, nil
		}
		return 0, 0, nil
	case '!':
		if len(data) < 3 {
			return 0, 0, nil
		}
		switch data[2] {
		case '[': // CDATA (lenient: the "CDATA[" spelling is not verified, as in gosax)
			if i := bytes.Index(data[3:], []byte("]]>")); i >= 0 {
				return i + 6, KindCData, nil
			}
			return 0, 0, nil
		case '-': // comment
			// The terminator search must start after the full "<!--" opener:
			// its trailing '-' must not double as the terminator's leading one
			// ("<!--->-->" is one comment "->", not comment "" plus text).
			if len(data) < 4 {
				return 0, 0, nil
			}
			if i := bytes.Index(data[4:], []byte("-->")); i >= 0 {
				return i + 7, KindComment, nil
			}
			return 0, 0, nil
		case 'D', 'd': // DOCTYPE, with bracket counting for internal subsets
			lv := 1
			for i := 2; i < len(data); i++ {
				switch data[i] {
				case '>':
					lv--
					if lv == 0 {
						return i + 1, KindDocType, nil
					}
				case '<':
					lv++
				}
			}
			return 0, 0, nil
		default:
			return 0, 0, errUnknownBang
		}
	default: // start tag (or self-closing tag)
		// Optimistic path: most start tags contain no quotes, so a single
		// IndexByte('>') finds the end. When quotes are present, skip each
		// quoted span and retry, so '>' inside attribute values never
		// terminates the tag.
		i := 1
		for {
			gt := bytes.IndexByte(data[i:], '>')
			if gt < 0 {
				return 0, 0, nil
			}
			seg := data[i : i+gt]
			q1 := bytes.IndexByte(seg, '"')
			q2 := bytes.IndexByte(seg, '\'')
			q := q1
			if q < 0 || (q2 >= 0 && q2 < q) {
				q = q2
			}
			if q < 0 {
				end := i + gt
				if data[end-1] == '/' {
					return end + 1, KindSelfClose, nil
				}
				return end + 1, KindStart, nil
			}
			quote := data[i+q]
			c := bytes.IndexByte(data[i+q+1:], quote)
			if c < 0 {
				return 0, 0, nil
			}
			i += q + 1 + c + 1
		}
	}
}
