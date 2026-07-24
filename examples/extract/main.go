// Command extract shows the pattern a real consumer needs most often: turn
// each record of a large document into a Go struct — correctly, i.e. without
// accidentally retaining pointers into the parser's recycled buffers.
//
// Rules applied below:
//
//   - rec.Data (and every slice cut from it) is valid only while the handler
//     runs. A struct field must therefore hold a COPY: string(...) conversions
//     and UnescapeAppend(nil, ...) both copy.
//   - Entities (&amp; etc.) are not decoded by the parser. UnescapeAppend
//     decodes into a fresh buffer, giving copy + decode in one step.
//   - Ordered: true delivers records in document order from one goroutine, so
//     appending to a plain slice needs no locking.
package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/Muravlev/fastsax"
)

// The kind of feed this pattern targets: repeated <city> records with an
// attribute on the record tag, child fields, and entities in the text.
const doc = `<geo>
	<city id="osl"><name>Oslo</name><note>capital &amp; largest city</note></city>
	<city id="ber"><name>Bergen</name><note>rain &#x2614; guaranteed</note></city>
	<city id="trd"><name>Trondheim</name><note>tech &amp; cathedral</note></city>
</geo>`

// City owns all of its memory: string fields never alias parser buffers.
type City struct {
	ID   string // from the id="..." attribute of <city>
	Name string
	Note string // entity-decoded
}

func main() {
	var cities []City // filled in document order thanks to Ordered: true

	err := fastsax.ParseRecords(strings.NewReader(doc), fastsax.RecordConfig{
		Tag:     []byte("city"),
		Workers: 0,    // parse in parallel on all cores...
		Ordered: true, // ...but deliver sequentially, in document order
	}, func(worker int, rec fastsax.Record) error {
		var c City

		// Events[0] is always the record's own start tag; its attributes are
		// in the raw tag bytes. Name gives us the attribute region and
		// NextAttribute cuts it into pairs (values arrive without quotes).
		_, attrs := fastsax.Name(rec.Bytes(rec.Events[0]))
		for len(attrs) > 0 {
			a, rest, err := fastsax.NextAttribute(attrs)
			if err != nil {
				return err // cancels the whole ParseRecords run
			}
			if len(a.Key) == 0 {
				break
			}
			attrs = rest
			if string(a.Key) == "id" {
				c.ID = string(a.Value) // string(...) copies: safe to keep
			}
		}

		// Walk the child fields. We remember the last-seen start-tag name and
		// assign the text event that follows it. This tolerates whitespace
		// text between elements (there is none inside <city> here, but the
		// pattern survives pretty-printed input).
		var field string
		for _, e := range rec.Events {
			switch e.Kind {
			case fastsax.KindStart:
				field = string(rec.Name(e))
			case fastsax.KindText:
				switch field {
				case "name":
					c.Name = string(rec.Bytes(e))
				case "note":
					// Copy AND decode entities in one step. The nil dst makes
					// UnescapeAppend allocate a fresh buffer; rec.Data itself
					// is left untouched (unlike in-place Unescape).
					out, err := fastsax.UnescapeAppend(nil, rec.Bytes(e))
					if err != nil {
						return err
					}
					c.Note = string(out)
				}
			case fastsax.KindEnd:
				field = "" // don't attach stray text to the previous field
			}
		}

		cities = append(cities, c) // single delivery goroutine: no lock needed
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	for _, c := range cities {
		fmt.Printf("%-4s %-10s %s\n", c.ID, c.Name, c.Note)
	}
}
