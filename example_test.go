// Runnable godoc examples for fastsax, written from the point of view of a
// first-time user. They live in the external package fastsax_test so every
// snippet shows exactly what a real consumer writes, including the import.
//
// Two contracts come up again and again below, so read them once here:
//
//  1. Lifetimes. Every []byte the parser hands out (Batch.Data, Record.Data
//     and anything sliced from them) aliases an internal buffer that is
//     recycled. A Batch is valid only until the next call to Next/Reset/Close;
//     a Record only for the duration of the handler call. Copy what you keep.
//  2. Nothing is decoded for you. Attribute values come without quotes but
//     with entities intact; text comes exactly as it appears in the input.
//     Decode on demand with Unescape (in place) or UnescapeAppend (copying).
package fastsax_test

import (
	"fmt"
	"io"
	"strings"

	"github.com/Muravlev/fastsax"
)

// ExampleParser_basic walks every event of a small document and prints its
// kind and, for tag events, its name. This is the core sequential loop: call
// Next until io.EOF, then range over the batch's event tape.
func ExampleParser_basic() {
	doc := `<doc><greeting>hello</greeting><empty/></doc>`

	// WithPrefetch(false) keeps the example single-goroutine; the default
	// (prefetch on) overlaps reading with parsing and is what you want for
	// real files.
	p, err := fastsax.NewParser(strings.NewReader(doc), fastsax.WithPrefetch(false))
	if err != nil {
		panic(err)
	}
	defer p.Close()

	for {
		b, err := p.Next() // one batch of events over one window of input
		if err == io.EOF {
			break // clean end of input
		}
		if err != nil {
			panic(err) // e.g. *fastsax.SyntaxError with an absolute offset
		}
		for _, e := range b.Events {
			switch e.Kind {
			case fastsax.KindStart, fastsax.KindEnd, fastsax.KindSelfClose:
				// b.Name reads the element name straight off the event tape.
				fmt.Printf("%s %s\n", e.Kind, b.Name(e))
			case fastsax.KindText:
				fmt.Printf("%s %q\n", e.Kind, b.Bytes(e))
			}
		}
	}
	// Output:
	// Start doc
	// Start greeting
	// Text "hello"
	// End greeting
	// SelfClose empty
	// End doc
}

// ExampleParser_attributes shows the attribute contract: Name returns the
// element name plus the raw attribute region, NextAttribute cuts one key/value
// pair at a time, values arrive without their surrounding quotes but with
// entities still encoded, and Unescape decodes a value in place.
func ExampleParser_attributes() {
	doc := `<link href="a.html?x=1&amp;y=2" title='Tom &amp; Jerry'/>`

	p, _ := fastsax.NewParser(strings.NewReader(doc), fastsax.WithPrefetch(false))
	defer p.Close()

	b, _ := p.Next()
	for _, e := range b.Events {
		if e.Kind != fastsax.KindSelfClose && e.Kind != fastsax.KindStart {
			continue
		}
		// We need the attribute region here, so we use the Name helper on the
		// raw tag bytes; its second return is everything after the name.
		// (If you only need the element name, prefer b.Name(e).)
		name, attrs := fastsax.Name(b.Bytes(e))
		fmt.Printf("element %s\n", name)

		for len(attrs) > 0 {
			a, rest, err := fastsax.NextAttribute(attrs)
			if err != nil {
				panic(err)
			}
			if len(a.Key) == 0 {
				break // empty key = no attributes remain
			}
			attrs = rest

			// a.Value is the raw bytes between the quotes — note there are no
			// quotes and the &amp; entities are still there.
			fmt.Printf("  %s = %q (raw)\n", a.Key, a.Value)

			// Unescape decodes in place, mutating the batch buffer. That is
			// allowed (each value is a disjoint view), but do it after you are
			// done with the raw bytes.
			decoded, err := fastsax.Unescape(a.Value)
			if err != nil {
				panic(err)
			}
			fmt.Printf("  %s = %q (decoded)\n", a.Key, decoded)
		}
	}
	// Output:
	// element link
	//   href = "a.html?x=1&amp;y=2" (raw)
	//   href = "a.html?x=1&y=2" (decoded)
	//   title = "Tom &amp; Jerry" (raw)
	//   title = "Tom & Jerry" (decoded)
}

// ExampleParser_text demonstrates the text contract: a single text node MAY be
// delivered as several consecutive KindText events when it crosses a window
// boundary. Consumers that need whole nodes must coalesce the pieces — and
// must decode entities only AFTER coalescing, because an entity like &amp;
// can itself be split down the middle.
//
// A tiny WithChunkSize forces the split here; with the default 1MB windows
// splits are rare but just as legal.
func ExampleParser_text() {
	long := strings.Repeat("all work and no play makes jack a dull boy ", 3) +
		"&amp; then some" // an entity near the end, possibly split by chunking
	doc := "<log><entry>" + long + "</entry></log>"

	// 64 bytes is the minimum window size; the ~150-byte text above therefore
	// spans several windows and arrives in several pieces.
	p, _ := fastsax.NewParser(strings.NewReader(doc),
		fastsax.WithChunkSize(64), fastsax.WithPrefetch(false))
	defer p.Close()

	var text []byte // our own buffer — safe to keep and mutate
	pieces := 0
	for {
		b, err := p.Next()
		if err != nil {
			break
		}
		for _, e := range b.Events {
			if e.Kind == fastsax.KindText {
				pieces++
				// Copy the piece out: b.Bytes(e) dies with this batch.
				text = append(text, b.Bytes(e)...)
			}
		}
	}

	// Only now, with the full node in hand, is it safe to decode entities.
	decoded, err := fastsax.Unescape(text)
	if err != nil {
		panic(err)
	}
	fmt.Println("split across batches:", pieces > 1)
	fmt.Println("tail:", string(decoded[len(decoded)-15:]))
	// Output:
	// split across batches: true
	// tail: boy & then some
}

// ExampleBatch_Name shows that Batch.Name is equivalent to the name half of
// the Name helper applied to the raw tag bytes — but cheaper: for start and
// self-close tags the scanner already recorded the name length on the event
// tape, so Batch.Name slices it out without re-scanning the tag. Prefer it
// whenever you do not need the attribute region.
func ExampleBatch_Name() {
	doc := `<city id="42" name="Oslo"></city>`
	p, _ := fastsax.NewParser(strings.NewReader(doc), fastsax.WithPrefetch(false))
	defer p.Close()

	b, _ := p.Next()
	for _, e := range b.Events {
		fromTape := b.Name(e)                   // preferred: tape-recorded length
		fromScan, _ := fastsax.Name(b.Bytes(e)) // re-derives the name each call
		fmt.Printf("%s: %s == %s: %v\n", e.Kind, fromTape, fromScan,
			string(fromTape) == string(fromScan))
	}
	// Output:
	// Start: city == city: true
	// End: city == city: true
}

// ExampleParseRecords is the minimal parallel-mode loop: cut the stream into
// <city> elements and pull one field out of each. Workers: 1 parses inline on
// the calling goroutine (no concurrency), which keeps this example's output
// deterministic; real workloads leave Workers at 0 (= GOMAXPROCS).
func ExampleParseRecords() {
	doc := `<cities>
		<city><name>Oslo</name><pop>717710</pop></city>
		<city><name>Bergen</name><pop>291940</pop></city>
	</cities>`

	records := 0
	err := fastsax.ParseRecords(strings.NewReader(doc),
		fastsax.RecordConfig{Tag: []byte("city"), Workers: 1},
		func(worker int, rec fastsax.Record) error {
			records++
			// rec.Events is the complete tape for this one record; offsets
			// index rec.Data. Find <name> and print the text that follows it.
			for i, e := range rec.Events {
				if e.Kind == fastsax.KindStart && string(rec.Name(e)) == "name" {
					fmt.Printf("record %d: %s\n", rec.Index, rec.Bytes(rec.Events[i+1]))
					break
				}
			}
			return nil // a non-nil error would cancel the whole run
		})
	if err != nil {
		panic(err)
	}
	fmt.Println("records:", records)
	// Output:
	// record 0: Oslo
	// record 1: Bergen
	// records: 2
}

// ExampleParseRecords_ordered uses Ordered mode: records are still parsed in
// parallel across Workers goroutines, but the handler is called from a single
// goroutine, in document order, always with worker == 0. This is the easy way
// to combine parallel parsing with order-dependent output.
func ExampleParseRecords_ordered() {
	var doc strings.Builder
	doc.WriteString("<root>")
	for i := 0; i < 5; i++ {
		fmt.Fprintf(&doc, "<item><n>%d</n></item>", i)
	}
	doc.WriteString("</root>")

	err := fastsax.ParseRecords(strings.NewReader(doc.String()),
		fastsax.RecordConfig{Tag: []byte("item"), Workers: 4, Ordered: true},
		func(worker int, rec fastsax.Record) error {
			// Delivery is sequenced, so plain (non-atomic) code is fine here
			// and rec.Index arrives strictly increasing.
			fmt.Printf("worker %d, index %d\n", worker, rec.Index)
			return nil
		})
	if err != nil {
		panic(err)
	}
	// Output:
	// worker 0, index 0
	// worker 0, index 1
	// worker 0, index 2
	// worker 0, index 3
	// worker 0, index 4
}

// ExampleParseRecords_autodetect leaves Tag nil, letting ParseRecords pick the
// first repeating depth-1 element from the first input window. OnDetect
// reports the choice before parsing starts.
func ExampleParseRecords_autodetect() {
	doc := `<root><city>Oslo</city><city>Bergen</city><city>Trondheim</city></root>`

	count := 0
	err := fastsax.ParseRecords(strings.NewReader(doc),
		fastsax.RecordConfig{
			Tag:     nil, // ask the parser to find the record element
			Workers: 1,
			OnDetect: func(tag []byte) {
				fmt.Println("detected record tag:", string(tag))
			},
		},
		func(worker int, rec fastsax.Record) error {
			count++
			return nil
		})
	if err != nil {
		panic(err)
	}
	fmt.Println("records:", count)
	// Output:
	// detected record tag: city
	// records: 3
}

// ExampleParseRecords_retention shows the copy-what-you-keep contract. Record
// buffers are recycled as soon as the handler returns, so retaining a slice of
// rec.Data is a use-after-reuse bug: the bytes will be silently overwritten by
// a later record. Retain copies instead.
func ExampleParseRecords_retention() {
	doc := `<root><w>alpha</w><w>beta</w><w>gamma</w></root>`

	var words [][]byte // survives the run: every element is a private copy
	err := fastsax.ParseRecords(strings.NewReader(doc),
		fastsax.RecordConfig{Tag: []byte("w"), Workers: 1},
		func(worker int, rec fastsax.Record) error {
			for _, e := range rec.Events {
				if e.Kind != fastsax.KindText {
					continue
				}
				// WRONG: words = append(words, rec.Bytes(e))
				// That stores a view into rec.Data, which is recycled for the
				// next record — all entries would end up aliasing the same
				// buffer with whatever bytes the last record left there.
				//
				// RIGHT: copy the value out. UnescapeAppend both copies and
				// decodes entities, so it is the natural retention primitive
				// (plain append([]byte(nil), b...) works when no decoding is
				// wanted).
				w, err := fastsax.UnescapeAppend(nil, rec.Bytes(e))
				if err != nil {
					return err
				}
				words = append(words, w)
			}
			return nil
		})
	if err != nil {
		panic(err)
	}
	for _, w := range words {
		fmt.Println(string(w))
	}
	// Output:
	// alpha
	// beta
	// gamma
}

// ExampleName extracts an element name and the raw attribute region from a
// tag's bytes. The angle brackets and any trailing "/" are stripped for you.
func ExampleName() {
	name, rest := fastsax.Name([]byte(`<city id="42" name="Oslo">`))
	fmt.Printf("name: %s\n", name)
	fmt.Printf("rest: %s\n", rest)
	// Output:
	// name: city
	// rest: id="42" name="Oslo"
}

// ExampleNextAttribute iterates the attribute region of a start tag. Values
// come without quotes; a zero-length key ends the iteration.
func ExampleNextAttribute() {
	rest := []byte(`id="42" name='Oslo'`)
	for len(rest) > 0 {
		a, tail, err := fastsax.NextAttribute(rest)
		if err != nil {
			panic(err)
		}
		if len(a.Key) == 0 {
			break
		}
		fmt.Printf("%s => %s\n", a.Key, a.Value)
		rest = tail
	}
	// Output:
	// id => 42
	// name => Oslo
}

// ExampleUnescapeAppend decodes entities into a fresh buffer, leaving the
// source untouched — use it when the decoded text must outlive its batch or
// record (Unescape, by contrast, rewrites its argument in place).
func ExampleUnescapeAppend() {
	src := []byte("caf&#233; &lt;b&gt; &amp; more")
	out, err := fastsax.UnescapeAppend(nil, src)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(out))
	fmt.Println(string(src)) // src is unchanged
	// Output:
	// café <b> & more
	// caf&#233; &lt;b&gt; &amp; more
}
