// Command countfields streams an XML file with the sequential Parser API and
// tallies how often each element name occurs, printing the most frequent ones.
//
// Usage:
//
//	countfields [file.xml]
//
// With no argument it runs over a small embedded document, so the program is
// deterministic out of the box.
//
// The interesting part is what it does NOT do: it never buffers the document.
// Each call to Next yields one batch of events over one ~1MB window of input,
// and everything sliced out of that batch (b.Name, b.Bytes) is only valid
// until the following Next call. The tally survives batches because Go's
// string(...) conversion copies the bytes — that conversion IS our
// copy-what-you-keep step.
package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/Muravlev/fastsax"
)

// fallback keeps the program runnable (and its output stable) with no args.
const fallback = `<library>
	<book id="1"><title>Dune</title><author>Herbert</author></book>
	<book id="2"><title>Solaris</title><author>Lem</author></book>
	<book id="3"><title>Blindsight</title><author>Watts</author></book>
</library>`

func main() {
	var in io.Reader = strings.NewReader(fallback)
	if len(os.Args) > 1 {
		f, err := os.Open(os.Args[1])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer f.Close()
		in = f
	}

	// Default options are the right call for files: 1MB windows and a
	// background prefetch goroutine that overlaps disk reads with parsing.
	p, err := fastsax.NewParser(in)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	// Close releases the prefetch goroutine; without it the goroutine would
	// linger until its in-flight Read returns.
	defer p.Close()

	counts := make(map[string]int)
	for {
		b, err := p.Next()
		if err == io.EOF {
			break // normal termination: the whole input has been consumed
		}
		if err != nil {
			// SyntaxError carries the absolute byte offset of the problem,
			// which %v includes — handy for opening the file at that spot.
			fmt.Fprintln(os.Stderr, "parse error:", err)
			os.Exit(1)
		}
		for _, e := range b.Events {
			// Count each element once: at its start tag. Self-closing tags
			// (<foo/>) are a single event, so they are counted here too.
			if e.Kind != fastsax.KindStart && e.Kind != fastsax.KindSelfClose {
				continue
			}
			// b.Name(e) slices the name straight off the event tape (the
			// scanner recorded its length) — cheaper than re-scanning the tag
			// bytes with the Name helper. The slice aliases the batch buffer;
			// string(...) copies it, so the map key stays valid forever.
			counts[string(b.Name(e))]++
		}
	}

	// Deterministic report: by count descending, then name ascending.
	names := make([]string, 0, len(counts))
	for n := range counts {
		names = append(names, n)
	}
	sort.Slice(names, func(i, j int) bool {
		if counts[names[i]] != counts[names[j]] {
			return counts[names[i]] > counts[names[j]]
		}
		return names[i] < names[j]
	})
	if len(names) > 10 {
		names = names[:10]
	}
	for _, n := range names {
		fmt.Printf("%8d  %s\n", counts[n], n)
	}
}
