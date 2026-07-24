// Command parallel demonstrates ParseRecords, the parallel record-mode API,
// over a document generated in memory: worker fan-out, safe shared state,
// error propagation, and the RecordStats counters.
//
// The one thing to internalize before copying this pattern: with Workers != 1
// and Ordered == false, the handler runs CONCURRENTLY on several goroutines.
// Anything it touches that is shared must be synchronized, and every byte of
// rec it wants to keep must be copied before returning (the record buffer is
// recycled the moment the handler returns).
package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	"github.com/Muravlev/fastsax"
)

const numRecords = 10000

// gen builds a well-formed document with numRecords <event> elements. Using an
// in-memory reader keeps the example self-contained; ParseRecords takes any
// io.Reader (an *os.File is the usual case).
func gen() *strings.Reader {
	var b strings.Builder
	b.WriteString("<events>\n")
	for i := 0; i < numRecords; i++ {
		fmt.Fprintf(&b, "<event><seq>%d</seq><level>%s</level></event>\n",
			i, []string{"info", "warn", "error"}[i%3])
	}
	b.WriteString("</events>\n")
	return strings.NewReader(b.String())
}

func main() {
	// Shared state, written from many handler goroutines at once, so it must
	// be atomic (or mutex-guarded). Plain ints here would be a data race.
	var total, errorLevel atomic.Int64

	// Stats is optional: pass a *RecordStats and the run fills in scheduler
	// counters you can inspect afterwards.
	var stats fastsax.RecordStats

	err := fastsax.ParseRecords(gen(), fastsax.RecordConfig{
		Tag:     []byte("event"), // byte-exact record element name
		Workers: 0,               // 0 = runtime.GOMAXPROCS(0) parallel workers
		Stats:   &stats,
	}, func(worker int, rec fastsax.Record) error {
		// This closure runs concurrently; `worker` says which goroutine
		// (0..Workers-1) is calling — useful for per-worker scratch buffers.
		total.Add(1)

		// Walk the record's own event tape. Offsets index rec.Data, and both
		// are valid ONLY until this function returns — we only read here, so
		// no copies are needed.
		for i, e := range rec.Events {
			if e.Kind != fastsax.KindStart || string(rec.Name(e)) != "level" {
				continue
			}
			// The event after <level> is its text (this document has no
			// comments or CDATA inside fields, so index i+1 is safe).
			if string(rec.Bytes(rec.Events[i+1])) == "error" {
				errorLevel.Add(1)
			}
			// Returning a non-nil error from any handler cancels the whole
			// run: dispatch stops, all goroutines are joined, and ParseRecords
			// returns that error. Example (disabled):
			//
			//	return fmt.Errorf("record %d: bad level", rec.Index)
			break
		}
		return nil
	})
	if err != nil {
		// Handler errors, XML syntax errors and reader errors all surface
		// here; errors.Is/As work as usual.
		var serr *fastsax.SyntaxError
		if errors.As(err, &serr) {
			fmt.Fprintf(os.Stderr, "malformed XML at byte %d: %v\n", serr.Offset, err)
		} else {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(1)
	}

	fmt.Printf("records: %d, level=error: %d\n", total.Load(), errorLevel.Load())

	// The stats are exact once ParseRecords has returned. IOBound reports
	// whether workers were mostly starved for input — a hint that fewer
	// workers would parse the same stream just as fast.
	fmt.Printf("dispatches: %d (empty-queue: %d), io-bound: %v\n",
		stats.Dispatches.Load(), stats.EmptyDispatch.Load(), stats.IOBound())
}
