package fastsax

import (
	"bytes"
	"math/rand"
	"testing"
)

// TestScanKernSSE2AVX2Parity compares the dispatched kernel with the
// force-SSE2 leg and the generic reference. On amd64 with AVX2 this is the
// AVX2-vs-SSE2-vs-generic triple parity; elsewhere the knob is a no-op and
// the test degenerates to (already covered) dispatched-vs-generic parity.
func TestScanKernSSE2AVX2Parity(t *testing.T) {
	defer scankernForceSSE2.Store(false)
	rng := rand.New(rand.NewSource(7))
	buf := make([]byte, 4096+65)
	for i := range buf {
		if rng.Intn(12) == 0 {
			buf[i] = '<'
		} else {
			buf[i] = byte('a' + rng.Intn(20))
		}
	}
	for _, n := range []int{0, 1, 63, 64, 65, 127, 128, 129, 191, 192, 300, 1024, 4096, len(buf)} {
		for _, off := range []int{0, 1, 7, 13} {
			if off+n > len(buf) {
				continue
			}
			b := buf[off : off+n]
			words := n / 64
			want := make([]uint64, words)
			maskByte64Generic(b, '<', want)

			scankernForceSSE2.Store(false)
			got := make([]uint64, words)
			maskByte64(b, '<', got)
			if !equalU64(got, want) {
				t.Fatalf("dispatched kernel diverges from generic at n=%d off=%d", n, off)
			}

			scankernForceSSE2.Store(true)
			got2 := make([]uint64, words)
			maskByte64(b, '<', got2)
			if !equalU64(got2, want) {
				t.Fatalf("forced-SSE2 kernel diverges from generic at n=%d off=%d", n, off)
			}
		}
	}
	// Whole-pipeline sanity with the knob flipped: same events either way.
	scankernForceSSE2.Store(true)
	toksA, errA := collect([]byte(sampleDoc), WithPrefetch(false))
	scankernForceSSE2.Store(false)
	toksB, errB := collect([]byte(sampleDoc), WithPrefetch(false))
	if (errA == nil) != (errB == nil) || !toksEqual(toksA, toksB) {
		t.Fatal("event stream differs between forced-SSE2 and dispatched kernels")
	}
	_ = bytes.MinRead
}

func equalU64(a, b []uint64) bool {
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
