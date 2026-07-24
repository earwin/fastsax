package fastsax

import (
	"math/rand"
	"testing"
)

// BenchmarkMaskByte64 races the dispatched kernel, the forced-SSE2 leg (amd64)
// and the generic SWAR reference over a 1MB window — the production call shape
// (one buildHitMask per skeletonFast window).
func BenchmarkMaskByte64(b *testing.B) {
	rng := rand.New(rand.NewSource(3))
	buf := make([]byte, 1<<20)
	for i := range buf {
		if rng.Intn(12) == 0 {
			buf[i] = '<'
		} else {
			buf[i] = byte('a' + rng.Intn(20))
		}
	}
	masks := make([]uint64, len(buf)/64)
	defer scankernForceSSE2.Store(false)
	for _, mode := range []struct {
		name      string
		forceSSE2 bool
		generic   bool
	}{{"dispatched", false, false}, {"sse2", true, false}, {"generic", false, true}} {
		b.Run(mode.name, func(b *testing.B) {
			scankernForceSSE2.Store(mode.forceSSE2)
			b.SetBytes(int64(len(buf)))
			for b.Loop() {
				if mode.generic {
					maskByte64Generic(buf, '<', masks)
				} else {
					maskByte64(buf, '<', masks)
				}
			}
		})
	}
}
