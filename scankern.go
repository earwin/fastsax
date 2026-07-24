package fastsax

import (
	"encoding/binary"
	"sync/atomic"
)

// Per-arch byte-equality mask kernels (see docs/design.md). One kernel exists:
// maskByte64, which turns 64 input bytes into one movemask word. It is used in
// exactly one place — buildHitMask, once per skeletonFast window — where the
// cost amortizes over a whole-window pass. Per-short-span kernel calls were
// measured and rejected (-38%): do not add call sites inside the byte-level
// scan loops.
//
// Dispatch is resolved at build time by architecture (scankern_arm64.s NEON,
// scankern_amd64.s SSE2/AVX2, scankern_generic.go pure-Go SWAR). The only
// runtime choice is on amd64, where AVX2 is selected once at init when the
// CPU and OS support it (cpuHasAVX2) and SSE2 remains the baseline fallback.

// scankernForceGeneric routes buildHitMask through the portable pure-Go
// kernel instead of the per-arch assembly. Test-only knob (mirrors
// skelForceFull): parity tests and fuzz targets compare the two paths.
var scankernForceGeneric atomic.Bool

// scankernForceSSE2 pins the amd64 dispatch to the SSE2 kernel even when
// AVX2 is available. Test-only knob for SSE2-vs-AVX2 parity and benches on
// AVX2 hardware; a no-op on other architectures (declared portably so tests
// compile everywhere).
var scankernForceSSE2 atomic.Bool

// maskByte64Generic is the portable reference kernel: fills masks[i] with the
// movemask of (b[64i+j] == c) for every complete 64-byte block of b — bit j of
// masks[i] is set iff b[64i+j] == c. len(masks) must be >= len(b)/64; the
// ragged tail block is NOT covered (callers finish it in Go). Byte-exact
// contract shared with the assembly kernels; ~3.5 GB/s (8-byte SWAR compare +
// multiply compress).
func maskByte64Generic(b []byte, c byte, masks []uint64) {
	n := len(b) / 64
	masks = masks[:n]
	pat := swarLo * uint64(c)
	const lo7 = ^uint64(swarHi) // 0x7f7f... : per-byte low-7-bit mask
	for i := range masks {
		blk := b[i*64 : i*64+64]
		var m uint64
		for w := 0; w < 64; w += 8 {
			x := binary.LittleEndian.Uint64(blk[w:]) ^ pat
			// Exact carry-free zero-byte test (Hacker's Delight): the scan
			// loops' cheaper (x-lo)&^x&hi form is NOT usable here — its
			// cross-byte borrow flags a byte equal to c^1 right after a true
			// match, fine when only the first match matters, wrong for a
			// full bitmask (caught by TestScanKernParity's randomized leg).
			hi := ^((x&lo7 + lo7) | x | lo7)
			// Compress the 0x80-per-matching-byte word to 8 mask bits.
			m |= uint64(byte((hi>>7)*0x0102040810204080>>56)) << w
		}
		masks[i] = m
	}
}

// buildHitMask (re)builds fs.mask as the '<' bitmask of win: one uint64 per
// 64 window bytes, ragged tail word built in Go. Called once per skeletonFast
// window walk. It must run on every call — windows are pooled, reused buffers,
// so caching a mask by buffer identity would read recycled bytes (see the
// fastState.mask comment).
func (fs *fastState) buildHitMask(win []byte) {
	nw := (len(win) + 63) / 64
	if cap(fs.mask) < nw {
		fs.mask = make([]uint64, nw)
	}
	fs.mask = fs.mask[:nw]
	full := len(win) / 64
	if scankernForceGeneric.Load() {
		maskByte64Generic(win, '<', fs.mask[:full])
	} else {
		maskByte64(win, '<', fs.mask[:full])
	}
	if t := full * 64; t < len(win) {
		var m uint64
		for j, c := range win[t:] {
			if c == '<' {
				m |= 1 << j
			}
		}
		fs.mask[full] = m
	}
}
