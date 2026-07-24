//go:build amd64

package fastsax

// hasScanKern reports (informationally) that maskByte64 is backed by
// architecture-specific assembly on this build.
const hasScanKern = true

// useAVX2 is resolved once at init: AVX2 requires CPU support (CPUID.7:EBX
// bit 5), AVX+OSXSAVE (CPUID.1:ECX bits 28/27) and OS-enabled XMM+YMM state
// (XGETBV(0) & 6 == 6). When false — including under Rosetta 2 on Apple
// Silicon, which does not translate AVX2 — every call takes the SSE2 kernel.
var useAVX2 = cpuHasAVX2()

// maskByte64 fills masks[i] with the movemask of (b[64i+j] == c) for every
// complete 64-byte block of b: bit j of masks[i] is set iff b[64i+j] == c.
// len(masks) must be >= len(b)/64; the ragged tail block is NOT covered
// (callers finish it in Go). Byte-exact with maskByte64Generic.
//
// Dispatch: AVX2 (2x VPCMPEQB/VPMOVMSKB on 32-byte lanes, measured on
// Ryzen 7 5800H — see docs/performance.md) when available, SSE2 (4x 16-byte
// lanes, amd64 baseline) otherwise. One dispatch branch per window-sized
// call; scankernForceSSE2 pins SSE2 for parity tests on AVX2 hardware.
func maskByte64(b []byte, c byte, masks []uint64) {
	if useAVX2 && !scankernForceSSE2.Load() {
		maskByte64AVX2(b, c, masks)
		return
	}
	maskByte64SSE2(b, c, masks)
}

// Assembly kernels (scankern_amd64.s), ABI0 with explicit FP offsets
// (documented choice: simplest correct ABI, NOSPLIT, no stack; one call per
// ~1MB window).
//
//go:noescape
func maskByte64SSE2(b []byte, c byte, masks []uint64)

//go:noescape
func maskByte64AVX2(b []byte, c byte, masks []uint64)

// cpuidx executes CPUID with the given EAX/ECX inputs.
func cpuidx(eaxIn, ecxIn uint32) (eax, ebx, ecx, edx uint32)

// xgetbv0 reads XCR0 (requires OSXSAVE, checked before calling).
func xgetbv0() uint64

func cpuHasAVX2() bool {
	maxID, _, _, _ := cpuidx(0, 0)
	if maxID < 7 {
		return false
	}
	_, _, ecx1, _ := cpuidx(1, 0)
	const osxsave = 1 << 27
	const avx = 1 << 28
	if ecx1&(osxsave|avx) != osxsave|avx {
		return false
	}
	if xgetbv0()&6 != 6 { // XMM and YMM state enabled by the OS
		return false
	}
	_, ebx7, _, _ := cpuidx(7, 0)
	return ebx7&(1<<5) != 0
}
