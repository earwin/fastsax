//go:build arm64

package fastsax

// hasScanKern reports (informationally) that maskByte64 is backed by
// architecture-specific assembly on this build.
const hasScanKern = true

// maskByte64 fills masks[i] with the movemask of (b[64i+j] == c) for every
// complete 64-byte block of b: bit j of masks[i] is set iff b[64i+j] == c.
// len(masks) must be >= len(b)/64; the ragged tail block is NOT covered
// (callers finish it in Go). Byte-exact with maskByte64Generic.
//
// NEON implementation (scankern_arm64.s): CMEQ x4 → AND bit-pattern → 3×ADDP
// fold per 64-byte block (simdjson-style), ~51 GB/s on M1 Pro. ABI0 with
// explicit FP offsets — documented choice: simplest correct ABI, NOSPLIT, no
// stack; call overhead is irrelevant at one call per ~1MB window.
//
//go:noescape
func maskByte64(b []byte, c byte, masks []uint64)
