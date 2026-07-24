//go:build !arm64 && !amd64

package fastsax

// hasScanKern reports (informationally) that maskByte64 has no assembly
// backing on this build and falls back to the portable SWAR kernel.
const hasScanKern = false

// maskByte64 is the portable fallback: identical contract to the assembly
// kernels (see scankern_arm64.go), implemented by maskByte64Generic
// (scankern.go). Still ~2x the effective candidate-search rate of the old
// per-8-byte SWAR sweep it replaced, so non-asm architectures do not regress.
func maskByte64(b []byte, c byte, masks []uint64) {
	maskByte64Generic(b, c, masks)
}
