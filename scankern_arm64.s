// NEON byte-equality mask kernel; see scankern_arm64.go for the contract and
// docs/performance.md for the measurements. ABI0 (explicit FP offsets),
// NOSPLIT, no stack frame.

#include "textflag.h"

// func maskByte64(b []byte, c byte, masks []uint64)
// Args: b_base+0, b_len+8, b_cap+16, c+24, masks_base+32, masks_len+40,
// masks_cap+48.
// For each complete 64-byte block of b, produce a 64-bit movemask of
// (byte == c) via CMEQ x4 -> AND bitpattern -> 3x ADDP fold (simdjson-style)
// and store it to the next masks word.
TEXT ·maskByte64(SB), NOSPLIT, $0-56
	MOVD	b_base+0(FP), R0
	MOVD	b_len+8(FP), R1
	MOVBU	c+24(FP), R3
	MOVD	masks_base+32(FP), R2
	VDUP	R3, V8.B16
	MOVD	$0x8040201008040201, R4
	VDUP	R4, V9.D2
loop64:
	CMP	$64, R1
	BLT	done
	VLD1.P	64(R0), [V0.B16, V1.B16, V2.B16, V3.B16]
	VCMEQ	V8.B16, V0.B16, V0.B16
	VCMEQ	V8.B16, V1.B16, V1.B16
	VCMEQ	V8.B16, V2.B16, V2.B16
	VCMEQ	V8.B16, V3.B16, V3.B16
	VAND	V9.B16, V0.B16, V0.B16
	VAND	V9.B16, V1.B16, V1.B16
	VAND	V9.B16, V2.B16, V2.B16
	VAND	V9.B16, V3.B16, V3.B16
	VADDP	V1.B16, V0.B16, V0.B16	// low = pairs(V0), high = pairs(V1)
	VADDP	V3.B16, V2.B16, V2.B16
	VADDP	V2.B16, V0.B16, V0.B16
	VADDP	V0.B16, V0.B16, V0.B16
	VMOV	V0.D[0], R5
	MOVD.P	R5, 8(R2)
	SUB	$64, R1
	B	loop64
done:
	RET
