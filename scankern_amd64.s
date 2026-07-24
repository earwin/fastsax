// amd64 byte-equality mask kernels; see scankern_amd64.go for the contract
// and dispatch. ABI0 (explicit FP offsets), NOSPLIT, no stack frame.
// maskByte64SSE2 uses only baseline-amd64 instructions; maskByte64AVX2 is
// called only after runtime detection (cpuHasAVX2).

#include "textflag.h"

// func maskByte64SSE2(b []byte, c byte, masks []uint64)
// Args: b_base+0, b_len+8, b_cap+16, c+24, masks_base+32, masks_len+40,
// masks_cap+48.
// For each complete 64-byte block of b, produce a 64-bit movemask of
// (byte == c) via PCMPEQB + PMOVMSKB on four 16-byte lanes and store it to
// the next masks word.
TEXT ·maskByte64SSE2(SB), NOSPLIT, $0-56
	MOVQ	b_base+0(FP), SI
	MOVQ	b_len+8(FP), CX
	MOVBLZX	c+24(FP), AX
	MOVQ	masks_base+32(FP), DI
	// Broadcast c to all 16 bytes of X0 (SSE2: unpack twice, then shuffle).
	MOVQ	AX, X0
	PUNPCKLBW	X0, X0
	PUNPCKLWL	X0, X0
	PSHUFD	$0, X0, X0
loop64:
	CMPQ	CX, $64
	JLT	done
	MOVOU	(SI), X1
	MOVOU	16(SI), X2
	MOVOU	32(SI), X3
	MOVOU	48(SI), X4
	PCMPEQB	X0, X1
	PCMPEQB	X0, X2
	PCMPEQB	X0, X3
	PCMPEQB	X0, X4
	PMOVMSKB	X1, AX
	PMOVMSKB	X2, BX
	PMOVMSKB	X3, DX
	PMOVMSKB	X4, R8
	SHLQ	$16, BX
	SHLQ	$32, DX
	SHLQ	$48, R8
	ORQ	BX, AX
	ORQ	DX, AX
	ORQ	R8, AX
	MOVQ	AX, (DI)
	ADDQ	$64, SI
	ADDQ	$8, DI
	SUBQ	$64, CX
	JMP	loop64
done:
	RET

// func maskByte64AVX2(b []byte, c byte, masks []uint64)
// Same contract as maskByte64SSE2: two 32-byte VPCMPEQB/VPMOVMSKB lanes per
// 64-byte block. VPMOVMSKB to a 32-bit register zero-extends, so the high
// half of AX is clean before the OR. VZEROUPPER on exit keeps later SSE code
// out of the AVX-SSE transition penalty.
TEXT ·maskByte64AVX2(SB), NOSPLIT, $0-56
	MOVQ	b_base+0(FP), SI
	MOVQ	b_len+8(FP), CX
	MOVQ	masks_base+32(FP), DI
	VPBROADCASTB	c+24(FP), Y0
avx_loop64:
	CMPQ	CX, $64
	JLT	avx_done
	VMOVDQU	(SI), Y1
	VMOVDQU	32(SI), Y2
	VPCMPEQB	Y0, Y1, Y1
	VPCMPEQB	Y0, Y2, Y2
	VPMOVMSKB	Y1, AX
	VPMOVMSKB	Y2, BX
	SHLQ	$32, BX
	ORQ	BX, AX
	MOVQ	AX, (DI)
	ADDQ	$64, SI
	ADDQ	$8, DI
	SUBQ	$64, CX
	JMP	avx_loop64
avx_done:
	VZEROUPPER
	RET

// func cpuidx(eaxIn, ecxIn uint32) (eax, ebx, ecx, edx uint32)
TEXT ·cpuidx(SB), NOSPLIT, $0-24
	MOVL	eaxIn+0(FP), AX
	MOVL	ecxIn+4(FP), CX
	CPUID
	MOVL	AX, eax+8(FP)
	MOVL	BX, ebx+12(FP)
	MOVL	CX, ecx+16(FP)
	MOVL	DX, edx+20(FP)
	RET

// func xgetbv0() uint64
TEXT ·xgetbv0(SB), NOSPLIT, $0-8
	XORL	CX, CX
	XGETBV
	SHLQ	$32, DX
	ORQ	DX, AX
	MOVQ	AX, ret+0(FP)
	RET
