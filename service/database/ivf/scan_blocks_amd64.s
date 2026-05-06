//go:build amd64

#include "textflag.h"

#define DIM_SCAN(off, qoff) \
	VPMOVSXWD off(R9), Y0; \
	VCVTDQ2PS Y0, Y0; \
	VMULPS Y15, Y0, Y0; \
	VBROADCASTSS qoff(DI), Y1; \
	VSUBPS Y1, Y0, Y0; \
	VMULPS Y0, Y0, Y0; \
	VADDPS Y0, Y2, Y2

TEXT ·scanBlocksAVX2(SB), NOSPLIT, $32-64
	MOVQ q+0(FP), DI
	MOVQ blocks+8(FP), SI
	MOVQ labels+16(FP), BX
	MOVQ start+24(FP), R12
	MOVQ end+32(FP), R13
	MOVQ topDist+40(FP), R14
	MOVQ topLabel+48(FP), R15
	MOVQ worst+56(FP), R11

	VBROADCASTSS ·distScale(SB), Y15

block_loop:
	CMPQ R12, R13
	JGE scan_done

	MOVQ R12, AX
	IMULQ $224, AX
	LEAQ (SI)(AX*1), R9

	VXORPS Y2, Y2, Y2
	DIM_SCAN(0, 0)
	DIM_SCAN(16, 4)
	DIM_SCAN(32, 8)
	DIM_SCAN(48, 12)
	MOVQ (R11), R10
	VBROADCASTSS (R14)(R10*4), Y3
	VCMPPS $1, Y3, Y2, Y4
	VMOVMSKPS Y4, AX
	TESTL AX, AX
	JZ next_block

	DIM_SCAN(64, 16)
	DIM_SCAN(80, 20)
	MOVQ (R11), R10
	VBROADCASTSS (R14)(R10*4), Y3
	VCMPPS $1, Y3, Y2, Y4
	VMOVMSKPS Y4, AX
	TESTL AX, AX
	JZ next_block

	DIM_SCAN(96, 24)
	DIM_SCAN(112, 28)

	MOVQ (R11), R10
	VBROADCASTSS (R14)(R10*4), Y3
	VCMPPS $1, Y3, Y2, Y4
	VMOVMSKPS Y4, AX
	TESTL AX, AX
	JZ next_block

	DIM_SCAN(128, 32)
	DIM_SCAN(144, 36)
	DIM_SCAN(160, 40)
	DIM_SCAN(176, 44)
	DIM_SCAN(192, 48)
	DIM_SCAN(208, 52)
	MOVQ (R11), R10
	VBROADCASTSS (R14)(R10*4), Y3
	VCMPPS $1, Y3, Y2, Y4
	VMOVMSKPS Y4, AX
	TESTL AX, AX
	JZ next_block
	VMOVUPS Y2, 0(SP)

	MOVQ R12, AX
	SHLQ $3, AX
	LEAQ (BX)(AX*1), R8

	XORQ CX, CX

slot_loop:
	CMPQ CX, $8
	JGE next_block

	MOVQ (R11), R10
	MOVL 0(SP)(CX*4), AX
	MOVL (R14)(R10*4), R9
	CMPL AX, R9
	JAE slot_next

	MOVL AX, (R14)(R10*4)
	MOVBLZX (R8)(CX*1), AX
	MOVB AX, (R15)(R10*1)

	XORQ R10, R10
	MOVL (R14), DX
	MOVQ $1, R9

worst_loop:
	CMPQ R9, $5
	JGE worst_done
	MOVL (R14)(R9*4), AX
	CMPL AX, DX
	JBE worst_next
	MOVL AX, DX
	MOVQ R9, R10

worst_next:
	INCQ R9
	JMP worst_loop

worst_done:
	MOVQ R10, (R11)

slot_next:
	INCQ CX
	JMP slot_loop

next_block:
	INCQ R12
	JMP block_loop

scan_done:
	VZEROUPPER
	RET
