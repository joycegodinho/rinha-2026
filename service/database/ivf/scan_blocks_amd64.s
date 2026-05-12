//go:build amd64

#include "textflag.h"

#define DIM_SCAN16(off, qoff) \
	VBROADCASTSS qoff(DI), Y1; \
	VPMOVSXWD off(R9), Y0; \
	VCVTDQ2PS Y0, Y0; \
	VMULPS Y15, Y0, Y0; \
	VSUBPS Y1, Y0, Y0; \
	VFMADD231PS Y0, Y0, Y2; \
	VPMOVSXWD off+16(R9), Y6; \
	VCVTDQ2PS Y6, Y6; \
	VMULPS Y15, Y6, Y6; \
	VSUBPS Y1, Y6, Y6; \
	VFMADD231PS Y6, Y6, Y5

#define DIM_SCAN_LOW(off, qoff) \
	VBROADCASTSS qoff(DI), Y1; \
	VPMOVSXWD off(R9), Y0; \
	VCVTDQ2PS Y0, Y0; \
	VMULPS Y15, Y0, Y0; \
	VSUBPS Y1, Y0, Y0; \
	VFMADD231PS Y0, Y0, Y2

#define DIM_SCAN_HIGH(off, qoff) \
	VBROADCASTSS qoff(DI), Y1; \
	VPMOVSXWD off+16(R9), Y6; \
	VCVTDQ2PS Y6, Y6; \
	VMULPS Y15, Y6, Y6; \
	VSUBPS Y1, Y6, Y6; \
	VFMADD231PS Y6, Y6, Y5

TEXT ·scanBlocksAVX2(SB), NOSPLIT, $80-64
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
	IMULQ $448, AX
	LEAQ (SI)(AX*1), R9
	MOVQ (R11), R10

	VXORPS Y2, Y2, Y2
	VXORPS Y5, Y5, Y5

	DIM_SCAN16(0, 0)
	DIM_SCAN16(32, 4)
	DIM_SCAN16(64, 8)
	DIM_SCAN16(96, 12)
	VBROADCASTSS (R14)(R10*4), Y3
	VCMPPS $1, Y3, Y2, Y4
	VCMPPS $1, Y3, Y5, Y7
	VMOVMSKPS Y4, AX
	VMOVMSKPS Y7, DX
	TESTL AX, AX
	JNZ stage6_both_check_high
	TESTL DX, DX
	JNZ stage6_high_only
	JMP block_next

stage6_both_check_high:
	TESTL DX, DX
	JZ stage6_low_only

	DIM_SCAN16(128, 16)
	DIM_SCAN16(160, 20)
	VBROADCASTSS (R14)(R10*4), Y3
	VCMPPS $1, Y3, Y2, Y4
	VCMPPS $1, Y3, Y5, Y7
	VMOVMSKPS Y4, AX
	VMOVMSKPS Y7, DX
	TESTL AX, AX
	JNZ stage8_both_check_high
	TESTL DX, DX
	JNZ stage8_high_only
	JMP block_next

stage8_both_check_high:
	TESTL DX, DX
	JZ stage8_low_only

	DIM_SCAN16(192, 24)
	DIM_SCAN16(224, 28)
	VBROADCASTSS (R14)(R10*4), Y3
	VCMPPS $1, Y3, Y2, Y4
	VCMPPS $1, Y3, Y5, Y7
	VMOVMSKPS Y4, AX
	VMOVMSKPS Y7, DX
	TESTL AX, AX
	JNZ tail_both_check_high
	TESTL DX, DX
	JNZ tail_high_only
	JMP block_next

tail_both_check_high:
	TESTL DX, DX
	JZ tail_low_only

	DIM_SCAN16(256, 32)
	DIM_SCAN16(288, 36)
	DIM_SCAN16(320, 40)
	DIM_SCAN16(352, 44)
	DIM_SCAN16(384, 48)
	DIM_SCAN16(416, 52)
	VBROADCASTSS (R14)(R10*4), Y3
	VCMPPS $1, Y3, Y2, Y4
	VCMPPS $1, Y3, Y5, Y7
	VMOVMSKPS Y4, AX
	VMOVMSKPS Y7, DX
	TESTL AX, AX
	JNZ store_both_masks
	TESTL DX, DX
	JNZ store_high_only_mask
	JMP block_next

stage6_low_only:
	DIM_SCAN_LOW(128, 16)
	DIM_SCAN_LOW(160, 20)
	VBROADCASTSS (R14)(R10*4), Y3
	VCMPPS $1, Y3, Y2, Y4
	VMOVMSKPS Y4, AX
	TESTL AX, AX
	JZ block_next
	JMP stage8_low_only

stage6_high_only:
	DIM_SCAN_HIGH(128, 16)
	DIM_SCAN_HIGH(160, 20)
	VBROADCASTSS (R14)(R10*4), Y3
	VCMPPS $1, Y3, Y5, Y7
	VMOVMSKPS Y7, DX
	TESTL DX, DX
	JZ block_next
	JMP stage8_high_only

stage8_low_only:
	DIM_SCAN_LOW(192, 24)
	DIM_SCAN_LOW(224, 28)
	VBROADCASTSS (R14)(R10*4), Y3
	VCMPPS $1, Y3, Y2, Y4
	VMOVMSKPS Y4, AX
	TESTL AX, AX
	JZ block_next
	JMP tail_low_only

stage8_high_only:
	DIM_SCAN_HIGH(192, 24)
	DIM_SCAN_HIGH(224, 28)
	VBROADCASTSS (R14)(R10*4), Y3
	VCMPPS $1, Y3, Y5, Y7
	VMOVMSKPS Y7, DX
	TESTL DX, DX
	JZ block_next
	JMP tail_high_only

tail_low_only:
	DIM_SCAN_LOW(256, 32)
	DIM_SCAN_LOW(288, 36)
	DIM_SCAN_LOW(320, 40)
	DIM_SCAN_LOW(352, 44)
	DIM_SCAN_LOW(384, 48)
	DIM_SCAN_LOW(416, 52)
	VBROADCASTSS (R14)(R10*4), Y3
	VCMPPS $1, Y3, Y2, Y4
	VMOVMSKPS Y4, AX
	TESTL AX, AX
	JZ block_next
	JMP store_low_only_mask

tail_high_only:
	DIM_SCAN_HIGH(256, 32)
	DIM_SCAN_HIGH(288, 36)
	DIM_SCAN_HIGH(320, 40)
	DIM_SCAN_HIGH(352, 44)
	DIM_SCAN_HIGH(384, 48)
	DIM_SCAN_HIGH(416, 52)
	VBROADCASTSS (R14)(R10*4), Y3
	VCMPPS $1, Y3, Y5, Y7
	VMOVMSKPS Y7, DX
	TESTL DX, DX
	JZ block_next
	JMP store_high_only_mask

store_both_masks:
	VMOVUPS Y2, 0(SP)
	VMOVUPS Y5, 32(SP)
	MOVL AX, 64(SP)
	MOVL DX, 68(SP)
	JMP prepare_slot_loops

store_low_only_mask:
	VMOVUPS Y2, 0(SP)
	MOVL AX, 64(SP)
	MOVL $0, 68(SP)
	JMP prepare_slot_loops

store_high_only_mask:
	VMOVUPS Y5, 32(SP)
	MOVL $0, 64(SP)
	MOVL DX, 68(SP)
	JMP prepare_slot_loops

prepare_slot_loops:
	MOVQ R12, AX
	SHLQ $4, AX
	LEAQ (BX)(AX*1), R8

low_mask_loop:
	MOVL 64(SP), AX
	TESTL AX, AX
	JZ high_mask_loop

	BSFL AX, CX
	LEAL -1(AX), DX
	ANDL DX, AX
	MOVL AX, 64(SP)

	MOVQ (R11), R10
	MOVL 0(SP)(CX*4), AX
	MOVL (R14)(R10*4), DX
	CMPL AX, DX
	JAE low_mask_loop

	MOVL AX, (R14)(R10*4)
	MOVBLZX (R8)(CX*1), AX
	MOVB AX, (R15)(R10*1)

	XORQ R10, R10
	MOVL (R14), DX
	MOVL 4(R14), AX
	CMPL AX, DX
	JBE low_cmp2
	MOVL AX, DX
	MOVQ $1, R10
low_cmp2:
	MOVL 8(R14), AX
	CMPL AX, DX
	JBE low_cmp3
	MOVL AX, DX
	MOVQ $2, R10
low_cmp3:
	MOVL 12(R14), AX
	CMPL AX, DX
	JBE low_cmp4
	MOVL AX, DX
	MOVQ $3, R10
low_cmp4:
	MOVL 16(R14), AX
	CMPL AX, DX
	JBE low_worst_done
	MOVQ $4, R10
low_worst_done:
	MOVQ R10, (R11)

	JMP low_mask_loop

high_mask_loop:
	MOVL 68(SP), AX
	TESTL AX, AX
	JZ block_next

	BSFL AX, CX
	LEAL -1(AX), DX
	ANDL DX, AX
	MOVL AX, 68(SP)

	MOVQ (R11), R10
	MOVL 32(SP)(CX*4), AX
	MOVL (R14)(R10*4), DX
	CMPL AX, DX
	JAE high_mask_loop

	MOVL AX, (R14)(R10*4)
	MOVBLZX 8(R8)(CX*1), AX
	MOVB AX, (R15)(R10*1)

	XORQ R10, R10
	MOVL (R14), DX
	MOVL 4(R14), AX
	CMPL AX, DX
	JBE high_cmp2
	MOVL AX, DX
	MOVQ $1, R10
high_cmp2:
	MOVL 8(R14), AX
	CMPL AX, DX
	JBE high_cmp3
	MOVL AX, DX
	MOVQ $2, R10
high_cmp3:
	MOVL 12(R14), AX
	CMPL AX, DX
	JBE high_cmp4
	MOVL AX, DX
	MOVQ $3, R10
high_cmp4:
	MOVL 16(R14), AX
	CMPL AX, DX
	JBE high_worst_done
	MOVQ $4, R10
high_worst_done:
	MOVQ R10, (R11)
	JMP high_mask_loop

block_next:
	INCQ R12
	JMP block_loop

scan_done:
	VZEROUPPER
	RET
