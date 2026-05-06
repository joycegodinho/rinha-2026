//go:build amd64

#include "textflag.h"

TEXT ·centroidDistsAVX2(SB), NOSPLIT, $0-32
	MOVQ q+0(FP), DI
	MOVQ centroids+8(FP), SI
	MOVQ k+16(FP), CX
	MOVQ out+24(FP), DX

	MOVQ CX, R10
	SHLQ $2, R10

	XORQ R8, R8
	VBROADCASTSS 0(DI), Y1
init_loop:
	CMPQ R8, CX
	JGE init_done
	VMOVUPS (SI)(R8*4), Y0
	VSUBPS Y1, Y0, Y0
	VMULPS Y0, Y0, Y0
	VMOVUPS Y0, (DX)(R8*4)
	ADDQ $8, R8
	JMP init_loop

init_done:
	MOVQ $1, R11
	LEAQ (SI)(R10*1), R9

dim_loop:
	CMPQ R11, $14
	JGE done

	VBROADCASTSS (DI)(R11*4), Y1
	XORQ R8, R8

add_loop:
	CMPQ R8, CX
	JGE add_done
	VMOVUPS (R9)(R8*4), Y0
	VSUBPS Y1, Y0, Y0
	VMULPS Y0, Y0, Y0
	VADDPS (DX)(R8*4), Y0, Y0
	VMOVUPS Y0, (DX)(R8*4)
	ADDQ $8, R8
	JMP add_loop

add_done:
	ADDQ $1, R11
	ADDQ R10, R9
	JMP dim_loop

done:
	VZEROUPPER
	RET
