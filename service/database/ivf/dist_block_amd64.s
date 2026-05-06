//go:build amd64

#include "textflag.h"

DATA ·distScale+0(SB)/4, $0x38d1b717
GLOBL ·distScale(SB), RODATA, $4

#define DIM(off, qoff) \
	VPMOVSXWD off(SI), Y0; \
	VCVTDQ2PS Y0, Y0; \
	VMULPS Y15, Y0, Y0; \
	VBROADCASTSS qoff(DI), Y1; \
	VSUBPS Y1, Y0, Y0; \
	VMULPS Y0, Y0, Y0; \
	VADDPS Y0, Y2, Y2

TEXT ·distBlockAVX2(SB), NOSPLIT, $0-32
	MOVQ q+0(FP), DI
	MOVQ blocks+8(FP), SI
	MOVQ base+16(FP), AX
	MOVQ out+24(FP), DX

	LEAQ (SI)(AX*2), SI

	VXORPS Y2, Y2, Y2
	VBROADCASTSS ·distScale(SB), Y15

	DIM(0, 0)
	DIM(16, 4)
	DIM(32, 8)
	DIM(48, 12)
	DIM(64, 16)
	DIM(80, 20)
	DIM(96, 24)
	DIM(112, 28)
	DIM(128, 32)
	DIM(144, 36)
	DIM(160, 40)
	DIM(176, 44)
	DIM(192, 48)
	DIM(208, 52)

	VMOVUPS Y2, (DX)
	VZEROUPPER
	RET
