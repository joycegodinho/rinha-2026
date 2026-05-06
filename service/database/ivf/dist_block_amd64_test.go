//go:build amd64

package ivf

import (
	"math"
	"testing"
)

func TestDistBlockAVX2MatchesPure(t *testing.T) {
	var q Vector
	for i := 0; i < indexDim; i++ {
		q[i] = float32(i-2) * 0.073
	}

	blocks := make([]int16, blockStride*3)
	for i := range blocks {
		blocks[i] = int16((i*7919)%20000 - 10000)
	}

	var got [vectorsPerBlock]float32
	var want [vectorsPerBlock]float32

	distBlockAVX2(&q, &blocks[0], blockStride, &got)
	distBlockPure(&q, blocks, blockStride, &want)

	for i := 0; i < vectorsPerBlock; i++ {
		if math.Abs(float64(got[i]-want[i])) > 1e-5 {
			t.Fatalf("slot %d: got %f, want %f", i, got[i], want[i])
		}
	}
}

func BenchmarkDistBlockAVX2(b *testing.B) {
	var q Vector
	for i := 0; i < indexDim; i++ {
		q[i] = float32(i) * 0.031
	}
	blocks := make([]int16, blockStride)
	for i := range blocks {
		blocks[i] = int16((i*1543)%20000 - 10000)
	}
	var out [vectorsPerBlock]float32

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		distBlockAVX2(&q, &blocks[0], 0, &out)
	}
}

func BenchmarkDistBlockPure(b *testing.B) {
	var q Vector
	for i := 0; i < indexDim; i++ {
		q[i] = float32(i) * 0.031
	}
	blocks := make([]int16, blockStride)
	for i := range blocks {
		blocks[i] = int16((i*1543)%20000 - 10000)
	}
	var out [vectorsPerBlock]float32

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		distBlockPure(&q, blocks, 0, &out)
	}
}
