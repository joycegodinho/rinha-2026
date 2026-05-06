//go:build amd64

package ivf

import (
	"math"
	"os"
	"testing"
)

func scanBlocksPureForTest(q *Vector, blocks []int16, labels []uint8, start, end int, topDist *[5]float32, topLabel *[5]uint8, worst *int) {
	for block := start; block < end; block++ {
		base := block * blockStride
		var dists [vectorsPerBlock]float32
		distBlockPure(q, blocks, base, &dists)
		labelBase := block * vectorsPerBlock
		for slot := 0; slot < vectorsPerBlock; slot++ {
			dist := dists[slot]
			if dist >= topDist[*worst] {
				continue
			}
			topDist[*worst] = dist
			topLabel[*worst] = labels[labelBase+slot]
			wi := 0
			wv := topDist[0]
			for i := 1; i < 5; i++ {
				if topDist[i] > wv {
					wv = topDist[i]
					wi = i
				}
			}
			*worst = wi
		}
	}
}

func TestScanBlocksAVX2MatchesPure(t *testing.T) {
	var q Vector
	for i := 0; i < indexDim; i++ {
		q[i] = float32(i-2) * 0.052
	}
	blocks := make([]int16, blockStride*32)
	labels := make([]uint8, vectorsPerBlock*32)
	for i := range blocks {
		blocks[i] = int16((i*7919)%20000 - 10000)
	}
	for i := range labels {
		labels[i] = uint8(i % 2)
	}

	gotDist := [5]float32{inf32(), inf32(), inf32(), inf32(), inf32()}
	wantDist := gotDist
	var gotLabel, wantLabel [5]uint8
	gotWorst, wantWorst := 0, 0

	scanBlocksAVX2(&q, &blocks[0], &labels[0], 3, 31, &gotDist, &gotLabel, &gotWorst)
	scanBlocksPureForTest(&q, blocks, labels, 3, 31, &wantDist, &wantLabel, &wantWorst)

	for i := 0; i < 5; i++ {
		if math.Abs(float64(gotDist[i]-wantDist[i])) > 1e-5 || gotLabel[i] != wantLabel[i] {
			t.Fatalf("slot %d: got (%f,%d), want (%f,%d)", i, gotDist[i], gotLabel[i], wantDist[i], wantLabel[i])
		}
	}
	if gotWorst != wantWorst {
		t.Fatalf("worst got %d, want %d", gotWorst, wantWorst)
	}
}

func inf32() float32 {
	return float32(math.Inf(1))
}

func BenchmarkScanBlocksAVX2(b *testing.B) {
	var q Vector
	for i := 0; i < indexDim; i++ {
		q[i] = float32(i) * 0.031
	}
	blocks := make([]int16, blockStride*256)
	labels := make([]uint8, vectorsPerBlock*256)
	for i := range blocks {
		blocks[i] = int16((i*1543)%20000 - 10000)
	}
	var topDist [5]float32
	var topLabel [5]uint8
	worst := 0

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 5; j++ {
			topDist[j] = inf32()
		}
		scanBlocksAVX2(&q, &blocks[0], &labels[0], 0, 256, &topDist, &topLabel, &worst)
	}
}

func BenchmarkScanBlocksPure(b *testing.B) {
	var q Vector
	for i := 0; i < indexDim; i++ {
		q[i] = float32(i) * 0.031
	}
	blocks := make([]int16, blockStride*256)
	labels := make([]uint8, vectorsPerBlock*256)
	for i := range blocks {
		blocks[i] = int16((i*1543)%20000 - 10000)
	}
	var topDist [5]float32
	var topLabel [5]uint8
	worst := 0

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 5; j++ {
			topDist[j] = inf32()
		}
		scanBlocksPureForTest(&q, blocks, labels, 0, 256, &topDist, &topLabel, &worst)
	}
}

func BenchmarkFraudCount5(b *testing.B) {
	db, err := LoadKMeansIndex(benchmarkIndexPath())
	if err != nil {
		b.Fatal(err)
	}
	q := Vector{0.1181, 0.3333, 0.9063, 0.2609, 0.3333, 0.0708, 0.2631, 0.1105, 0.2, 1, 0, 1, 0.3, 0.0134}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.FraudCount5(&q)
	}
}

func BenchmarkFraudCount5WithWorkspace(b *testing.B) {
	db, err := LoadKMeansIndex(benchmarkIndexPath())
	if err != nil {
		b.Fatal(err)
	}
	q := Vector{0.1181, 0.3333, 0.9063, 0.2609, 0.3333, 0.0708, 0.2631, 0.1105, 0.2, 1, 0, 1, 0.3, 0.0134}
	var ws SearchWorkspace

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.FraudCount5WithWorkspace(&q, &ws)
	}
}

func benchmarkIndexPath() string {
	if path := os.Getenv("IVF_INDEX_PATH"); path != "" {
		return path
	}
	return "../../index.bin.gz"
}
