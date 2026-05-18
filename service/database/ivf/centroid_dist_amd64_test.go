//go:build amd64

package ivf

import (
	"math"
	"testing"
)

func TestCentroidDistsAVX2MatchesPure(t *testing.T) {
	var q Vector
	for i := 0; i < indexDim; i++ {
		q[i] = float32(i-3) * 0.041
	}
	centroids := make([]float32, maxCentroids*indexDim)
	for i := range centroids {
		centroids[i] = float32((i*3571)%20000-10000) * 0.0001
	}

	var got [maxCentroids]float32
	var want [maxCentroids]float32
	centroidDistsAVX2(&q, &centroids[0], maxCentroids, &got)
	centroidDistsPure(&q, centroids, maxCentroids, &want)

	for i := 0; i < maxCentroids; i++ {
		if math.Abs(float64(got[i]-want[i])) > 1e-5 {
			t.Fatalf("centroid %d: got %f, want %f", i, got[i], want[i])
		}
	}
}

func BenchmarkCentroidDistsAVX2(b *testing.B) {
	var q Vector
	for i := 0; i < indexDim; i++ {
		q[i] = float32(i) * 0.031
	}
	centroids := make([]float32, maxCentroids*indexDim)
	for i := range centroids {
		centroids[i] = float32((i*3571)%20000-10000) * 0.0001
	}
	var out [maxCentroids]float32

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		centroidDistsAVX2(&q, &centroids[0], maxCentroids, &out)
	}
}

func BenchmarkCentroidDistsPure(b *testing.B) {
	var q Vector
	for i := 0; i < indexDim; i++ {
		q[i] = float32(i) * 0.031
	}
	centroids := make([]float32, maxCentroids*indexDim)
	for i := range centroids {
		centroids[i] = float32((i*3571)%20000-10000) * 0.0001
	}
	var out [maxCentroids]float32

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		centroidDistsPure(&q, centroids, maxCentroids, &out)
	}
}
