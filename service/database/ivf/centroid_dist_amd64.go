//go:build amd64

package ivf

//go:noescape
func centroidDistsAVX2(q *Vector, centroids *float32, k int, out *[maxCentroids]float32)

func centroidDists(q *Vector, centroids []float32, k int, out *[maxCentroids]float32) {
	centroidDistsAVX2(q, &centroids[0], k, out)
}
