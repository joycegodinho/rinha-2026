//go:build !amd64

package ivf

func centroidDists(q *Vector, centroids []float32, k int, out *[maxCentroids]float32) {
	centroidDistsPure(q, centroids, k, out)
}
