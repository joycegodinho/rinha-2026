package ivf

func centroidDistsPure(q *Vector, centroids []float32, k int, out *[maxCentroids]float32) {
	q0 := q[0]
	for ci := 0; ci < k; ci++ {
		diff := q0 - centroids[ci]
		out[ci] = diff * diff
	}

	for d := 1; d < indexDim; d++ {
		base := d * k
		qd := q[d]
		for ci := 0; ci < k; ci++ {
			diff := qd - centroids[base+ci]
			out[ci] += diff * diff
		}
	}
}
