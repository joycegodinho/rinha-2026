package ivf

func distBlockPure(q *Vector, blocks []int16, base int, out *[vectorsPerBlock]float32) {
	for slot := 0; slot < vectorsPerBlock; slot++ {
		out[slot] = 0
	}
	for d := 0; d < indexDim; d++ {
		dimBase := base + d*vectorsPerBlock
		qd := q[d]
		for slot := 0; slot < vectorsPerBlock; slot++ {
			ref := float32(blocks[dimBase+slot]) * vectorScale
			diff := ref - qd
			out[slot] += diff * diff
		}
	}
}
