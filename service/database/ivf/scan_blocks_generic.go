//go:build !amd64

package ivf

func scanBlocks(q *Vector, blocks []int16, labels []uint8, start, end int, topDist *[5]float32, topLabel *[5]uint8, worst *int) {
	for block := start; block < end; block++ {
		base := block * blockStride
		var dists [vectorsPerBlock]float32
		distBlock(q, blocks, base, &dists)

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
