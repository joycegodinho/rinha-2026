//go:build amd64

package ivf

//go:noescape
func scanBlocksAVX2(q *Vector, blocks *int16, labels *uint8, start int, end int, topDist *[5]float32, topLabel *[5]uint8, worst *int)

func scanBlocks(q *Vector, blocks []int16, labels []uint8, start, end int, topDist *[5]float32, topLabel *[5]uint8, worst *int) {
	if start >= end {
		return
	}
	scanBlocksAVX2(q, &blocks[0], &labels[0], start, end, topDist, topLabel, worst)
}
