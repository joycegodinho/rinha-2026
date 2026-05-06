//go:build !amd64

package ivf

func distBlock(q *Vector, blocks []int16, base int, out *[vectorsPerBlock]float32) {
	distBlockPure(q, blocks, base, out)
}
