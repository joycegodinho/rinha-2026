//go:build amd64

package ivf

//go:noescape
func distBlockAVX2(q *Vector, blocks *int16, base int, out *[vectorsPerBlock]float32)

func distBlock(q *Vector, blocks []int16, base int, out *[vectorsPerBlock]float32) {
	distBlockAVX2(q, &blocks[0], base, out)
}
