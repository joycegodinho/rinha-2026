package ivf

const (
	Legit uint8 = 0
	Fraud uint8 = 1

	Dim         = 16
	Scale       = 10000
	BucketCount = 16 * 8 * 9 * 16 * 8
)

type Vector [Dim]float32
type QVector [Dim]int16

type IVF struct {
	Vectors []int16
	Labels  []uint8
	Offsets []uint32
	IDs     []uint32
	mmap    []byte

	Centroids []float32
	Blocks    []int16
	RawBlocks []byte
	K         int
}

type Neighbor struct {
	ID   int
	Dist int64
}

type Workspace struct {
	Query QVector
	Best  [5]Neighbor
	Size  int
}

type SearchWorkspace struct {
	Probes        [maxProbe]int
	CentroidDists [maxCentroids]float32
	TopDist       [5]float32
	TopLabel      [5]uint8
	Quantized     Vector
}

type PruneCounts struct {
	Blocks     int
	Pruned4    int
	Pruned6    int
	Pruned8    int
	Pruned14   int
	Survived   int
	TailPruned [6]int
}

type SearchTrace struct {
	Path           int
	Frauds         int
	QuickFrauds    int
	RescoreFrauds  int
	QuickBlocks    int
	ExpandedBlocks int
	RescoreBlocks  int
	QuickPrune     PruneCounts
	RescorePrune   PruneCounts
}

func NewWorkspace(k int) *Workspace {
	return &Workspace{}
}

func (db *IVF) Len() int {
	if db == nil {
		return 0
	}
	return len(db.Labels)
}

func clampBin(x int, max int) int {
	if x < 0 {
		return 0
	}
	if x > max {
		return max
	}
	return x
}

func Quantize(v *Vector) QVector {
	var q QVector
	for i := 0; i < Dim; i++ {
		x := int(v[i]*Scale + 0.5)
		if v[i] < 0 {
			x = int(v[i]*Scale - 0.5)
		}
		if x < -32768 {
			x = -32768
		} else if x > 32767 {
			x = 32767
		}
		q[i] = int16(x)
	}
	return q
}

func Dist(a *QVector, vectors []int16, id int) int64 {
	base := id * Dim
	var sum int64
	for i := 0; i < Dim; i++ {
		d := int32(a[i]) - int32(vectors[base+i])
		sum += int64(d * d)
	}
	return sum
}

func coarseParts(q *QVector) (a, r, last, km, flags int) {
	a = clampBin((int(q[0])*16)/Scale, 15)
	r = clampBin((int(q[2])*8)/Scale, 7)
	if q[5] < 0 {
		last = 0
	} else {
		last = 1 + clampBin((int(q[5])*8)/Scale, 7)
	}
	km = clampBin((int(q[7])*16)/Scale, 15)

	if q[9] > Scale/2 {
		flags |= 1
	}
	if q[10] > Scale/2 {
		flags |= 2
	}
	if q[11] > Scale/2 {
		flags |= 4
	}

	return
}

func coarseKeyParts(a, r, t, km, flags int) int {
	return (((a*8+r)*9+t)*16+km)*8 + flags
}

func coarseKey(q *QVector) int {
	a, r, t, km, flags := coarseParts(q)
	return coarseKeyParts(a, r, t, km, flags)
}

func (w *Workspace) reset(k int) {
	w.Size = 0
}

func (w *Workspace) add(id int, dist int64, k int) {
	n := w.Size
	if n < k {
		w.Best[n] = Neighbor{ID: id, Dist: dist}
		w.Size++
		i := n
		for i > 0 && w.Best[i].Dist < w.Best[i-1].Dist {
			w.Best[i], w.Best[i-1] = w.Best[i-1], w.Best[i]
			i--
		}
		return
	}

	if n == 0 || dist >= w.Best[n-1].Dist {
		return
	}

	w.Best[n-1] = Neighbor{ID: id, Dist: dist}
	for i := n - 1; i > 0 && w.Best[i].Dist < w.Best[i-1].Dist; i-- {
		w.Best[i], w.Best[i-1] = w.Best[i-1], w.Best[i]
	}
}

func (db *IVF) scanBucket(key int, q *QVector, w *Workspace, k int) {
	start := db.Offsets[key]
	end := db.Offsets[key+1]
	for i := start; i < end; i++ {
		id := int(db.IDs[i])
		w.add(id, Dist(q, db.Vectors, id), k)
	}
}

func (db *IVF) SearchK(v *Vector, w *Workspace, k int) []Neighbor {
	if k <= 0 {
		return nil
	}
	if k > len(w.Best) {
		k = len(w.Best)
	}

	q := Quantize(v)
	w.Query = q
	w.reset(k)

	a, r, last, km, flags := coarseParts(&q)
	for da := -1; da <= 1; da++ {
		aa := a + da
		if aa < 0 || aa > 15 {
			continue
		}
		for dr := -1; dr <= 1; dr++ {
			rr := r + dr
			if rr < 0 || rr > 7 {
				continue
			}
			for dl := -1; dl <= 1; dl++ {
				ll := last + dl
				if ll < 0 || ll > 8 || (last == 0 && ll != 0) {
					continue
				}
				for dk := -1; dk <= 1; dk++ {
					kk := km + dk
					if kk < 0 || kk > 15 {
						continue
					}
					db.scanBucket(coarseKeyParts(aa, rr, ll, kk, flags), &q, w, k)
				}
			}
		}
	}

	if w.Size >= k {
		return w.Best[:w.Size]
	}

	// If the exact flag partition is too sparse, broaden only after trying the
	// closest numeric cells. This keeps normal queries cheap but avoids empty
	// candidate sets on edge cases.
	for f := 0; f < 8; f++ {
		if f == flags {
			continue
		}
		db.scanBucket(coarseKeyParts(a, r, last, km, f), &q, w, k)
	}

	return w.Best[:w.Size]
}
