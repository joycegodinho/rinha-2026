package main

import (
	"bufio"
	"compress/gzip"
	"container/heap"
	"encoding/binary"
	"encoding/json"
	"log"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	dim               = 14
	defaultK          = 4096
	defaultNIter      = 25
	defaultSampleSize = 50000
	defaultSeed       = 0xdeadbeefcafebabe
	scale             = 10000
)

func fraudLabelByte(label byte) byte {
	return label & 1
}

var naturalDimOrder = [dim]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}
var hot6DimOrder = [dim]int{0, 1, 2, 7, 8, 12, 3, 4, 5, 6, 9, 10, 11, 13}

type record struct {
	Vector [dim]float32 `json:"vector"`
	Label  string       `json:"label"`
}

type lcg struct {
	state uint64
}

func (r *lcg) nextU64() uint64 {
	r.state = r.state*6364136223846793005 + 1442695040888963407
	return r.state
}

func (r *lcg) nextInt(n int) int {
	return int(r.nextU64()>>33) % n
}

func (r *lcg) nextFloat64() float64 {
	return float64(r.nextU64()>>11) / float64(uint64(1)<<53)
}

func loadDataset(path string) ([][dim]float32, []byte) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	dec := json.NewDecoder(bufio.NewReaderSize(f, 1<<20))
	if _, err := dec.Token(); err != nil {
		log.Fatal(err)
	}

	vectors := make([][dim]float32, 0, 3_100_000)
	labels := make([]byte, 0, 3_100_000)

	for dec.More() {
		var rec record
		if err := dec.Decode(&rec); err != nil {
			log.Fatal(err)
		}
		vectors = append(vectors, rec.Vector)
		if rec.Label == "fraud" {
			labels = append(labels, 1)
		} else {
			labels = append(labels, 0)
		}
	}

	return vectors, labels
}

func distSq(a, b *[dim]float32) float32 {
	var d float32
	for i := 0; i < dim; i++ {
		diff := a[i] - b[i]
		d += diff * diff
	}
	return d
}

func prefix6DistSq(a, b *[dim]float32) float32 {
	var d float32
	for i := 0; i < 6; i++ {
		diff := a[i] - b[i]
		d += diff * diff
	}
	return d
}

func kmeansPlusPlusInit(vectors [][dim]float32, seed uint64, k, sampleSize int) [][dim]float32 {
	n := len(vectors)
	rng := lcg{state: seed}
	ss := sampleSize
	if n < ss {
		ss = n
	}

	sample := make([]int, ss)
	for i := 0; i < ss; i++ {
		sample[i] = rng.nextInt(n)
	}

	centroids := make([][dim]float32, 0, k)
	centroids = append(centroids, vectors[sample[rng.nextInt(ss)]])
	minDists := make([]float32, ss)
	for i := range minDists {
		minDists[i] = float32(math.Inf(1))
	}

	for len(centroids) < k {
		last := centroids[len(centroids)-1]
		var total float64
		for i, vi := range sample {
			d := distSq(&vectors[vi], &last)
			if d < minDists[i] {
				minDists[i] = d
			}
			total += float64(minDists[i])
		}

		target := rng.nextFloat64() * total
		var acc float64
		chosen := ss - 1
		for i, d := range minDists {
			acc += float64(d)
			if acc >= target {
				chosen = i
				break
			}
		}
		centroids = append(centroids, vectors[sample[chosen]])
	}

	return centroids
}

func nearestCentroid(v *[dim]float32, centroids [][dim]float32) uint16 {
	best := float32(math.Inf(1))
	bestIdx := uint16(0)
	for i := range centroids {
		d := distSq(v, &centroids[i])
		if d < best {
			best = d
			bestIdx = uint16(i)
		}
	}
	return bestIdx
}

func nearestTwoCentroids(v *[dim]float32, centroids [][dim]float32) (uint16, uint16) {
	best := float32(math.Inf(1))
	second := float32(math.Inf(1))
	bestIdx := uint16(0)
	secondIdx := uint16(0)
	for i := range centroids {
		d := distSq(v, &centroids[i])
		if d < best {
			second = best
			secondIdx = bestIdx
			best = d
			bestIdx = uint16(i)
		} else if d < second {
			second = d
			secondIdx = uint16(i)
		}
	}
	return bestIdx, secondIdx
}

func assignParallel(vectors [][dim]float32, centroids [][dim]float32, assignments []uint16) int {
	threads := runtime.NumCPU()
	if threads > 16 {
		threads = 16
	}
	chunk := (len(vectors) + threads - 1) / threads

	var changed atomic.Int64
	var wg sync.WaitGroup
	for start := 0; start < len(vectors); start += chunk {
		end := start + chunk
		if end > len(vectors) {
			end = len(vectors)
		}
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			local := 0
			for i := start; i < end; i++ {
				best := nearestCentroid(&vectors[i], centroids)
				if assignments[i] != best {
					assignments[i] = best
					local++
				}
			}
			changed.Add(int64(local))
		}(start, end)
	}
	wg.Wait()
	return int(changed.Load())
}

func updateCentroids(vectors [][dim]float32, assignments []uint16, centroids [][dim]float32) {
	k := len(centroids)
	sums := make([][dim]float64, k)
	counts := make([]uint32, k)

	for i := range vectors {
		ci := int(assignments[i])
		counts[ci]++
		for d := 0; d < dim; d++ {
			sums[ci][d] += float64(vectors[i][d])
		}
	}

	for ci := 0; ci < k; ci++ {
		if counts[ci] == 0 {
			continue
		}
		div := float64(counts[ci])
		for d := 0; d < dim; d++ {
			centroids[ci][d] = float32(sums[ci][d] / div)
		}
	}
}

func trainKMeans(vectors [][dim]float32, k, nIter, sampleSize int, seed uint64, name string) ([][dim]float32, []uint16) {
	log.Printf("kmeans++ init name=%s k=%d nIter=%d sampleSize=%d seed=0x%x", name, k, nIter, sampleSize, seed)
	centroids := kmeansPlusPlusInit(vectors, seed, k, sampleSize)
	assignments := make([]uint16, len(vectors))
	for iter := 0; iter < nIter; iter++ {
		t := time.Now()
		changed := assignParallel(vectors, centroids, assignments)
		updateCentroids(vectors, assignments, centroids)
		log.Printf("iter name=%s %02d changed=%.2f%% elapsed=%s", name, iter+1,
			float64(changed)*100/float64(len(vectors)), time.Since(t))
		if changed*1000 < len(vectors) {
			break
		}
	}
	return centroids, assignments
}

func sampleTrainingVectors(vectors [][dim]float32, sampleSize int, seed uint64) [][dim]float32 {
	if sampleSize >= len(vectors) {
		return vectors
	}
	rng := lcg{state: seed}
	out := make([][dim]float32, sampleSize)
	for i := range out {
		out[i] = vectors[rng.nextInt(len(vectors))]
	}
	return out
}

func trainSampleKMeans(vectors [][dim]float32, k, nIter, sampleSize int, seed uint64, name string) ([][dim]float32, []uint16) {
	sample := sampleTrainingVectors(vectors, sampleSize, seed^0x73616d706c65)
	log.Printf("sample-train name=%s full_n=%d sample_n=%d k=%d nIter=%d seed=0x%x",
		name, len(vectors), len(sample), k, nIter, seed)
	centroids, _ := trainKMeans(sample, k, nIter, len(sample), seed, name+"-sample")

	assignments := make([]uint16, len(vectors))
	t := time.Now()
	changed := assignParallel(vectors, centroids, assignments)
	log.Printf("sample-train full assignment changed=%d elapsed=%s", changed, time.Since(t))
	return centroids, assignments
}

func rebalanceAssignmentsTop2(vectors [][dim]float32, centroids [][dim]float32, assignments []uint16) {
	k := len(centroids)
	if k <= 1 || len(vectors) == 0 {
		return
	}
	log.Printf("balance2: reassigning %d vectors to final centroids and recording second nearest", len(vectors))
	alt := make([]uint16, len(vectors))
	threads := runtime.NumCPU()
	if threads > 16 {
		threads = 16
	}
	chunk := (len(vectors) + threads - 1) / threads
	var changed atomic.Int64
	var wg sync.WaitGroup
	for start := 0; start < len(vectors); start += chunk {
		end := start + chunk
		if end > len(vectors) {
			end = len(vectors)
		}
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			localChanged := 0
			for i := start; i < end; i++ {
				best, second := nearestTwoCentroids(&vectors[i], centroids)
				if assignments[i] != best {
					localChanged++
				}
				assignments[i] = best
				alt[i] = second
			}
			changed.Add(int64(localChanged))
		}(start, end)
	}
	wg.Wait()

	counts := make([]uint32, k)
	for _, ci := range assignments {
		counts[int(ci)]++
	}
	avg := uint32(len(vectors) / k)
	maxCap := avg*3 + 1
	if avg+10 > maxCap {
		maxCap = avg + 10
	}
	var beforeMax uint32
	for _, c := range counts {
		if c > beforeMax {
			beforeMax = c
		}
	}

	moved := 0
	for i, c1 := range assignments {
		c2 := alt[i]
		if counts[int(c1)] > maxCap && counts[int(c2)] < maxCap {
			counts[int(c1)]--
			counts[int(c2)]++
			assignments[i] = c2
			moved++
		}
	}
	var afterMax uint32
	for _, c := range counts {
		if c > afterMax {
			afterMax = c
		}
	}
	log.Printf("balance2: final_reassign_changed=%d moved=%d avg=%d max_cap=%d max_before=%d max_after=%d",
		changed.Load(), moved, avg, maxCap, beforeMax, afterMax)
}

func classSplitFraudPercent(layout string) (int, bool) {
	if layout == "classsplit" {
		return 50, true
	}
	const prefix = "classsplit"
	if !strings.HasPrefix(layout, prefix) {
		return 0, false
	}
	pct, err := strconv.Atoi(layout[len(prefix):])
	if err != nil || pct <= 0 || pct >= 100 {
		log.Fatalf("invalid classsplit layout %q; use classsplit or classsplitNN, e.g. classsplit50", layout)
	}
	return pct, true
}

func trainClassSplit(vectors [][dim]float32, labels []byte, k, nIter, sampleSize int, seed uint64, layout string) ([][dim]float32, []uint16) {
	fraudPercent, ok := classSplitFraudPercent(layout)
	if !ok {
		log.Fatalf("internal error: trainClassSplit called for layout %q", layout)
	}
	fraudK := k * fraudPercent / 100
	if fraudK < 1 {
		fraudK = 1
	}
	if fraudK >= k {
		fraudK = k - 1
	}
	legitK := k - fraudK

	legitVectors := make([][dim]float32, 0, len(vectors)*2/3)
	fraudVectors := make([][dim]float32, 0, len(vectors)/3)
	legitIDs := make([]int, 0, len(vectors)*2/3)
	fraudIDs := make([]int, 0, len(vectors)/3)
	for i, label := range labels {
		if fraudLabelByte(label) == 0 {
			legitIDs = append(legitIDs, i)
			legitVectors = append(legitVectors, vectors[i])
		} else {
			fraudIDs = append(fraudIDs, i)
			fraudVectors = append(fraudVectors, vectors[i])
		}
	}
	if len(legitVectors) < legitK || len(fraudVectors) < fraudK {
		log.Fatalf("classsplit k too high: legit=%d legitK=%d fraud=%d fraudK=%d",
			len(legitVectors), legitK, len(fraudVectors), fraudK)
	}

	log.Printf("classsplit layout=%s legit=%d fraud=%d legitK=%d fraudK=%d",
		layout, len(legitVectors), len(fraudVectors), legitK, fraudK)
	legitCentroids, legitAssignments := trainKMeans(legitVectors, legitK, nIter, sampleSize, seed^0x6c65676974, "legit")
	fraudCentroids, fraudAssignments := trainKMeans(fraudVectors, fraudK, nIter, sampleSize, seed^0x6672617564, "fraud")

	centroids := make([][dim]float32, 0, k)
	centroids = append(centroids, legitCentroids...)
	centroids = append(centroids, fraudCentroids...)

	assignments := make([]uint16, len(vectors))
	for i, id := range legitIDs {
		assignments[id] = legitAssignments[i]
	}
	for i, id := range fraudIDs {
		assignments[id] = uint16(legitK + int(fraudAssignments[i]))
	}
	return centroids, assignments
}

type splitRange struct {
	start int
	end   int
}

func (r splitRange) len() int {
	return r.end - r.start
}

type splitRangeHeap []splitRange

func (h splitRangeHeap) Len() int           { return len(h) }
func (h splitRangeHeap) Less(i, j int) bool { return h[i].len() > h[j].len() }
func (h splitRangeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *splitRangeHeap) Push(x any) {
	*h = append(*h, x.(splitRange))
}

func (h *splitRangeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func widestDimForRange(vectors [][dim]float32, indices []int, r splitRange) int {
	var lo, hi [dim]float32
	for d := 0; d < dim; d++ {
		lo[d] = float32(math.Inf(1))
		hi[d] = float32(math.Inf(-1))
	}
	for _, id := range indices[r.start:r.end] {
		v := vectors[id]
		for d := 0; d < dim; d++ {
			if v[d] < lo[d] {
				lo[d] = v[d]
			}
			if v[d] > hi[d] {
				hi[d] = v[d]
			}
		}
	}
	bestDim := 0
	bestWidth := float32(math.Inf(-1))
	for d := 0; d < dim; d++ {
		width := hi[d] - lo[d]
		if width > bestWidth {
			bestWidth = width
			bestDim = d
		}
	}
	return bestDim
}

func trainWidestSplit(vectors [][dim]float32, k int) ([][dim]float32, []uint16) {
	log.Printf("widesplit init k=%d", k)
	if k <= 0 || k > len(vectors) {
		log.Fatalf("invalid widesplit k=%d for n=%d", k, len(vectors))
	}
	indices := make([]int, len(vectors))
	for i := range indices {
		indices[i] = i
	}
	ranges := splitRangeHeap{{start: 0, end: len(indices)}}
	heap.Init(&ranges)

	t0 := time.Now()
	for ranges.Len() < k {
		r := heap.Pop(&ranges).(splitRange)
		if r.len() <= 1 {
			heap.Push(&ranges, r)
			break
		}
		splitDim := widestDimForRange(vectors, indices, r)
		slice := indices[r.start:r.end]
		sort.Slice(slice, func(i, j int) bool {
			vi := vectors[slice[i]][splitDim]
			vj := vectors[slice[j]][splitDim]
			if vi == vj {
				return slice[i] < slice[j]
			}
			return vi < vj
		})
		mid := r.start + r.len()/2
		heap.Push(&ranges, splitRange{start: r.start, end: mid})
		heap.Push(&ranges, splitRange{start: mid, end: r.end})
		if ranges.Len()%512 == 0 {
			log.Printf("widesplit ranges=%d elapsed=%s", ranges.Len(), time.Since(t0))
		}
	}

	finalRanges := make([]splitRange, ranges.Len())
	for i := range finalRanges {
		finalRanges[i] = heap.Pop(&ranges).(splitRange)
	}
	sort.Slice(finalRanges, func(i, j int) bool {
		return finalRanges[i].start < finalRanges[j].start
	})

	centroids := make([][dim]float32, len(finalRanges))
	assignments := make([]uint16, len(vectors))
	for ci, r := range finalRanges {
		var sums [dim]float64
		for _, id := range indices[r.start:r.end] {
			assignments[id] = uint16(ci)
			for d := 0; d < dim; d++ {
				sums[d] += float64(vectors[id][d])
			}
		}
		inv := 1.0 / float64(r.len())
		for d := 0; d < dim; d++ {
			centroids[ci][d] = float32(sums[d] * inv)
		}
	}
	log.Printf("widesplit built ranges=%d elapsed=%s", len(finalRanges), time.Since(t0))
	return centroids, assignments
}

func quantize(v float32) int16 {
	x := int(math.Round(float64(v * scale)))
	if x < -32768 {
		return -32768
	}
	if x > 32767 {
		return 32767
	}
	return int16(x)
}

func quantizeWithInvScale(v float32, invScale float32) int16 {
	x := int(math.Round(float64(v * invScale)))
	if x < -32768 {
		return -32768
	}
	if x > 32767 {
		return 32767
	}
	return int16(x)
}

func perDimDequantScales(vectors [][dim]float32, dimOrder [dim]int) [dim]float32 {
	var out [dim]float32
	for d := 0; d < dim; d++ {
		maxAbs := float32(0)
		srcDim := dimOrder[d]
		for i := range vectors {
			v := vectors[i][srcDim]
			if v < 0 {
				v = -v
			}
			if v > maxAbs {
				maxAbs = v
			}
		}
		if maxAbs <= 0 || math.IsNaN(float64(maxAbs)) || math.IsInf(float64(maxAbs), 0) {
			out[d] = 1.0 / scale
			continue
		}
		inv := float32(32760.0) / maxAbs
		if inv < 1 {
			inv = 1
		}
		out[d] = 1.0 / inv
	}
	return out
}

func diagonalTransformAlpha(layout string) (float64, bool) {
	if layout == "diaglda" {
		return 8.0, true
	}
	if !strings.HasPrefix(layout, "diaglda") {
		return 0, false
	}
	suffix := strings.TrimPrefix(layout, "diaglda")
	if suffix == "" {
		return 8.0, true
	}
	alpha, err := strconv.ParseFloat(suffix, 64)
	if err != nil || alpha <= 0 {
		log.Fatalf("invalid diagonal transform layout %q", layout)
	}
	return alpha, true
}

func mahalanobisReg(layout string) (float64, bool) {
	if layout == "mahal" {
		return 0.05, true
	}
	if !strings.HasPrefix(layout, "mahal") {
		return 0, false
	}
	suffix := strings.TrimPrefix(layout, "mahal")
	if suffix == "" {
		return 0.05, true
	}
	reg, err := strconv.ParseFloat(suffix, 64)
	if err != nil || reg < 0 {
		log.Fatalf("invalid mahal layout %q", layout)
	}
	return reg, true
}

func activeHotDims(layout string) (int, bool) {
	const prefix = "activehot"
	if !strings.HasPrefix(layout, prefix) {
		return 0, false
	}
	suffix := strings.TrimPrefix(layout, prefix)
	if suffix == "" {
		return 10, true
	}
	n, err := strconv.Atoi(suffix)
	if err != nil || n < 6 || n > dim {
		log.Fatalf("invalid activehot layout %q; use activehotN with 6 <= N <= %d", layout, dim)
	}
	return n, true
}

func computeActiveHotTransform(active int) [dim * dim]float32 {
	var matrix [dim * dim]float32
	for r := 0; r < active; r++ {
		matrix[r*dim+hot6DimOrder[r]] = 1
	}
	for r := active; r < dim; r++ {
		for c := 0; c < dim; c++ {
			matrix[r*dim+c] = 0
		}
	}
	log.Printf("activehot active_dims=%d order=%v", active, hot6DimOrder)
	return matrix
}

func computeDiagonalLDATransform(vectors [][dim]float32, labels []byte, alpha float64) [dim]float32 {
	var sum0, sum1 [dim]float64
	var sumSq0, sumSq1 [dim]float64
	var n0, n1 int
	for i := range vectors {
		if fraudLabelByte(labels[i]) == 0 {
			n0++
			for d := 0; d < dim; d++ {
				v := float64(vectors[i][d])
				sum0[d] += v
				sumSq0[d] += v * v
			}
		} else {
			n1++
			for d := 0; d < dim; d++ {
				v := float64(vectors[i][d])
				sum1[d] += v
				sumSq1[d] += v * v
			}
		}
	}
	if n0 == 0 || n1 == 0 {
		log.Fatalf("diaglda requires both classes: n0=%d n1=%d", n0, n1)
	}

	var score [dim]float64
	maxScore := 0.0
	for d := 0; d < dim; d++ {
		m0 := sum0[d] / float64(n0)
		m1 := sum1[d] / float64(n1)
		v0 := sumSq0[d]/float64(n0) - m0*m0
		v1 := sumSq1[d]/float64(n1) - m1*m1
		if v0 < 1e-12 {
			v0 = 1e-12
		}
		if v1 < 1e-12 {
			v1 = 1e-12
		}
		score[d] = math.Abs(m1-m0) / math.Sqrt(0.5*(v0+v1))
		if score[d] > maxScore {
			maxScore = score[d]
		}
	}
	if maxScore <= 0 || math.IsNaN(maxScore) || math.IsInf(maxScore, 0) {
		maxScore = 1
	}

	var weights [dim]float64
	for d := 0; d < dim; d++ {
		weights[d] = 1.0 + alpha*(score[d]/maxScore)
	}

	maxAbs := 0.0
	for i := range vectors {
		for d := 0; d < dim; d++ {
			v := math.Abs(float64(vectors[i][d]) * weights[d])
			if v > maxAbs {
				maxAbs = v
			}
		}
	}
	global := 1.0
	if maxAbs > 0 && !math.IsNaN(maxAbs) && !math.IsInf(maxAbs, 0) {
		global = 3.0 / maxAbs
	}

	var scales [dim]float32
	for d := 0; d < dim; d++ {
		scales[d] = float32(weights[d] * global)
		log.Printf("diaglda d=%d fisher=%.6f weight=%.6f query_scale=%.9g",
			d, score[d], weights[d], scales[d])
	}
	return scales
}

func applyDiagonalTransform(vectors [][dim]float32, scales [dim]float32) {
	for i := range vectors {
		for d := 0; d < dim; d++ {
			vectors[i][d] *= scales[d]
		}
	}
}

func computeMahalanobisTransform(vectors [][dim]float32, reg float64) [dim * dim]float32 {
	var mean [dim]float64
	for i := range vectors {
		for d := 0; d < dim; d++ {
			mean[d] += float64(vectors[i][d])
		}
	}
	invN := 1.0 / float64(len(vectors))
	for d := 0; d < dim; d++ {
		mean[d] *= invN
	}

	var cov [dim][dim]float64
	for i := range vectors {
		for r := 0; r < dim; r++ {
			vr := float64(vectors[i][r]) - mean[r]
			for c := 0; c <= r; c++ {
				vc := float64(vectors[i][c]) - mean[c]
				cov[r][c] += vr * vc
			}
		}
	}
	for r := 0; r < dim; r++ {
		for c := 0; c <= r; c++ {
			v := cov[r][c] * invN
			cov[r][c] = v
			cov[c][r] = v
		}
	}

	var eig [dim][dim]float64
	for d := 0; d < dim; d++ {
		eig[d][d] = 1
	}
	for iter := 0; iter < 96; iter++ {
		p, q := 0, 1
		maxAbs := math.Abs(cov[p][q])
		for r := 0; r < dim; r++ {
			for c := r + 1; c < dim; c++ {
				if v := math.Abs(cov[r][c]); v > maxAbs {
					maxAbs = v
					p, q = r, c
				}
			}
		}
		if maxAbs < 1e-12 {
			break
		}
		app := cov[p][p]
		aqq := cov[q][q]
		apq := cov[p][q]
		theta := 0.5 * math.Atan2(2*apq, aqq-app)
		cs := math.Cos(theta)
		sn := math.Sin(theta)

		for k := 0; k < dim; k++ {
			akp := cov[k][p]
			akq := cov[k][q]
			cov[k][p] = cs*akp - sn*akq
			cov[k][q] = sn*akp + cs*akq
		}
		for k := 0; k < dim; k++ {
			apk := cov[p][k]
			aqk := cov[q][k]
			cov[p][k] = cs*apk - sn*aqk
			cov[q][k] = sn*apk + cs*aqk
		}
		cov[p][q] = 0
		cov[q][p] = 0

		for k := 0; k < dim; k++ {
			vkp := eig[k][p]
			vkq := eig[k][q]
			eig[k][p] = cs*vkp - sn*vkq
			eig[k][q] = sn*vkp + cs*vkq
		}
	}

	var mat64 [dim][dim]float64
	for e := 0; e < dim; e++ {
		lambda := cov[e][e]
		if lambda < 1e-9 {
			lambda = 1e-9
		}
		weight := 1.0 / math.Sqrt(lambda+reg)
		for src := 0; src < dim; src++ {
			mat64[e][src] = eig[src][e] * weight
		}
	}

	maxAbs := 0.0
	for i := range vectors {
		for r := 0; r < dim; r++ {
			sum := 0.0
			for c := 0; c < dim; c++ {
				sum += mat64[r][c] * float64(vectors[i][c])
			}
			if a := math.Abs(sum); a > maxAbs {
				maxAbs = a
			}
		}
	}
	global := 1.0
	if maxAbs > 0 && !math.IsNaN(maxAbs) && !math.IsInf(maxAbs, 0) {
		global = 3.0 / maxAbs
	}

	var out [dim * dim]float32
	for r := 0; r < dim; r++ {
		for c := 0; c < dim; c++ {
			out[r*dim+c] = float32(mat64[r][c] * global)
		}
		log.Printf("mahal row=%d lambda=%.9g scale=%.9g", r, cov[r][r], global)
	}
	return out
}

func applyMatrixTransform(vectors [][dim]float32, matrix [dim * dim]float32) {
	for i := range vectors {
		var out [dim]float32
		for r := 0; r < dim; r++ {
			var sum float32
			for c := 0; c < dim; c++ {
				sum += matrix[r*dim+c] * vectors[i][c]
			}
			out[r] = sum
		}
		vectors[i] = out
	}
}

func dimensionOrder(layout string) [dim]int {
	switch layout {
	case "dimhot6":
		return hot6DimOrder
	default:
		return naturalDimOrder
	}
}

func percentile(sorted []float32, p float64) float32 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func logIndexQuality(vectors [][dim]float32, clusters [][]uint32, centroids [][dim]float32) {
	clusterMax := make([]float32, 0, len(clusters))
	blockWidths := make([]float32, 0, (len(vectors)+15)/16)
	blockMaxRadii := make([]float32, 0, (len(vectors)+15)/16)
	emptyClusters := 0
	minSize := math.MaxInt
	maxSize := 0
	totalSize := 0

	for ci, ids := range clusters {
		if len(ids) == 0 {
			emptyClusters++
			continue
		}
		if len(ids) < minSize {
			minSize = len(ids)
		}
		if len(ids) > maxSize {
			maxSize = len(ids)
		}
		totalSize += len(ids)

		maxDist := float32(0)
		for _, id := range ids {
			d := distSq(&vectors[id], &centroids[ci])
			if d > maxDist {
				maxDist = d
			}
		}
		clusterMax = append(clusterMax, maxDist)

		for start := 0; start < len(ids); start += 16 {
			end := start + 16
			if end > len(ids) {
				end = len(ids)
			}
			minDist := float32(math.Inf(1))
			maxDist = 0
			for _, id := range ids[start:end] {
				d := distSq(&vectors[id], &centroids[ci])
				if d < minDist {
					minDist = d
				}
				if d > maxDist {
					maxDist = d
				}
			}
			blockWidths = append(blockWidths, maxDist-minDist)
			blockMaxRadii = append(blockMaxRadii, maxDist)
		}
	}

	sort.Slice(clusterMax, func(i, j int) bool { return clusterMax[i] < clusterMax[j] })
	sort.Slice(blockWidths, func(i, j int) bool { return blockWidths[i] < blockWidths[j] })
	sort.Slice(blockMaxRadii, func(i, j int) bool { return blockMaxRadii[i] < blockMaxRadii[j] })

	nonEmpty := len(clusterMax)
	avgSize := 0.0
	if nonEmpty > 0 {
		avgSize = float64(totalSize) / float64(nonEmpty)
	}
	log.Printf(
		"quality clusters non_empty=%d empty=%d min=%d avg=%.2f max=%d",
		nonEmpty, emptyClusters, minSize, avgSize, maxSize,
	)
	log.Printf(
		"quality cluster_radius_sq p50=%.8f p90=%.8f p95=%.8f p99=%.8f max=%.8f",
		percentile(clusterMax, 0.50), percentile(clusterMax, 0.90), percentile(clusterMax, 0.95), percentile(clusterMax, 0.99), percentile(clusterMax, 1),
	)
	log.Printf(
		"quality block_width_sq p50=%.8f p90=%.8f p95=%.8f p99=%.8f max=%.8f blocks=%d",
		percentile(blockWidths, 0.50), percentile(blockWidths, 0.90), percentile(blockWidths, 0.95), percentile(blockWidths, 0.99), percentile(blockWidths, 1), len(blockWidths),
	)
	log.Printf(
		"quality block_max_radius_sq p50=%.8f p90=%.8f p95=%.8f p99=%.8f max=%.8f",
		percentile(blockMaxRadii, 0.50), percentile(blockMaxRadii, 0.90), percentile(blockMaxRadii, 0.95), percentile(blockMaxRadii, 0.99), percentile(blockMaxRadii, 1),
	)
}

func writeIndex(path string, vectors [][dim]float32, labels []byte, assignments []uint16, centroids [][dim]float32, layout string, queryScales *[dim]float32, queryMatrix *[dim * dim]float32) {
	k := len(centroids)
	dimOrder := dimensionOrder(layout)
	perDimScale := layout == "perdim"
	var dequantScales [dim]float32
	if perDimScale {
		log.Print("applying per-dimension int16 quantization")
		dequantScales = perDimDequantScales(vectors, dimOrder)
		for d := 0; d < dim; d++ {
			log.Printf("perdim d=%d source_dim=%d dequant_scale=%.9g inv_scale=%.3f",
				d, dimOrder[d], dequantScales[d], 1.0/dequantScales[d])
		}
	}
	clusters := make([][]uint32, k)
	for i, ci := range assignments {
		clusters[ci] = append(clusters[ci], uint32(i))
	}

	type clusterItem struct {
		id   uint32
		dist float32
	}
	for ci := 0; ci < k; ci++ {
		ids := clusters[ci]
		if len(ids) < 2 {
			continue
		}
		items := make([]clusterItem, len(ids))
		for i, id := range ids {
			items[i] = clusterItem{
				id:   id,
				dist: distSq(&vectors[id], &centroids[ci]),
			}
		}
		sort.Slice(items, func(i, j int) bool {
			return items[i].dist < items[j].dist
		})
		for i := range items {
			ids[i] = items[i].id
		}
	}

	switch {
	case layout == "" || layout == "radius" || layout == "balance2" || layout == "sample" || layout == "samplebalance2" || layout == "widesplit" || layout == "perdim" || strings.HasPrefix(layout, "classsplit"):
	case queryScales != nil:
		log.Printf("applying diagonal query transform layout=%s", layout)
	case queryMatrix != nil:
		log.Printf("applying matrix query transform layout=%s", layout)
	case layout == "dimhot6":
		log.Print("applying dimhot6 dimension layout")
	case layout == "prefix6-halves":
		log.Print("applying prefix6-halves slot layout")
		for ci := 0; ci < k; ci++ {
			ids := clusters[ci]
			for start := 0; start < len(ids); start += 16 {
				end := start + 16
				if end > len(ids) {
					end = len(ids)
				}
				blockIDs := ids[start:end]
				sort.Slice(blockIDs, func(i, j int) bool {
					di := prefix6DistSq(&vectors[blockIDs[i]], &centroids[ci])
					dj := prefix6DistSq(&vectors[blockIDs[j]], &centroids[ci])
					if di == dj {
						return blockIDs[i] < blockIDs[j]
					}
					return di < dj
				})
			}
		}
	default:
		log.Fatalf("unknown index layout %q", layout)
	}

	logIndexQuality(vectors, clusters, centroids)

	offsets := make([]uint32, k+1)
	for ci := 0; ci < k; ci++ {
		sz := uint32(len(clusters[ci]))
		offsets[ci+1] = offsets[ci] + (sz+15)/16
	}

	totalBlocks := int(offsets[k])
	paddedN := totalBlocks * 16
	outLabels := make([]byte, paddedN)
	outBlocks := make([]int16, totalBlocks*dim*16)

	for ci := 0; ci < k; ci++ {
		blockStart := int(offsets[ci])
		vecs := clusters[ci]
		nBlocks := int(offsets[ci+1] - offsets[ci])
		for bk := 0; bk < nBlocks; bk++ {
			blockBase := (blockStart + bk) * dim * 16
			labelBase := (blockStart + bk) * 16
			for slot := 0; slot < 16; slot++ {
				pos := bk*16 + slot
				if pos < len(vecs) {
					vi := int(vecs[pos])
					for d := 0; d < dim; d++ {
						v := vectors[vi][dimOrder[d]]
						if perDimScale {
							outBlocks[blockBase+d*16+slot] = quantizeWithInvScale(v, 1.0/dequantScales[d])
						} else {
							outBlocks[blockBase+d*16+slot] = quantize(v)
						}
					}
					outLabels[labelBase+slot] = labels[vi]
				} else {
					for d := 0; d < dim; d++ {
						outBlocks[blockBase+d*16+slot] = math.MaxInt16
					}
				}
			}
		}
	}

	centroidsT := make([]float32, dim*k)
	for ci := 0; ci < k; ci++ {
		for d := 0; d < dim; d++ {
			centroidsT[d*k+ci] = centroids[ci][dimOrder[d]]
		}
	}

	if err := os.MkdirAll("service", 0o755); err != nil {
		log.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	bw := bufio.NewWriterSize(f, 1<<20)
	gz, err := gzip.NewWriterLevel(bw, gzip.BestCompression)
	if err != nil {
		log.Fatal(err)
	}

	magic := "IVF1"
	if perDimScale {
		magic = "IVS1"
	}
	if queryScales != nil {
		magic = "IVD1"
	}
	if queryMatrix != nil {
		magic = "IVM1"
	}
	if _, err := gz.Write([]byte(magic)); err != nil {
		log.Fatal(err)
	}
	writeU32(gz, uint32(len(vectors)))
	writeU32(gz, uint32(k))
	writeU32(gz, dim)
	must(binary.Write(gz, binary.LittleEndian, centroidsT))
	if queryScales != nil {
		must(binary.Write(gz, binary.LittleEndian, *queryScales))
	}
	if queryMatrix != nil {
		must(binary.Write(gz, binary.LittleEndian, *queryMatrix))
	}
	if perDimScale {
		must(binary.Write(gz, binary.LittleEndian, dequantScales))
	}
	must(binary.Write(gz, binary.LittleEndian, offsets))
	if _, err := gz.Write(outLabels); err != nil {
		log.Fatal(err)
	}
	must(binary.Write(gz, binary.LittleEndian, outBlocks))
	must(gz.Close())
	must(bw.Flush())
}

func writeU32(w *gzip.Writer, v uint32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	_, err := w.Write(buf[:])
	must(err)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	input := "service/references.json"
	output := "service/index.bin.gz"
	k := defaultK
	nIter := defaultNIter
	sampleSize := defaultSampleSize
	layout := "radius"
	seed := uint64(defaultSeed)
	if len(os.Args) > 1 {
		input = os.Args[1]
	}
	if len(os.Args) > 2 {
		output = os.Args[2]
	}
	if len(os.Args) > 3 {
		k = parsePositiveInt(os.Args[3], "k")
	}
	if len(os.Args) > 4 {
		nIter = parsePositiveInt(os.Args[4], "nIter")
	}
	if len(os.Args) > 5 {
		sampleSize = parsePositiveInt(os.Args[5], "sampleSize")
	}
	if len(os.Args) > 6 {
		layout = os.Args[6]
	}
	if len(os.Args) > 7 {
		seed = parseUint64(os.Args[7], "seed")
	}
	if k > 1<<16 {
		log.Fatalf("k=%d exceeds uint16 assignment capacity", k)
	}

	t0 := time.Now()
	log.Print("loading dataset")
	vectors, labels := loadDataset(input)
	log.Printf("loaded %d vectors in %s", len(vectors), time.Since(t0))

	var queryScales *[dim]float32
	var queryMatrix *[dim * dim]float32
	if alpha, ok := diagonalTransformAlpha(layout); ok {
		log.Printf("applying supervised diagonal transform layout=%s alpha=%.3f", layout, alpha)
		scales := computeDiagonalLDATransform(vectors, labels, alpha)
		applyDiagonalTransform(vectors, scales)
		queryScales = &scales
	}
	if reg, ok := mahalanobisReg(layout); ok {
		log.Printf("applying full matrix whitening layout=%s reg=%.6f", layout, reg)
		matrix := computeMahalanobisTransform(vectors, reg)
		applyMatrixTransform(vectors, matrix)
		queryMatrix = &matrix
	}
	if active, ok := activeHotDims(layout); ok {
		log.Printf("applying active hot-dim transform layout=%s active=%d", layout, active)
		matrix := computeActiveHotTransform(active)
		applyMatrixTransform(vectors, matrix)
		queryMatrix = &matrix
	}

	var centroids [][dim]float32
	var assignments []uint16
	if layout == "widesplit" {
		centroids, assignments = trainWidestSplit(vectors, k)
	} else if _, ok := classSplitFraudPercent(layout); ok {
		centroids, assignments = trainClassSplit(vectors, labels, k, nIter, sampleSize, seed, layout)
	} else if layout == "sample" || layout == "samplebalance2" {
		centroids, assignments = trainSampleKMeans(vectors, k, nIter, sampleSize, seed, "all")
		if layout == "samplebalance2" {
			rebalanceAssignmentsTop2(vectors, centroids, assignments)
		}
	} else {
		centroids, assignments = trainKMeans(vectors, k, nIter, sampleSize, seed, "all")
		if layout == "balance2" {
			rebalanceAssignmentsTop2(vectors, centroids, assignments)
		}
	}

	log.Printf("writing index layout=%s", layout)
	writeIndex(output, vectors, labels, assignments, centroids, layout, queryScales, queryMatrix)
	log.Printf("done in %s", time.Since(t0))
}

func parsePositiveInt(s, name string) int {
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		log.Fatalf("invalid %s %q", name, s)
	}
	return v
}

func parseUint64(s, name string) uint64 {
	v, err := strconv.ParseUint(s, 0, 64)
	if err != nil {
		log.Fatalf("invalid %s %q", name, s)
	}
	return v
}
