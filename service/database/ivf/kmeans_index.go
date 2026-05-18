package ivf

import (
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"time"
	"unsafe"
)

const (
	maxCentroids    = 4096
	indexDim        = 14
	quickProbe      = 8
	expandedProbe   = 20
	maxProbe        = 32
	vectorsPerBlock = 16
	blockStride     = indexDim * vectorsPerBlock
	vectorScale     = 0.0001
	initialTopDist  = float32(1.0e30)
)

func LoadKMeansIndex(path string) (*IVF, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()

	var magic [4]byte
	if _, err := io.ReadFull(gz, magic[:]); err != nil {
		return nil, err
	}
	if string(magic[:]) != "IVF1" {
		return nil, fmt.Errorf("invalid kmeans index magic")
	}

	n, err := readU32(gz)
	if err != nil {
		return nil, err
	}
	_ = n
	k, err := readU32(gz)
	if err != nil {
		return nil, err
	}
	d, err := readU32(gz)
	if err != nil {
		return nil, err
	}
	if d != indexDim {
		return nil, fmt.Errorf("invalid kmeans index dimension %d", d)
	}

	centroids := make([]float32, int(k)*indexDim)
	if err := binary.Read(gz, binary.LittleEndian, centroids); err != nil {
		return nil, err
	}

	offsets := make([]uint32, int(k)+1)
	if err := binary.Read(gz, binary.LittleEndian, offsets); err != nil {
		return nil, err
	}

	totalBlocks := int(offsets[k])
	paddedN := totalBlocks * vectorsPerBlock

	labels := make([]byte, paddedN)
	if _, err := io.ReadFull(gz, labels); err != nil {
		return nil, err
	}

	rawBlocks := make([]byte, totalBlocks*blockStride*2)
	if _, err := io.ReadFull(gz, rawBlocks); err != nil {
		return nil, err
	}
	blocks := unsafe.Slice((*int16)(unsafe.Pointer(&rawBlocks[0])), len(rawBlocks)/2)
	var blockMinRadii, blockMaxRadii []float32
	if os.Getenv("IVF_BLOCK_BOUNDS") != "0" {
		blockMinRadii, blockMaxRadii = buildBlockRadii(centroids, int(k), blocks, offsets)
	}

	return &IVF{
		Labels:        labels,
		Offsets:       offsets,
		Centroids:     centroids,
		BlockMinRadii: blockMinRadii,
		BlockMaxRadii: blockMaxRadii,
		Blocks:        blocks,
		RawBlocks:     rawBlocks,
		K:             int(k),
		IDs:           make([]uint32, 0),
		Vectors:       make([]int16, 0),
	}, nil
}

func readU32(r io.Reader) (uint32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf[:]), nil
}

func topCentroids(q *Vector, centroids []float32, k int, nprobe int, out []int, dists *[maxCentroids]float32) {
	var topD [maxProbe]float32
	var topI [maxProbe]int
	for i := 0; i < nprobe; i++ {
		topD[i] = float32(math.Inf(1))
	}

	centroidDists(q, centroids, k, dists)

	for ci := 0; ci < k; ci++ {
		dist := dists[ci]
		if dist >= topD[nprobe-1] {
			continue
		}
		pos := nprobe - 1
		for pos > 0 && dist < topD[pos-1] {
			topD[pos] = topD[pos-1]
			topI[pos] = topI[pos-1]
			pos--
		}
		topD[pos] = dist
		topI[pos] = ci
	}

	copy(out[:nprobe], topI[:nprobe])
}

func (db *IVF) scanProbes(qv *Vector, probes *[maxProbe]int, from, to int, topDist *[5]float32, topLabel *[5]uint8, worst *int) {
	for i := from; i < to; i++ {
		ci := probes[i]
		start := int(db.Offsets[ci])
		end := int(db.Offsets[ci+1])
		scanBlocks(qv, db.Blocks, db.Labels, start, end, topDist, topLabel, worst)
	}
}

func (db *IVF) scanProbesBounded(qv *Vector, probes *[maxProbe]int, dists *[maxCentroids]float32, from, to int, topDist *[5]float32, topLabel *[5]uint8, worst *int) {
	if len(db.BlockMinRadii) != len(db.BlockMaxRadii) || len(db.BlockMinRadii) == 0 {
		db.scanProbes(qv, probes, from, to, topDist, topLabel, worst)
		return
	}

	for i := from; i < to; i++ {
		ci := probes[i]
		start := int(db.Offsets[ci])
		end := int(db.Offsets[ci+1])
		start, end = db.boundProbeRange(start, end, dists[ci], topDist[*worst])
		if start >= end {
			continue
		}
		scanBlocks(qv, db.Blocks, db.Labels, start, end, topDist, topLabel, worst)
	}
}

func (db *IVF) scanProbesTrace(qv *Vector, probes *[maxProbe]int, from, to int, topDist *[5]float32, topLabel *[5]uint8, worst *int, counts *PruneCounts) {
	for i := from; i < to; i++ {
		ci := probes[i]
		start := int(db.Offsets[ci])
		end := int(db.Offsets[ci+1])
		scanBlocksTrace(qv, db.Blocks, db.Labels, start, end, topDist, topLabel, worst, counts)
	}
}

func buildBlockRadii(centroids []float32, k int, blocks []int16, offsets []uint32) ([]float32, []float32) {
	totalBlocks := int(offsets[k])
	minRadii := make([]float32, totalBlocks)
	maxRadii := make([]float32, totalBlocks)
	for ci := 0; ci < k; ci++ {
		for block := int(offsets[ci]); block < int(offsets[ci+1]); block++ {
			base := block * blockStride
			minDist := float32(math.Inf(1))
			maxDist := float32(0)
			for slot := 0; slot < vectorsPerBlock; slot++ {
				if isPaddingSlot(blocks, base, slot) {
					continue
				}
				dist := float32(0)
				for d := 0; d < indexDim; d++ {
					ref := float32(blocks[base+d*vectorsPerBlock+slot]) * vectorScale
					diff := ref - centroids[d*k+ci]
					dist += diff * diff
				}
				if dist < minDist {
					minDist = dist
				}
				if dist > maxDist {
					maxDist = dist
				}
			}
			if minDist == float32(math.Inf(1)) {
				minDist = 0
			}
			minRadii[block] = float32(math.Sqrt(float64(minDist)))
			maxRadii[block] = float32(math.Sqrt(float64(maxDist)))
		}
	}
	return minRadii, maxRadii
}

func isPaddingSlot(blocks []int16, base, slot int) bool {
	const padding = int16(math.MaxInt16)
	for d := 0; d < indexDim; d++ {
		if blocks[base+d*vectorsPerBlock+slot] != padding {
			return false
		}
	}
	return true
}

func (db *IVF) boundProbeRange(start, end int, centerDistSq, worstDistSq float32) (int, int) {
	if start >= end || worstDistSq >= initialTopDist {
		return start, end
	}

	centerDist := float32(math.Sqrt(float64(centerDistSq)))
	bestDist := float32(math.Sqrt(float64(worstDistSq)))
	lowerRadius := centerDist - bestDist
	upperRadius := centerDist + bestDist

	if lowerRadius > 0 {
		lo, hi := start, end
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if db.BlockMaxRadii[mid] < lowerRadius {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		start = lo
	}

	lo, hi := start, end
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if db.BlockMinRadii[mid] <= upperRadius {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	end = lo

	return start, end
}

func (db *IVF) countProbeBlocks(probes *[maxProbe]int, from, to int) int {
	blocks := 0
	for i := from; i < to; i++ {
		ci := probes[i]
		blocks += int(db.Offsets[ci+1] - db.Offsets[ci])
	}
	return blocks
}

func countFrauds(topLabel *[5]uint8) int {
	frauds := 0
	for i := 0; i < 5; i++ {
		if topLabel[i] == Fraud {
			frauds++
		}
	}
	return frauds
}

func countFraudsBridge(topLabel *[5]uint8) int {
	return int(topLabel[0] + topLabel[1] + topLabel[2] + topLabel[3] + topLabel[4])
}

func selectTop8FromDists(k int, out *[maxProbe]int, dists *[maxCentroids]float32) {
	selectTop8AVX2(k, out, dists)
}

func selectTop8FromDistsPure(k int, out *[maxProbe]int, dists *[maxCentroids]float32) {
	topD := [quickProbe]float32{
		initialTopDist, initialTopDist, initialTopDist, initialTopDist,
		initialTopDist, initialTopDist, initialTopDist, initialTopDist,
	}
	var topI [quickProbe]int

	for ci := 0; ci < k; ci++ {
		dist := dists[ci]
		if dist >= topD[quickProbe-1] {
			continue
		}
		pos := quickProbe - 1
		for pos > 0 && dist < topD[pos-1] {
			topD[pos] = topD[pos-1]
			topI[pos] = topI[pos-1]
			pos--
		}
		topD[pos] = dist
		topI[pos] = ci
	}

	copy(out[:quickProbe], topI[:])
}

func selectTop20FromDists(k int, out *[maxProbe]int, dists *[maxCentroids]float32) {
	selectTop20AVX2(k, out, dists)
}

func selectTop20FromDistsPure(k int, out *[maxProbe]int, dists *[maxCentroids]float32) {
	topD := [expandedProbe]float32{
		initialTopDist, initialTopDist, initialTopDist, initialTopDist,
		initialTopDist, initialTopDist, initialTopDist, initialTopDist,
		initialTopDist, initialTopDist, initialTopDist, initialTopDist,
		initialTopDist, initialTopDist, initialTopDist, initialTopDist,
		initialTopDist, initialTopDist, initialTopDist, initialTopDist,
	}
	var topI [expandedProbe]int

	for ci := 0; ci < k; ci++ {
		dist := dists[ci]
		if dist >= topD[expandedProbe-1] {
			continue
		}
		pos := expandedProbe - 1
		for pos > 0 && dist < topD[pos-1] {
			topD[pos] = topD[pos-1]
			topI[pos] = topI[pos-1]
			pos--
		}
		topD[pos] = dist
		topI[pos] = ci
	}

	copy(out[:expandedProbe], topI[:])
}

func resetTop5(ws *SearchWorkspace) {
	ws.TopDist[0] = initialTopDist
	ws.TopDist[1] = initialTopDist
	ws.TopDist[2] = initialTopDist
	ws.TopDist[3] = initialTopDist
	ws.TopDist[4] = initialTopDist
	ws.TopLabel[0] = 0
	ws.TopLabel[1] = 0
	ws.TopLabel[2] = 0
	ws.TopLabel[3] = 0
	ws.TopLabel[4] = 0
}

func (db *IVF) rescoreQuantizedBridge(qv *Vector, ws *SearchWorkspace) int {
	for i := 0; i < indexDim; i++ {
		x := int(qv[i]*Scale + 0.5)
		if qv[i] < 0 {
			x = int(qv[i]*Scale - 0.5)
		}
		if x < -32768 {
			x = -32768
		} else if x > 32767 {
			x = 32767
		}
		ws.Quantized[i] = float32(int16(x)) * vectorScale
	}

	resetTop5(ws)
	worst := 0

	db.scanProbes(&ws.Quantized, &ws.Probes, 0, expandedProbe, &ws.TopDist, &ws.TopLabel, &worst)
	return countFraudsBridge(&ws.TopLabel)
}

func (db *IVF) FraudCount5Bridge(q *Vector, ws *SearchWorkspace) int {
	if db.K == 0 {
		return db.FraudCount5WithWorkspace(q, ws)
	}

	centroidDists(q, db.Centroids, db.K, &ws.CentroidDists)
	selectTop8FromDists(db.K, &ws.Probes, &ws.CentroidDists)

	resetTop5(ws)
	worst := 0

	db.scanProbesBounded(q, &ws.Probes, &ws.CentroidDists, 0, quickProbe, &ws.TopDist, &ws.TopLabel, &worst)
	fast := countFraudsBridge(&ws.TopLabel)
	if fast != 2 && fast != 3 {
		return fast
	}
	selectTop20FromDists(db.K, &ws.Probes, &ws.CentroidDists)
	return db.rescoreQuantizedBridge(q, ws)
}

func (db *IVF) FraudCount5BridgeProfile(q *Vector, ws *SearchWorkspace, prof *BridgeProfile) int {
	if prof == nil {
		return db.FraudCount5Bridge(q, ws)
	}
	if db.K == 0 {
		return db.FraudCount5WithWorkspace(q, ws)
	}

	prof.Calls++

	start := time.Now()
	centroidDists(q, db.Centroids, db.K, &ws.CentroidDists)
	prof.CentroidNS += uint64(time.Since(start).Nanoseconds())

	start = time.Now()
	selectTop8FromDists(db.K, &ws.Probes, &ws.CentroidDists)
	prof.Select8NS += uint64(time.Since(start).Nanoseconds())
	prof.QuickBlocks += uint64(db.countProbeBlocks(&ws.Probes, 0, quickProbe))

	resetTop5(ws)
	worst := 0

	start = time.Now()
	db.scanProbesBounded(q, &ws.Probes, &ws.CentroidDists, 0, quickProbe, &ws.TopDist, &ws.TopLabel, &worst)
	prof.QuickScanNS += uint64(time.Since(start).Nanoseconds())

	fast := countFraudsBridge(&ws.TopLabel)
	if fast >= 0 && fast < len(prof.FastBins) {
		prof.FastBins[fast]++
	}
	if fast != 2 && fast != 3 {
		prof.QuickOnly++
		return fast
	}

	prof.Rescore++
	rescoreStart := time.Now()

	start = time.Now()
	selectTop20FromDists(db.K, &ws.Probes, &ws.CentroidDists)
	prof.Select20NS += uint64(time.Since(start).Nanoseconds())
	prof.RescoreBlocks += uint64(db.countProbeBlocks(&ws.Probes, 0, expandedProbe))

	start = time.Now()
	for i := 0; i < indexDim; i++ {
		x := int(q[i]*Scale + 0.5)
		if q[i] < 0 {
			x = int(q[i]*Scale - 0.5)
		}
		if x < -32768 {
			x = -32768
		} else if x > 32767 {
			x = 32767
		}
		ws.Quantized[i] = float32(int16(x)) * vectorScale
	}
	prof.VectorizeNS += uint64(time.Since(start).Nanoseconds())

	resetTop5(ws)
	worst = 0

	start = time.Now()
	db.scanProbes(&ws.Quantized, &ws.Probes, 0, expandedProbe, &ws.TopDist, &ws.TopLabel, &worst)
	prof.RescoreScanNS += uint64(time.Since(start).Nanoseconds())

	out := countFraudsBridge(&ws.TopLabel)
	if out >= 0 && out < len(prof.RescoreBins) {
		prof.RescoreBins[out]++
	}
	prof.RescoreNS += uint64(time.Since(rescoreStart).Nanoseconds())
	return out
}

func (db *IVF) rescoreQuantized(qv *Vector, ws *SearchWorkspace, nprobe int) int {
	q := Quantize(qv)
	for i := 0; i < indexDim; i++ {
		ws.Quantized[i] = float32(q[i]) * vectorScale
	}

	for i := 0; i < 5; i++ {
		ws.TopDist[i] = float32(math.Inf(1))
		ws.TopLabel[i] = 0
	}
	worst := 0

	db.scanProbes(&ws.Quantized, &ws.Probes, 0, nprobe, &ws.TopDist, &ws.TopLabel, &worst)
	return countFrauds(&ws.TopLabel)
}

func (db *IVF) FraudCount5TraceProbes(q *Vector, ws *SearchWorkspace, quick, expanded int) (int, int) {
	if db.K > 0 {
		if expanded < 1 {
			expanded = 1
		} else if expanded > maxProbe {
			expanded = maxProbe
		}
		if quick < 1 {
			quick = 1
		} else if quick > expanded {
			quick = expanded
		}
		if expanded < quick {
			expanded = quick
		}

		topCentroids(q, db.Centroids, db.K, expanded, ws.Probes[:], &ws.CentroidDists)

		for i := 0; i < 5; i++ {
			ws.TopDist[i] = float32(math.Inf(1))
			ws.TopLabel[i] = 0
		}
		worst := 0

		db.scanProbesBounded(q, &ws.Probes, &ws.CentroidDists, 0, quick, &ws.TopDist, &ws.TopLabel, &worst)
		fast := countFrauds(&ws.TopLabel)
		if fast != 2 && fast != 3 {
			return fast, 0
		}
		return db.rescoreQuantized(q, ws, expanded), 2
	}

	var w Workspace
	pairs := db.SearchK(q, &w, 5)
	frauds := 0
	for i := 0; i < len(pairs); i++ {
		if db.Labels[pairs[i].ID] == Fraud {
			frauds++
		}
	}
	return frauds, 0
}

func (db *IVF) FraudCount5Trace(q *Vector, ws *SearchWorkspace, quick int) (int, int) {
	return db.FraudCount5TraceProbes(q, ws, quick, expandedProbe)
}

func (db *IVF) FraudCount5TraceDetailed(q *Vector, ws *SearchWorkspace, quick, expanded int) SearchTrace {
	trace := SearchTrace{}
	if db.K > 0 {
		if expanded < 1 {
			expanded = 1
		} else if expanded > maxProbe {
			expanded = maxProbe
		}
		if quick < 1 {
			quick = 1
		} else if quick > expanded {
			quick = expanded
		}
		if expanded < quick {
			expanded = quick
		}

		topCentroids(q, db.Centroids, db.K, expanded, ws.Probes[:], &ws.CentroidDists)
		trace.QuickBlocks = db.countProbeBlocks(&ws.Probes, 0, quick)

		for i := 0; i < 5; i++ {
			ws.TopDist[i] = float32(math.Inf(1))
			ws.TopLabel[i] = 0
		}
		worst := 0

		db.scanProbesTrace(q, &ws.Probes, 0, quick, &ws.TopDist, &ws.TopLabel, &worst, &trace.QuickPrune)
		fast := countFrauds(&ws.TopLabel)
		trace.QuickFrauds = fast
		if fast != 2 && fast != 3 {
			trace.Path = 0
			trace.Frauds = fast
			return trace
		}

		trace.Path = 2
		trace.RescoreBlocks = db.countProbeBlocks(&ws.Probes, 0, expanded)
		qv := Quantize(q)
		for i := 0; i < indexDim; i++ {
			ws.Quantized[i] = float32(qv[i]) * vectorScale
		}
		for i := 0; i < 5; i++ {
			ws.TopDist[i] = float32(math.Inf(1))
			ws.TopLabel[i] = 0
		}
		worst = 0
		db.scanProbesTrace(&ws.Quantized, &ws.Probes, 0, expanded, &ws.TopDist, &ws.TopLabel, &worst, &trace.RescorePrune)
		trace.RescoreFrauds = countFrauds(&ws.TopLabel)
		trace.Frauds = trace.RescoreFrauds
		return trace
	}

	trace.Frauds = db.FraudCount5WithWorkspace(q, ws)
	return trace
}

func (db *IVF) FraudCount5WithProbes(q *Vector, ws *SearchWorkspace, quick int) int {
	frauds, _ := db.FraudCount5Trace(q, ws, quick)
	return frauds
}

func (db *IVF) FraudCount5WithWorkspace(q *Vector, ws *SearchWorkspace) int {
	return db.FraudCount5WithProbes(q, ws, quickProbe)
}

func (db *IVF) FraudCount5(q *Vector) int {
	var ws SearchWorkspace
	return db.FraudCount5WithWorkspace(q, &ws)
}
