package ivf

import (
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"unsafe"
)

const (
	maxCentroids    = 4096
	indexDim        = 14
	quickProbe              = 8
	adaptiveQuickProbe      = 6
	adaptiveQuickRatioLimit = 1.01
	expandedProbe           = 20
	expandedProbe3          = 16
	maxProbe                = 32
	vectorsPerBlock = 16
	blockStride     = indexDim * vectorsPerBlock
	vectorScale     = 0.0001
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

	return &IVF{
		Labels:    labels,
		Offsets:   offsets,
		Centroids: centroids,
		Blocks:    blocks,
		RawBlocks: rawBlocks,
		K:         int(k),
		IDs:       make([]uint32, 0),
		Vectors:   make([]int16, 0),
	}, nil
}

func readU32(r io.Reader) (uint32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf[:]), nil
}

func adaptiveQuickCount(ws *SearchWorkspace, quick, expanded int) int {
	if quick < 8 || expanded < 8 {
		return quick
	}
	d6 := ws.CentroidDists[ws.Probes[5]]
	d7 := ws.CentroidDists[ws.Probes[6]]
	if d6 > 1e-9 && d7/d6 >= adaptiveQuickRatioLimit {
		return adaptiveQuickProbe
	}
	return quick
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

func (db *IVF) scanProbesTrace(qv *Vector, probes *[maxProbe]int, from, to int, topDist *[5]float32, topLabel *[5]uint8, worst *int, counts *PruneCounts) {
	for i := from; i < to; i++ {
		ci := probes[i]
		start := int(db.Offsets[ci])
		end := int(db.Offsets[ci+1])
		scanBlocksTrace(qv, db.Blocks, db.Labels, start, end, topDist, topLabel, worst, counts)
	}
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

		db.scanProbes(q, &ws.Probes, 0, quick, &ws.TopDist, &ws.TopLabel, &worst)
		fast := countFrauds(&ws.TopLabel)
		if fast != 2 && fast != 3 {
			return fast, 0
		}
		rescoreProbe := expanded
		if fast == 3 && rescoreProbe > expandedProbe3 {
			rescoreProbe = expandedProbe3
		}
		return db.rescoreQuantized(q, ws, rescoreProbe), 2
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
		rescoreProbe := expanded
		if fast == 3 && rescoreProbe > expandedProbe3 {
			rescoreProbe = expandedProbe3
		}
		trace.RescoreBlocks = db.countProbeBlocks(&ws.Probes, 0, rescoreProbe)
		qv := Quantize(q)
		for i := 0; i < indexDim; i++ {
			ws.Quantized[i] = float32(qv[i]) * vectorScale
		}
		for i := 0; i < 5; i++ {
			ws.TopDist[i] = float32(math.Inf(1))
			ws.TopLabel[i] = 0
		}
		worst = 0
		db.scanProbesTrace(&ws.Quantized, &ws.Probes, 0, rescoreProbe, &ws.TopDist, &ws.TopLabel, &worst, &trace.RescorePrune)
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
	if db.K > 0 {
		topCentroids(q, db.Centroids, db.K, expandedProbe, ws.Probes[:], &ws.CentroidDists)
		quick := adaptiveQuickCount(ws, quickProbe, expandedProbe)

		for i := 0; i < 5; i++ {
			ws.TopDist[i] = float32(math.Inf(1))
			ws.TopLabel[i] = 0
		}
		worst := 0

		db.scanProbes(q, &ws.Probes, 0, quick, &ws.TopDist, &ws.TopLabel, &worst)
		fast := countFrauds(&ws.TopLabel)
		if fast != 2 && fast != 3 {
			return fast
		}

		rescoreProbe := expandedProbe
		if fast == 3 && rescoreProbe > expandedProbe3 {
			rescoreProbe = expandedProbe3
		}
		return db.rescoreQuantized(q, ws, rescoreProbe)
	}

	var w Workspace
	pairs := db.SearchK(q, &w, 5)
	frauds := 0
	for i := 0; i < len(pairs); i++ {
		if db.Labels[pairs[i].ID] == Fraud {
			frauds++
		}
	}
	return frauds
}

func (db *IVF) FraudCount5(q *Vector) int {
	var ws SearchWorkspace
	return db.FraudCount5WithWorkspace(q, &ws)
}
