package ivf

var tracePruneOrder = [indexDim]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}

func partialCanImprove(dists *[vectorsPerBlock]float32, limit float32) bool {
	for i := 0; i < vectorsPerBlock; i++ {
		if dists[i] < limit {
			return true
		}
	}
	return false
}

func scanBlocksTrace(q *Vector, blocks []int16, labels []uint8, start, end int, topDist *[5]float32, topLabel *[5]uint8, worst *int, counts *PruneCounts) {
	for block := start; block < end; block++ {
		counts.Blocks++
		base := block * blockStride
		var dists [vectorsPerBlock]float32

		for stage := 0; stage < 4; stage++ {
			dim := tracePruneOrder[stage]
			row := base + dim*vectorsPerBlock
			qv := q[dim]
			for slot := 0; slot < vectorsPerBlock; slot++ {
				diff := float32(blocks[row+slot])*vectorScale - qv
				dists[slot] += diff * diff
			}
		}
		limit := topDist[*worst]
		if !partialCanImprove(&dists, limit) {
			counts.Pruned4++
			continue
		}

		for stage := 4; stage < 6; stage++ {
			dim := tracePruneOrder[stage]
			row := base + dim*vectorsPerBlock
			qv := q[dim]
			for slot := 0; slot < vectorsPerBlock; slot++ {
				diff := float32(blocks[row+slot])*vectorScale - qv
				dists[slot] += diff * diff
			}
		}
		limit = topDist[*worst]
		if !partialCanImprove(&dists, limit) {
			counts.Pruned6++
			continue
		}

		for stage := 6; stage < 8; stage++ {
			dim := tracePruneOrder[stage]
			row := base + dim*vectorsPerBlock
			qv := q[dim]
			for slot := 0; slot < vectorsPerBlock; slot++ {
				diff := float32(blocks[row+slot])*vectorScale - qv
				dists[slot] += diff * diff
			}
		}
		limit = topDist[*worst]
		if !partialCanImprove(&dists, limit) {
			counts.Pruned8++
			continue
		}

		prunedTail := false
		for stage := 8; stage < indexDim; stage++ {
			dim := tracePruneOrder[stage]
			row := base + dim*vectorsPerBlock
			qv := q[dim]
			for slot := 0; slot < vectorsPerBlock; slot++ {
				diff := float32(blocks[row+slot])*vectorScale - qv
				dists[slot] += diff * diff
			}
			limit = topDist[*worst]
			if !partialCanImprove(&dists, limit) {
				counts.Pruned14++
				counts.TailPruned[stage-8]++
				prunedTail = true
				break
			}
		}
		if prunedTail {
			continue
		}

		counts.Survived++
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
