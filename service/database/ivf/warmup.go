package ivf

const warmupPageBytes = 4096

var warmupSink uint64

func touchBytes(buf []byte) uint64 {
	if len(buf) == 0 {
		return 0
	}
	var acc uint64
	for i := 0; i < len(buf); i += warmupPageBytes {
		acc += uint64(buf[i])
	}
	acc += uint64(buf[len(buf)-1])
	return acc
}

func touchUint32(buf []uint32) uint64 {
	if len(buf) == 0 {
		return 0
	}
	step := warmupPageBytes / 4
	if step < 1 {
		step = 1
	}
	var acc uint64
	for i := 0; i < len(buf); i += step {
		acc += uint64(buf[i])
	}
	acc += uint64(buf[len(buf)-1])
	return acc
}

func touchFloat32(buf []float32) uint64 {
	if len(buf) == 0 {
		return 0
	}
	step := warmupPageBytes / 4
	if step < 1 {
		step = 1
	}
	var acc uint64
	for i := 0; i < len(buf); i += step {
		acc += uint64(int32(buf[i] * 1000))
	}
	acc += uint64(int32(buf[len(buf)-1] * 1000))
	return acc
}

func touchInt16(buf []int16) uint64 {
	if len(buf) == 0 {
		return 0
	}
	step := warmupPageBytes / 2
	if step < 1 {
		step = 1
	}
	var acc uint64
	for i := 0; i < len(buf); i += step {
		acc += uint64(uint16(buf[i]))
	}
	acc += uint64(uint16(buf[len(buf)-1]))
	return acc
}

func (db *IVF) Warmup() {
	if db == nil {
		return
	}

	var acc uint64
	acc += touchBytes(db.RawBlocks)
	acc += touchBytes(db.Labels)
	acc += touchUint32(db.Offsets)
	acc += touchUint32(db.IDs)
	acc += touchFloat32(db.Centroids)
	acc += touchInt16(db.Vectors)

	// Run a few representative searches so centroid scoring, block scans and
	// handler-sized workspaces are all in the hot path before the benchmark hits.
	if db.K > 0 && len(db.Centroids) >= indexDim {
		var ws SearchWorkspace
		samples := db.K
		if samples > 8 {
			samples = 8
		}
		step := 1
		if samples > 0 && db.K > samples {
			step = db.K / samples
			if step < 1 {
				step = 1
			}
		}
		warmed := 0
		for ci := 0; ci < db.K && warmed < samples; ci += step {
			base := ci * indexDim
			var q Vector
			for i := 0; i < indexDim; i++ {
				q[i] = db.Centroids[base+i]
			}
			acc += uint64(db.FraudCount5WithWorkspace(&q, &ws))
			warmed++
		}
	}

	warmupSink = acc
}
