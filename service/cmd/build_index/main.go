package main

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"log"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	dim               = 14
	defaultK          = 4096
	defaultNIter      = 25
	defaultSampleSize = 50000
	scale             = 10000
)

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

func writeIndex(path string, vectors [][dim]float32, labels []byte, assignments []uint16, centroids [][dim]float32) {
	k := len(centroids)
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

	offsets := make([]uint32, k+1)
	for ci := 0; ci < k; ci++ {
		sz := uint32(len(clusters[ci]))
		offsets[ci+1] = offsets[ci] + (sz+7)/8
	}

	totalBlocks := int(offsets[k])
	paddedN := totalBlocks * 8
	outLabels := make([]byte, paddedN)
	outBlocks := make([]int16, totalBlocks*dim*8)

	for ci := 0; ci < k; ci++ {
		blockStart := int(offsets[ci])
		vecs := clusters[ci]
		nBlocks := int(offsets[ci+1] - offsets[ci])
		for bk := 0; bk < nBlocks; bk++ {
			blockBase := (blockStart + bk) * dim * 8
			labelBase := (blockStart + bk) * 8
			for slot := 0; slot < 8; slot++ {
				pos := bk*8 + slot
				if pos < len(vecs) {
					vi := int(vecs[pos])
					for d := 0; d < dim; d++ {
						outBlocks[blockBase+d*8+slot] = quantize(vectors[vi][d])
					}
					outLabels[labelBase+slot] = labels[vi]
				} else {
					for d := 0; d < dim; d++ {
						outBlocks[blockBase+d*8+slot] = math.MaxInt16
					}
				}
			}
		}
	}

	centroidsT := make([]float32, dim*k)
	for ci := 0; ci < k; ci++ {
		for d := 0; d < dim; d++ {
			centroidsT[d*k+ci] = centroids[ci][d]
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

	if _, err := gz.Write([]byte("IVF1")); err != nil {
		log.Fatal(err)
	}
	writeU32(gz, uint32(len(vectors)))
	writeU32(gz, uint32(k))
	writeU32(gz, dim)
	must(binary.Write(gz, binary.LittleEndian, centroidsT))
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
	if k > 1<<16 {
		log.Fatalf("k=%d exceeds uint16 assignment capacity", k)
	}

	t0 := time.Now()
	log.Print("loading dataset")
	vectors, labels := loadDataset(input)
	log.Printf("loaded %d vectors in %s", len(vectors), time.Since(t0))

	log.Printf("kmeans++ init k=%d nIter=%d sampleSize=%d", k, nIter, sampleSize)
	centroids := kmeansPlusPlusInit(vectors, 0xdeadbeef_cafebabe, k, sampleSize)

	assignments := make([]uint16, len(vectors))
	for iter := 0; iter < nIter; iter++ {
		t := time.Now()
		changed := assignParallel(vectors, centroids, assignments)
		updateCentroids(vectors, assignments, centroids)
		log.Printf("iter %02d changed=%.2f%% elapsed=%s", iter+1, float64(changed)*100/float64(len(vectors)), time.Since(t))
		if changed*1000 < len(vectors) {
			break
		}
	}

	log.Print("writing index")
	writeIndex(output, vectors, labels, assignments, centroids)
	log.Printf("done in %s", time.Since(t0))
}

func parsePositiveInt(s, name string) int {
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		log.Fatalf("invalid %s %q", name, s)
	}
	return v
}
