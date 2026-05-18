package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
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
	dim                = 14
	defaultSampleSize  = 50000
	defaultSilhouetteN = 4096
	defaultIterations  = 20
	defaultRandomSeed  = 0xdecafbadbeef1234
	maxKForUint16      = 1 << 16
	amountBins         = 16
	ratioBins          = 8
	lastBins           = 9
	kmBins             = 16
	flagBins           = 8
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

func clampBin(x int, max int) int {
	if x < 0 {
		return 0
	}
	if x > max {
		return max
	}
	return x
}

func coarseParts(v *[dim]float32) (a, r, last, km, flags int) {
	a = clampBin(int(v[0]*amountBins), amountBins-1)
	r = clampBin(int(v[2]*ratioBins), ratioBins-1)
	if v[5] < 0 {
		last = 0
	} else {
		last = 1 + clampBin(int(v[5]*8), 7)
	}
	km = clampBin(int(v[7]*kmBins), kmBins-1)
	if v[9] > 0.5 {
		flags |= 1
	}
	if v[10] > 0.5 {
		flags |= 2
	}
	if v[11] > 0.5 {
		flags |= 4
	}
	return
}

func stratumKey(v *[dim]float32, label string) int {
	a, r, last, km, flags := coarseParts(v)
	key := (((a*ratioBins+r)*lastBins+last)*kmBins+km)*flagBins + flags
	if label == "fraud" {
		key |= 1 << 20
	}
	return key
}

func countStrata(path string) (map[int]int, int) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	dec := json.NewDecoder(bufio.NewReaderSize(f, 1<<20))
	if _, err := dec.Token(); err != nil {
		log.Fatal(err)
	}

	counts := make(map[int]int, 4096)
	total := 0
	for dec.More() {
		var rec record
		if err := dec.Decode(&rec); err != nil {
			log.Fatal(err)
		}
		counts[stratumKey(&rec.Vector, rec.Label)]++
		total++
	}

	return counts, total
}

type quotaInfo struct {
	key       int
	count     int
	quota     int
	remainder float64
}

func buildQuotas(counts map[int]int, total, sampleSize int) map[int]int {
	if sampleSize > total {
		sampleSize = total
	}

	infos := make([]quotaInfo, 0, len(counts))
	used := 0
	for key, count := range counts {
		exact := float64(sampleSize) * float64(count) / float64(total)
		quota := int(math.Floor(exact))
		infos = append(infos, quotaInfo{
			key:       key,
			count:     count,
			quota:     quota,
			remainder: exact - float64(quota),
		})
		used += quota
	}

	sort.Slice(infos, func(i, j int) bool {
		if infos[i].remainder == infos[j].remainder {
			return infos[i].count > infos[j].count
		}
		return infos[i].remainder > infos[j].remainder
	})

	for i := 0; used < sampleSize && i < len(infos); i++ {
		infos[i].quota++
		used++
	}

	quotas := make(map[int]int, len(infos))
	for _, info := range infos {
		if info.quota > 0 {
			quotas[info.key] = info.quota
		}
	}
	return quotas
}

type reservoir struct {
	quota int
	seen  int
	vecs  [][dim]float32
}

func sampleDataset(path string, quotas map[int]int, seed uint64) [][dim]float32 {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	dec := json.NewDecoder(bufio.NewReaderSize(f, 1<<20))
	if _, err := dec.Token(); err != nil {
		log.Fatal(err)
	}

	reservoirs := make(map[int]*reservoir, len(quotas))
	for key, quota := range quotas {
		reservoirs[key] = &reservoir{
			quota: quota,
			vecs:  make([][dim]float32, 0, quota),
		}
	}

	rng := lcg{state: seed}
	for dec.More() {
		var rec record
		if err := dec.Decode(&rec); err != nil {
			log.Fatal(err)
		}
		key := stratumKey(&rec.Vector, rec.Label)
		r := reservoirs[key]
		if r == nil {
			continue
		}

		r.seen++
		if len(r.vecs) < r.quota {
			r.vecs = append(r.vecs, rec.Vector)
			continue
		}
		j := rng.nextInt(r.seen)
		if j < r.quota {
			r.vecs[j] = rec.Vector
		}
	}

	total := 0
	for _, r := range reservoirs {
		total += len(r.vecs)
	}

	out := make([][dim]float32, 0, total)
	for _, r := range reservoirs {
		out = append(out, r.vecs...)
	}
	return out
}

func distSq(a, b *[dim]float32) float32 {
	var d float32
	for i := 0; i < dim; i++ {
		diff := a[i] - b[i]
		d += diff * diff
	}
	return d
}

func kmeansPlusPlusInit(vectors [][dim]float32, seed uint64, k int) [][dim]float32 {
	n := len(vectors)
	rng := lcg{state: seed}

	centroids := make([][dim]float32, 0, k)
	centroids = append(centroids, vectors[rng.nextInt(n)])
	minDists := make([]float32, n)
	for i := range minDists {
		minDists[i] = float32(math.Inf(1))
	}

	for len(centroids) < k {
		last := centroids[len(centroids)-1]
		var total float64
		for i := range vectors {
			d := distSq(&vectors[i], &last)
			if d < minDists[i] {
				minDists[i] = d
			}
			total += float64(minDists[i])
		}

		target := float64(rng.nextU64()>>11) / float64(uint64(1)<<53) * total
		var acc float64
		chosen := n - 1
		for i, d := range minDists {
			acc += float64(d)
			if acc >= target {
				chosen = i
				break
			}
		}
		centroids = append(centroids, vectors[chosen])
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

func runKMeans(vectors [][dim]float32, k, nIter int, seed uint64) ([][dim]float32, []uint16) {
	centroids := kmeansPlusPlusInit(vectors, seed, k)
	assignments := make([]uint16, len(vectors))
	for iter := 0; iter < nIter; iter++ {
		changed := assignParallel(vectors, centroids, assignments)
		updateCentroids(vectors, assignments, centroids)
		if changed*1000 < len(vectors) {
			break
		}
	}
	return centroids, assignments
}

func computeWCSS(vectors [][dim]float32, centroids [][dim]float32, assignments []uint16) float64 {
	var sum float64
	for i := range vectors {
		sum += float64(distSq(&vectors[i], &centroids[assignments[i]]))
	}
	return sum
}

func chooseSubset(n, limit int, seed uint64) []int {
	if limit <= 0 || limit >= n {
		out := make([]int, n)
		for i := range out {
			out[i] = i
		}
		return out
	}

	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	rng := lcg{state: seed}
	for i := 0; i < limit; i++ {
		j := i + rng.nextInt(n-i)
		idx[i], idx[j] = idx[j], idx[i]
	}
	return idx[:limit]
}

func computeSilhouette(vectors [][dim]float32, assignments []uint16, k, subsetSize int, seed uint64) float64 {
	subset := chooseSubset(len(vectors), subsetSize, seed)
	if len(subset) < 2 {
		return 0
	}

	clusterSums := make([]float64, k)
	clusterCounts := make([]int, k)
	touched := make([]int, 0, k)

	var total float64
	for _, idx := range subset {
		ci := int(assignments[idx])
		touched = touched[:0]

		for _, other := range subset {
			if other == idx {
				continue
			}
			cj := int(assignments[other])
			if clusterCounts[cj] == 0 {
				touched = append(touched, cj)
			}
			clusterCounts[cj]++
			clusterSums[cj] += math.Sqrt(float64(distSq(&vectors[idx], &vectors[other])))
		}

		a := 0.0
		if clusterCounts[ci] > 0 {
			a = clusterSums[ci] / float64(clusterCounts[ci])
		}

		b := math.Inf(1)
		for _, cj := range touched {
			if cj == ci || clusterCounts[cj] == 0 {
				continue
			}
			mean := clusterSums[cj] / float64(clusterCounts[cj])
			if mean < b {
				b = mean
			}
		}

		s := 0.0
		if !math.IsInf(b, 1) {
			den := math.Max(a, b)
			if den > 0 {
				s = (b - a) / den
			}
		}
		total += s

		for _, cj := range touched {
			clusterCounts[cj] = 0
			clusterSums[cj] = 0
		}
	}

	return total / float64(len(subset))
}

func clusterStats(assignments []uint16, k int) (nonEmpty, minSize, maxSize int, avgSize float64) {
	counts := make([]int, k)
	for _, ci := range assignments {
		counts[int(ci)]++
	}

	minSize = math.MaxInt
	total := 0
	for _, c := range counts {
		if c == 0 {
			continue
		}
		nonEmpty++
		total += c
		if c < minSize {
			minSize = c
		}
		if c > maxSize {
			maxSize = c
		}
	}
	if nonEmpty == 0 {
		return 0, 0, 0, 0
	}
	avgSize = float64(total) / float64(nonEmpty)
	return
}

func parseKList(raw string) []int {
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		v, err := strconv.Atoi(part)
		if err != nil || v <= 1 || v > maxKForUint16 {
			log.Fatalf("invalid k %q", part)
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		log.Fatal("empty k list")
	}
	return out
}

func main() {
	input := flag.String("input", "service/references.json", "path to references json")
	kList := flag.String("k", "3072,3584,3712,3840,3968,4096", "comma-separated list of K values")
	sampleSize := flag.Int("sample", defaultSampleSize, "stratified sample size for k-means evaluation")
	silhouetteN := flag.Int("silhouette", defaultSilhouetteN, "subset size for exact silhouette")
	nIter := flag.Int("iter", defaultIterations, "max lloyd iterations per K")
	seed := flag.Uint64("seed", defaultRandomSeed, "random seed")
	flag.Parse()

	ks := parseKList(*kList)
	t0 := time.Now()
	log.Printf("counting strata from %s", *input)
	counts, total := countStrata(*input)
	quotas := buildQuotas(counts, total, *sampleSize)
	log.Printf("dataset_total=%d active_strata=%d sample_target=%d", total, len(quotas), *sampleSize)

	log.Print("sampling stratified dataset")
	sample := sampleDataset(*input, quotas, *seed)
	log.Printf("sampled=%d elapsed=%s", len(sample), time.Since(t0))

	for _, k := range ks {
		start := time.Now()
		log.Printf("evaluating k=%d", k)
		centroids, assignments := runKMeans(sample, k, *nIter, *seed+uint64(k))
		wcss := computeWCSS(sample, centroids, assignments)
		sil := computeSilhouette(sample, assignments, k, *silhouetteN, *seed+uint64(k)*17)
		nonEmpty, minSize, maxSize, avgSize := clusterStats(assignments, k)
		log.Printf(
			"k=%d sample=%d wcss=%.4f silhouette=%.6f non_empty=%d min_cluster=%d avg_cluster=%.2f max_cluster=%d elapsed=%s",
			k, len(sample), wcss, sil, nonEmpty, minSize, avgSize, maxSize, time.Since(start),
		)
	}

	fmt.Printf("\nDone in %s\n", time.Since(t0))
}
