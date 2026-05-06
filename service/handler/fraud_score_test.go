package handler

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"service/database/ivf"
	"service/runtime"
	"sort"
	"sync"
	"testing"
)

func chdirRepoRoot(t *testing.T) {
	t.Helper()
	for i := 0; i < 5; i++ {
		if _, err := os.Stat("service/index.bin.gz"); err == nil {
			return
		}
		if err := os.Chdir(".."); err != nil {
			t.Fatal(err)
		}
	}
	wd, _ := os.Getwd()
	t.Fatalf("could not find repo root from %s", filepath.Clean(wd))
}

func chdirRepoRootB(b *testing.B) {
	b.Helper()
	for i := 0; i < 5; i++ {
		if _, err := os.Stat("service/index.bin.gz"); err == nil {
			return
		}
		if err := os.Chdir(".."); err != nil {
			b.Fatal(err)
		}
	}
	wd, _ := os.Getwd()
	b.Fatalf("could not find repo root from %s", filepath.Clean(wd))
}

func TestFraudScoreAgainstDataset(t *testing.T) {
	path := os.Getenv("TEST_DATA_PATH")
	if path == "" {
		t.Skip("set TEST_DATA_PATH to run dataset evaluation")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	var fixture struct {
		Entries []struct {
			Request            json.RawMessage `json:"request"`
			ExpectedApproved   bool            `json:"expected_approved"`
			ExpectedFraudScore float32         `json:"expected_fraud_score"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	chdirRepoRoot(t)

	rt := runtime.Init()
	for i, entry := range fixture.Entries {
		var q ivf.Vector
		buildVectorUltra(entry.Request, rt, &q)
		frauds := rt.DB.FraudCount5(&q)
		approved := frauds < 3
		if approved != entry.ExpectedApproved {
			t.Fatalf("entry %d: approved=%v frauds=%d expected_approved=%v expected_score=%v body=%s", i, approved, frauds, entry.ExpectedApproved, entry.ExpectedFraudScore, string(entry.Request))
		}
	}
}

func TestBuildVectorUltraResetsReusedVector(t *testing.T) {
	chdirRepoRoot(t)
	rt := runtime.Init()

	first := []byte(`{
	  "amount": 100.0,
	  "installments": 2,
	  "requested_at": "2026-03-15T14:23:11Z",
	  "is_online": true,
	  "card_present": true,
	  "customer": {
	    "avg_amount": 200.0,
	    "tx_count_24h": 9,
	    "km_from_home": 11.0,
	    "known_merchants": ["m_1"]
	  },
	  "merchant": {
	    "id": "m_2",
	    "mcc": "5411",
	    "avg_amount": 80.0
	  },
	  "last_transaction": {
	    "timestamp": "2026-03-15T13:55:41Z",
	    "km_from_current": 4.0
	  }
	}`)

	second := []byte(`{
	  "amount": 100.0,
	  "installments": 2,
	  "requested_at": "2026-03-15T14:23:11Z",
	  "customer": {
	    "avg_amount": 200.0,
	    "tx_count_24h": 9,
	    "km_from_home": 11.0,
	    "known_merchants": ["m_1"]
	  },
	  "merchant": {
	    "id": "m_1",
	    "mcc": "5411",
	    "avg_amount": 80.0
	  },
	  "last_transaction": null
	}`)

	var q ivf.Vector
	buildVectorUltra(first, rt, &q)
	buildVectorUltra(second, rt, &q)

	if q[9] != 0 {
		t.Fatalf("expected is_online to reset to 0, got %v", q[9])
	}
	if q[10] != 0 {
		t.Fatalf("expected card_present to reset to 0, got %v", q[10])
	}
	if q[11] != 0 {
		t.Fatalf("expected unknown_merchant to reset to 0, got %v", q[11])
	}
	if q[5] != -1 || q[6] != -1 {
		t.Fatalf("expected last_transaction features to reset to -1, got (%v,%v)", q[5], q[6])
	}
}

func TestFraudScoreProbeStats(t *testing.T) {
	path := os.Getenv("TEST_DATA_PATH")
	if path == "" {
		t.Skip("set TEST_DATA_PATH to run probe stats")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	var fixture struct {
		Entries []struct {
			Request          json.RawMessage `json:"request"`
			ExpectedApproved bool            `json:"expected_approved"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	chdirRepoRoot(t)

	rt := runtime.Init()
	type costlyEntry struct {
		Index  int
		Path   int
		Blocks int
	}
	for _, expandedProbe := range []int{12, 16, 20, 24, 28, 32} {
		for _, quick := range []int{4, 6, 8, 10, 12, 14, 16} {
			if quick > expandedProbe {
				continue
			}
			var wrong, quickOnly, expanded, rescore int
			var quickBlocks, expandedBlocks, rescoreBlocks int
			costly := make([]costlyEntry, 0, 5)
			var ws ivf.SearchWorkspace
			for idx, entry := range fixture.Entries {
				var q ivf.Vector
				buildVectorUltra(entry.Request, rt, &q)
				trace := rt.DB.FraudCount5TraceDetailed(&q, &ws, quick, expandedProbe)
				approved := trace.Frauds < 3
				if approved != entry.ExpectedApproved {
					wrong++
				}
				quickBlocks += trace.QuickBlocks
				expandedBlocks += trace.ExpandedBlocks
				rescoreBlocks += trace.RescoreBlocks
				totalBlocks := trace.QuickBlocks + trace.ExpandedBlocks + trace.RescoreBlocks
				if len(costly) < 5 {
					costly = append(costly, costlyEntry{Index: idx, Path: trace.Path, Blocks: totalBlocks})
				} else {
					worst := 0
					for i := 1; i < len(costly); i++ {
						if costly[i].Blocks < costly[worst].Blocks {
							worst = i
						}
					}
					if totalBlocks > costly[worst].Blocks {
						costly[worst] = costlyEntry{Index: idx, Path: trace.Path, Blocks: totalBlocks}
					}
				}
				switch trace.Path {
				case 0:
					quickOnly++
				case 1:
					expanded++
				case 2:
					rescore++
				}
			}
			sort.Slice(costly, func(i, j int) bool {
				return costly[i].Blocks > costly[j].Blocks
			})
			t.Logf(
				"quick=%d expanded_probe=%d wrong=%d quick_only=%d expanded=%d rescore=%d quick_blocks=%d expanded_blocks=%d rescore_blocks=%d top_costly=%v",
				quick,
				expandedProbe,
				wrong,
				quickOnly,
				expanded,
				rescore,
				quickBlocks,
				expandedBlocks,
				rescoreBlocks,
				costly,
			)
		}
	}
}

func TestFraudScorePruneStageStats(t *testing.T) {
	path := os.Getenv("TEST_DATA_PATH")
	if path == "" {
		t.Skip("set TEST_DATA_PATH to run prune stage stats")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	var fixture struct {
		Entries []struct {
			Request          json.RawMessage `json:"request"`
			ExpectedApproved bool            `json:"expected_approved"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	chdirRepoRoot(t)

	rt := runtime.Init()
	type costlyEntry struct {
		Index        int
		Path         int
		Blocks       int
		QuickPrune   ivf.PruneCounts
		RescorePrune ivf.PruneCounts
	}

	hotSet := map[int]struct{}{
		19246: {},
		25640: {},
		29100: {},
		41981: {},
		4244:  {},
	}

	var wrong, quickOnly, rescore int
	var quickAgg, rescoreAgg ivf.PruneCounts
	hotEntries := make([]costlyEntry, 0, len(hotSet))
	var ws ivf.SearchWorkspace

	for idx, entry := range fixture.Entries {
		var q ivf.Vector
		buildVectorUltra(entry.Request, rt, &q)
		trace := rt.DB.FraudCount5TraceDetailed(&q, &ws, 8, 20)
		approved := trace.Frauds < 3
		if approved != entry.ExpectedApproved {
			wrong++
		}
		switch trace.Path {
		case 0:
			quickOnly++
		case 2:
			rescore++
		}

		quickAgg.Blocks += trace.QuickPrune.Blocks
		quickAgg.Pruned4 += trace.QuickPrune.Pruned4
		quickAgg.Pruned6 += trace.QuickPrune.Pruned6
		quickAgg.Pruned8 += trace.QuickPrune.Pruned8
		quickAgg.Pruned14 += trace.QuickPrune.Pruned14
		quickAgg.Survived += trace.QuickPrune.Survived
		for i := range quickAgg.TailPruned {
			quickAgg.TailPruned[i] += trace.QuickPrune.TailPruned[i]
		}

		rescoreAgg.Blocks += trace.RescorePrune.Blocks
		rescoreAgg.Pruned4 += trace.RescorePrune.Pruned4
		rescoreAgg.Pruned6 += trace.RescorePrune.Pruned6
		rescoreAgg.Pruned8 += trace.RescorePrune.Pruned8
		rescoreAgg.Pruned14 += trace.RescorePrune.Pruned14
		rescoreAgg.Survived += trace.RescorePrune.Survived
		for i := range rescoreAgg.TailPruned {
			rescoreAgg.TailPruned[i] += trace.RescorePrune.TailPruned[i]
		}

		if _, ok := hotSet[idx]; ok {
			hotEntries = append(hotEntries, costlyEntry{
				Index:        idx,
				Path:         trace.Path,
				Blocks:       trace.QuickPrune.Blocks + trace.RescorePrune.Blocks,
				QuickPrune:   trace.QuickPrune,
				RescorePrune: trace.RescorePrune,
			})
		}
	}

	sort.Slice(hotEntries, func(i, j int) bool {
		return hotEntries[i].Blocks > hotEntries[j].Blocks
	})

	t.Logf("current_config quick=8 expanded=20 wrong=%d quick_only=%d rescore=%d", wrong, quickOnly, rescore)
	t.Logf(
		"quick_prune blocks=%d prune4=%d prune6=%d prune8=%d prune14=%d survived=%d tail=%s",
		quickAgg.Blocks,
		quickAgg.Pruned4,
		quickAgg.Pruned6,
		quickAgg.Pruned8,
		quickAgg.Pruned14,
		quickAgg.Survived,
		formatTailPruned(quickAgg.TailPruned),
	)
	t.Logf(
		"rescore_prune blocks=%d prune4=%d prune6=%d prune8=%d prune14=%d survived=%d tail=%s",
		rescoreAgg.Blocks,
		rescoreAgg.Pruned4,
		rescoreAgg.Pruned6,
		rescoreAgg.Pruned8,
		rescoreAgg.Pruned14,
		rescoreAgg.Survived,
		formatTailPruned(rescoreAgg.TailPruned),
	)
	for _, entry := range hotEntries {
		t.Logf(
			"hot_entry index=%d path=%d blocks=%d quick_tail=%s rescore_tail=%s quick_prune={4:%d 6:%d 8:%d 14:%d surv:%d} rescore_prune={4:%d 6:%d 8:%d 14:%d surv:%d}",
			entry.Index,
			entry.Path,
			entry.Blocks,
			formatTailPruned(entry.QuickPrune.TailPruned),
			formatTailPruned(entry.RescorePrune.TailPruned),
			entry.QuickPrune.Pruned4,
			entry.QuickPrune.Pruned6,
			entry.QuickPrune.Pruned8,
			entry.QuickPrune.Pruned14,
			entry.QuickPrune.Survived,
			entry.RescorePrune.Pruned4,
			entry.RescorePrune.Pruned6,
			entry.RescorePrune.Pruned8,
			entry.RescorePrune.Pruned14,
			entry.RescorePrune.Survived,
		)
	}
}

var tailPruneNames = [6]string{
	"tx_count_24h",
	"is_online",
	"card_present",
	"unknown_merchant",
	"mcc_risk",
	"merchant_avg",
}

func formatTailPruned(counts [6]int) string {
	out := ""
	for i, name := range tailPruneNames {
		if i > 0 {
			out += " "
		}
		out += fmt.Sprintf("%s=%d", name, counts[i])
	}
	return out
}

var benchmarkRequestBody = []byte(`{
  "amount": 119.90,
  "installments": 1,
  "requested_at": "2026-03-15T14:23:11Z",
  "is_online": true,
  "card_present": false,
  "customer": {
    "avg_amount": 284.31,
    "tx_count_24h": 7,
    "km_from_home": 18.2,
    "known_merchants": ["m_101", "m_205", "m_999"]
  },
  "merchant": {
    "id": "m_404",
    "mcc": "5411",
    "avg_amount": 91.52
  },
  "last_transaction": {
    "timestamp": "2026-03-15T13:55:41Z",
    "km_from_current": 4.8
  }
}`)

func BenchmarkBuildAndClassifyLocalState(b *testing.B) {
	chdirRepoRootB(b)
	rt := runtime.Init()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var q ivf.Vector
		var ws ivf.SearchWorkspace
		buildVectorUltra(benchmarkRequestBody, rt, &q)
		_ = rt.DB.FraudCount5WithWorkspace(&q, &ws)
	}
}

func BenchmarkBuildAndClassifyPooledState(b *testing.B) {
	chdirRepoRootB(b)
	rt := runtime.Init()
	pool := sync.Pool{
		New: func() any {
			return new(requestState)
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state := pool.Get().(*requestState)
		buildVectorUltra(benchmarkRequestBody, rt, &state.q)
		_ = rt.DB.FraudCount5WithWorkspace(&state.q, &state.ws)
		pool.Put(state)
	}
}
