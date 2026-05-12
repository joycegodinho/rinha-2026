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

func TestFraudScoreAdaptiveProbeCandidates(t *testing.T) {
	path := os.Getenv("TEST_DATA_PATH")
	if path == "" {
		t.Skip("set TEST_DATA_PATH to run adaptive probe stats")
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
	var ws16, ws20 ivf.SearchWorkspace
	for idx, entry := range fixture.Entries {
		var q ivf.Vector
		buildVectorUltra(entry.Request, rt, &q)
		trace16 := rt.DB.FraudCount5TraceDetailed(&q, &ws16, 8, 16)
		trace20 := rt.DB.FraudCount5TraceDetailed(&q, &ws20, 8, 20)
		ok16 := trace16.Frauds < 3
		ok20 := trace20.Frauds < 3
		if ok16 == entry.ExpectedApproved {
			continue
		}
		t.Logf(
			"wrong16 index=%d expected=%v got16=%v got20=%v quickFrauds16=%d rescoreFrauds16=%d quickBlocks=%d rescoreBlocks16=%d rescoreBlocks20=%d body=%s",
			idx,
			entry.ExpectedApproved,
			ok16,
			ok20,
			trace16.QuickFrauds,
			trace16.RescoreFrauds,
			trace16.QuickBlocks,
			trace16.RescoreBlocks,
			trace20.RescoreBlocks,
			string(entry.Request),
		)
	}
}

func TestFraudScoreAdaptiveRescoreDistribution(t *testing.T) {
	path := os.Getenv("TEST_DATA_PATH")
	if path == "" {
		t.Skip("set TEST_DATA_PATH to run adaptive rescore distribution")
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
	var wsQuick, ws16, ws20 ivf.SearchWorkspace
	var bins [6]int
	var quickBins [6]int
	var rescored, rerunNeeded, adaptiveWrong int
	var quick2AdaptiveWrong, quick3AdaptiveWrong int
	var directQuick2Wrong, directQuick3Wrong int
	var rescoreBlocks16, extraBlocks20 int
	var quick2AdaptiveBlocks, quick3AdaptiveBlocks int
	var directQuick2Blocks, directQuick3Blocks int
	for _, entry := range fixture.Entries {
		var q ivf.Vector
		buildVectorUltra(entry.Request, rt, &q)

		quickFrauds, pathQuick := rt.DB.FraudCount5TraceProbes(&q, &wsQuick, 8, 8)
		if pathQuick != 2 {
			continue
		}
		rescored++
		if quickFrauds >= 0 && quickFrauds <= 5 {
			quickBins[quickFrauds]++
		}

		f16, _ := rt.DB.FraudCount5TraceProbes(&q, &ws16, 8, 16)
		if f16 >= 0 && f16 <= 5 {
			bins[f16]++
		}
		trace16 := rt.DB.FraudCount5TraceDetailed(&q, &ws16, 8, 16)
		trace20 := rt.DB.FraudCount5TraceDetailed(&q, &ws20, 8, 20)
		rescoreBlocks16 += trace16.RescoreBlocks
		finalFrauds := f16
		if f16 == 2 || f16 == 3 {
			rerunNeeded++
			extraBlocks20 += trace20.RescoreBlocks
			finalFrauds = trace20.Frauds
		}
		if (finalFrauds < 3) != entry.ExpectedApproved {
			adaptiveWrong++
		}

		fQuick2 := f16
		quick2AdaptiveBlocks += trace16.RescoreBlocks
		if quickFrauds == 2 {
			fQuick2 = trace20.Frauds
			quick2AdaptiveBlocks += trace20.RescoreBlocks
		}
		if (fQuick2 < 3) != entry.ExpectedApproved {
			quick2AdaptiveWrong++
		}

		fQuick3 := f16
		quick3AdaptiveBlocks += trace16.RescoreBlocks
		if quickFrauds == 3 {
			fQuick3 = trace20.Frauds
			quick3AdaptiveBlocks += trace20.RescoreBlocks
		}
		if (fQuick3 < 3) != entry.ExpectedApproved {
			quick3AdaptiveWrong++
		}

		fDirectQuick2 := trace16.Frauds
		directQuick2Blocks += trace16.RescoreBlocks
		if quickFrauds == 2 {
			fDirectQuick2 = trace20.Frauds
			directQuick2Blocks += trace20.RescoreBlocks - trace16.RescoreBlocks
		}
		if (fDirectQuick2 < 3) != entry.ExpectedApproved {
			directQuick2Wrong++
		}

		fDirectQuick3 := trace16.Frauds
		directQuick3Blocks += trace16.RescoreBlocks
		if quickFrauds == 3 {
			fDirectQuick3 = trace20.Frauds
			directQuick3Blocks += trace20.RescoreBlocks - trace16.RescoreBlocks
		}
		if (fDirectQuick3 < 3) != entry.ExpectedApproved {
			directQuick3Wrong++
		}
	}

	t.Logf(
		"rescored=%d quickBins=%v bins16=%v rerunNeeded=%d adaptiveWrong=%d rescoreBlocks16=%d extraBlocks20=%d quick2AdaptiveWrong=%d quick2AdaptiveBlocks=%d quick3AdaptiveWrong=%d quick3AdaptiveBlocks=%d directQuick2Wrong=%d directQuick2Blocks=%d directQuick3Wrong=%d directQuick3Blocks=%d",
		rescored,
		quickBins,
		bins,
		rerunNeeded,
		adaptiveWrong,
		rescoreBlocks16,
		extraBlocks20,
		quick2AdaptiveWrong,
		quick2AdaptiveBlocks,
		quick3AdaptiveWrong,
		quick3AdaptiveBlocks,
		directQuick2Wrong,
		directQuick2Blocks,
		directQuick3Wrong,
		directQuick3Blocks,
	)
}

var tailPruneNames = [6]string{
	"tx_count_24h",
	"is_online",
	"card_present",
	"unknown_merchant",
	"mcc_risk",
	"merchant_avg",
}

func TestFraudScoreAdaptiveQuickByCentroidGap(t *testing.T) {
	path := os.Getenv("TEST_DATA_PATH")
	if path == "" {
		t.Skip("set TEST_DATA_PATH to run adaptive quick gap stats")
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
	var ws6, ws8 ivf.SearchWorkspace

	type sample struct {
		gap   float32
		ratio float32
		base  ivf.SearchTrace
		alt   ivf.SearchTrace
		want  bool
	}

	samples := make([]sample, 0, len(fixture.Entries))
	var baseWrong, baseBlocks, altWrong, altBlocks int
	for _, entry := range fixture.Entries {
		var q ivf.Vector
		buildVectorUltra(entry.Request, rt, &q)

		trace8 := rt.DB.FraudCount5TraceDetailed(&q, &ws8, 8, 20)
		trace6 := rt.DB.FraudCount5TraceDetailed(&q, &ws6, 6, 20)
		baseApproved := trace8.Frauds < 3
		altApproved := trace6.Frauds < 3
		if baseApproved != entry.ExpectedApproved {
			baseWrong++
		}
		if altApproved != entry.ExpectedApproved {
			altWrong++
		}
		baseBlocks += trace8.QuickBlocks + trace8.RescoreBlocks
		altBlocks += trace6.QuickBlocks + trace6.RescoreBlocks

		d6 := ws8.CentroidDists[ws8.Probes[5]]
		d7 := ws8.CentroidDists[ws8.Probes[6]]
		gap := d7 - d6
		ratio := float32(9999)
		if d6 > 1e-9 {
			ratio = d7 / d6
		}
		samples = append(samples, sample{gap: gap, ratio: ratio, base: trace8, alt: trace6, want: entry.ExpectedApproved})
	}

	t.Logf("baseline quick8 wrong=%d blocks=%d | quick6 wrong=%d blocks=%d", baseWrong, baseBlocks, altWrong, altBlocks)

	for _, th := range []float32{0.0005, 0.0010, 0.0020, 0.0030, 0.0050, 0.0075, 0.0100, 0.0150, 0.0200} {
		var wrong, use6, totalBlocks int
		for _, s := range samples {
			chosen := s.base
			if s.gap >= th {
				chosen = s.alt
				use6++
			}
			if (chosen.Frauds < 3) != s.want {
				wrong++
			}
			totalBlocks += chosen.QuickBlocks + chosen.RescoreBlocks
		}
		t.Logf("gap threshold=%.4f wrong=%d use6=%d blocks=%d saved=%d", th, wrong, use6, totalBlocks, baseBlocks-totalBlocks)
	}

	for _, th := range []float32{1.005, 1.010, 1.020, 1.030, 1.050, 1.080, 1.100, 1.150, 1.200} {
		var wrong, use6, totalBlocks int
		for _, s := range samples {
			chosen := s.base
			if s.ratio >= th {
				chosen = s.alt
				use6++
			}
			if (chosen.Frauds < 3) != s.want {
				wrong++
			}
			totalBlocks += chosen.QuickBlocks + chosen.RescoreBlocks
		}
		t.Logf("ratio threshold=%.3f wrong=%d use6=%d blocks=%d saved=%d", th, wrong, use6, totalBlocks, baseBlocks-totalBlocks)
	}
}

func TestFraudScoreAdaptiveQuickMultiLevel(t *testing.T) {
	path := os.Getenv("TEST_DATA_PATH")
	if path == "" {
		t.Skip("set TEST_DATA_PATH to run adaptive quick multi-level stats")
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
	var ws4, ws6, ws8 ivf.SearchWorkspace

	type sample struct {
		r45  float32
		r67  float32
		t4   ivf.SearchTrace
		t6   ivf.SearchTrace
		t8   ivf.SearchTrace
		want bool
	}

	samples := make([]sample, 0, len(fixture.Entries))
	var baseBlocks, wrong4, wrong6, wrong8 int
	for _, entry := range fixture.Entries {
		var q ivf.Vector
		buildVectorUltra(entry.Request, rt, &q)

		trace8 := rt.DB.FraudCount5TraceDetailed(&q, &ws8, 8, 20)
		trace6 := rt.DB.FraudCount5TraceDetailed(&q, &ws6, 6, 20)
		trace4 := rt.DB.FraudCount5TraceDetailed(&q, &ws4, 4, 20)
		if (trace8.Frauds < 3) != entry.ExpectedApproved {
			wrong8++
		}
		if (trace6.Frauds < 3) != entry.ExpectedApproved {
			wrong6++
		}
		if (trace4.Frauds < 3) != entry.ExpectedApproved {
			wrong4++
		}
		baseBlocks += trace8.QuickBlocks + trace8.RescoreBlocks

		d4 := ws8.CentroidDists[ws8.Probes[3]]
		d5 := ws8.CentroidDists[ws8.Probes[4]]
		d6 := ws8.CentroidDists[ws8.Probes[5]]
		d7 := ws8.CentroidDists[ws8.Probes[6]]
		r45 := float32(9999)
		r67 := float32(9999)
		if d4 > 1e-9 {
			r45 = d5 / d4
		}
		if d6 > 1e-9 {
			r67 = d7 / d6
		}
		samples = append(samples, sample{r45: r45, r67: r67, t4: trace4, t6: trace6, t8: trace8, want: entry.ExpectedApproved})
	}

	t.Logf("baseline wrong: quick4=%d quick6=%d quick8=%d blocks8=%d", wrong4, wrong6, wrong8, baseBlocks)

	for _, th4 := range []float32{1.01, 1.02, 1.03, 1.05, 1.08, 1.10, 1.15, 1.20, 1.30} {
		var wrong, use4, use6, totalBlocks int
		for _, s := range samples {
			chosen := s.t8
			switch {
			case s.r45 >= th4:
				chosen = s.t4
				use4++
			case s.r67 >= 1.01:
				chosen = s.t6
				use6++
			}
			if (chosen.Frauds < 3) != s.want {
				wrong++
			}
			totalBlocks += chosen.QuickBlocks + chosen.RescoreBlocks
		}
		t.Logf("multi threshold4=%.3f wrong=%d use4=%d use6=%d blocks=%d saved=%d", th4, wrong, use4, use6, totalBlocks, baseBlocks-totalBlocks)
	}
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
