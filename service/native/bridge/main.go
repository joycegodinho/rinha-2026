package main

/*
#include <stddef.h>
#include <stdint.h>
*/
import "C"

import (
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"service/database/ivf"
	"service/handler"
	appruntime "service/runtime"
	"strconv"
	"sync"
	"time"
	"unsafe"
)

var (
	initOnce   sync.Once
	initErr    string
	classifier *handler.Classifier
	db         *ivf.IVF
	bodyState  handler.DirectState
	vectorWS   ivf.SearchWorkspace
	ivfProfile ivf.BridgeProfile

	profileEnabled     bool
	profileVectorCalls uint64
	profileVectorNS    uint64
	profileBodyCalls   uint64
	profileBodyNS      uint64
)

func tuneGCForNativeBridge() {
	switch os.Getenv("GC_MODE") {
	case "off":
		debug.SetGCPercent(-1)
		log.Printf("[native-bridge] GC disabled after startup")
	case "high":
		debug.SetGCPercent(1000)
		log.Printf("[native-bridge] GC high threshold after startup")
	}
}

func bootstrap() {
	defer func() {
		if r := recover(); r != nil {
			initErr = "panic during bootstrap"
			log.Printf("[native-bridge] bootstrap panic: %v", r)
		}
	}()
	if value := os.Getenv("NATIVE_GOMAXPROCS"); value != "" {
		if n, err := strconv.Atoi(value); err == nil && n > 0 {
			runtime.GOMAXPROCS(n)
		}
	}
	rt := appruntime.Init()
	db = rt.DB
	classifier = handler.NewClassifier(rt)
	profileEnabled = os.Getenv("NATIVE_PROFILE") == "1"
	tuneGCForNativeBridge()
	log.Printf("[native-bridge] classifier initialized")
}

//export fraud_init
func fraud_init() C.int {
	initOnce.Do(bootstrap)
	if initErr != "" || classifier == nil {
		return 1
	}
	return 0
}

//export fraud_classify
func fraud_classify(body *C.uint8_t, n C.size_t) C.int {
	if body == nil || n == 0 {
		return -1
	}
	if initErr != "" || classifier == nil {
		return -1
	}
	b := unsafe.Slice((*byte)(unsafe.Pointer(body)), int(n))
	if profileEnabled {
		start := time.Now()
		out := classifier.FraudCountDirect(b, &bodyState)
		profileBodyCalls++
		profileBodyNS += uint64(time.Since(start).Nanoseconds())
		return C.int(out)
	}
	return C.int(classifier.FraudCountDirect(b, &bodyState))
}

//export fraud_classify_vector
func fraud_classify_vector(vec *C.float) C.int {
	q := (*ivf.Vector)(unsafe.Pointer(vec))
	if profileEnabled {
		start := time.Now()
		out := db.FraudCount5BridgeProfile(q, &vectorWS, &ivfProfile)
		profileVectorCalls++
		profileVectorNS += uint64(time.Since(start).Nanoseconds())
		return C.int(out)
	}
	return C.int(db.FraudCount5Bridge(q, &vectorWS))
}

//export fraud_profile_report
func fraud_profile_report() {
	if !profileEnabled {
		return
	}
	if profileVectorCalls > 0 {
		log.Printf("[native-bridge-profile] vector_calls=%d avg_ivf_ns=%d total_ivf_ms=%.3f",
			profileVectorCalls,
			profileVectorNS/profileVectorCalls,
			float64(profileVectorNS)/1_000_000.0)
	}
	if ivfProfile.Calls > 0 {
		calls := ivfProfile.Calls
		rescore := ivfProfile.Rescore
		if rescore == 0 {
			rescore = 1
		}
		log.Printf("[native-ivf-profile] calls=%d quick_only=%d rescore=%d fast_bins=%v rescore_bins=%v",
			ivfProfile.Calls,
			ivfProfile.QuickOnly,
			ivfProfile.Rescore,
			ivfProfile.FastBins,
			ivfProfile.RescoreBins)
		log.Printf("[native-ivf-profile] avg_centroid_ns=%d avg_select8_ns=%d avg_quick_scan_ns=%d avg_quick_blocks=%d",
			ivfProfile.CentroidNS/calls,
			ivfProfile.Select8NS/calls,
			ivfProfile.QuickScanNS/calls,
			ivfProfile.QuickBlocks/calls)
		log.Printf("[native-ivf-profile] avg_select20_ns=%d avg_vectorize_ns=%d avg_rescore_scan_ns=%d avg_rescore_total_ns=%d avg_rescore_blocks=%d",
			ivfProfile.Select20NS/rescore,
			ivfProfile.VectorizeNS/rescore,
			ivfProfile.RescoreScanNS/rescore,
			ivfProfile.RescoreNS/rescore,
			ivfProfile.RescoreBlocks/rescore)
	}
	if profileBodyCalls > 0 {
		log.Printf("[native-bridge-profile] body_calls=%d avg_body_go_ns=%d total_body_go_ms=%.3f",
			profileBodyCalls,
			profileBodyNS/profileBodyCalls,
			float64(profileBodyNS)/1_000_000.0)
	}
}

func main() {}
