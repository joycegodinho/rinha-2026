package main

/*
#include <stddef.h>
#include <stdint.h>
*/
import "C"

import (
	"log"
	"service/database/ivf"
	"service/handler"
	appruntime "service/runtime"
	"sync"
	"unsafe"
)

var (
	initOnce   sync.Once
	initErr    string
	classifier *handler.Classifier
	db         *ivf.IVF
	bodyState  handler.DirectState
	vectorWS   ivf.SearchWorkspace
)

func bootstrap() {
	defer func() {
		if r := recover(); r != nil {
			initErr = "panic during bootstrap"
			log.Printf("[native-bridge] bootstrap panic: %v", r)
		}
	}()
	rt := appruntime.Init()
	db = rt.DB
	classifier = handler.NewClassifier(rt)
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
	return C.int(classifier.FraudCountDirect(b, &bodyState))
}

//export fraud_classify_vector
func fraud_classify_vector(vec *C.float) C.int {
	q := (*ivf.Vector)(unsafe.Pointer(vec))
	return C.int(db.FraudCount5Bridge(q, &vectorWS))
}

func main() {}
