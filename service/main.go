package main

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	goruntime "runtime"
	"runtime/debug"
	"service/handler"
	appruntime "service/runtime"
	"strconv"
	"strings"

	"time"

	"github.com/valyala/fasthttp"
)

func MainHandler(rt *appruntime.RuntimeData) fasthttp.RequestHandler {
	fraudHandler := handler.Handler(rt)
	readyHandler := handler.ReadyHandler()

	return func(ctx *fasthttp.RequestCtx) {
		path := ctx.Path()
		switch {
		case pathEquals(path, "/ready"):
			readyHandler(ctx)

		case pathEquals(path, "/fraud-score"):
			if ctx.IsPost() {
				fraudHandler(ctx)
				return
			}
			ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)

		default:
			ctx.SetStatusCode(fasthttp.StatusNotFound)
		}
	}
}

func pathEquals(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(b); i++ {
		if b[i] != s[i] {
			return false
		}
	}
	return true
}

func checkFraudRate(path string) {
	data, _ := os.ReadFile(path)

	var arr []struct {
		Label string `json:"label"`
	}

	json.Unmarshal(data, &arr)

	fraud := 0
	for _, r := range arr {
		if r.Label == "fraud" {
			fraud++
		}
	}

	total := len(arr)

	log.Println("total:", total)
	log.Println("fraud:", fraud)
	log.Println("rate:", float32(fraud)/float32(total))
}

func main() {
	// checkFraudRate("service/references.json")
	goruntime.GOMAXPROCS(1)
	applyMemoryLimit()
	log.Print("Start main")
	// startPprofServer()
	rt := appruntime.Init()
	debug.FreeOSMemory()
	tuneGCForHotPath()

	socketPath := os.Getenv("SOCKET_PATH")
	if socketPath == "" {
		log.Fatal("SOCKET_PATH environment variable not set")
	}
	_ = os.Remove(socketPath)

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("Error creating UNIX listener: %v", err)
	}

	// Ensure correct permissions for other processes to access
	if err := os.Chmod(socketPath, 0666); err != nil {
		log.Fatalf("Failed to set socket permissions: %v", err)
	}

	server := &fasthttp.Server{
		Handler: MainHandler(rt),

		Name: "rinha-backend",

		ReadTimeout:  750 * time.Millisecond,
		WriteTimeout: 750 * time.Millisecond,
		IdleTimeout:  10 * time.Second,

		// memória
		ReduceMemoryUsage: false,

		// buffers pequenos
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,

		// payload controlado
		MaxRequestBodySize: 4 * 1024,

		// micro otimizações
		DisableHeaderNamesNormalizing: true,
		NoDefaultDate:                 true,
		NoDefaultServerHeader:         true,
		NoDefaultContentType:          true,
		DisablePreParseMultipartForm:  true,

		Concurrency: 4096,

		DisableKeepalive: false,
		TCPKeepalive:     true,
		LogAllErrors:     false,
	}

	log.Printf("Service running on socket %s", socketPath)
	if err := server.Serve(ln); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	log.Println("Service stopped")
	log.Println("Exiting...")
}

func applyMemoryLimit() {
	if limit, ok := parseMemoryLimitEnv(os.Getenv("GOMEMLIMIT")); ok {
		debug.SetMemoryLimit(limit)
		log.Printf("[GC] Memory limit set from GOMEMLIMIT=%s (%d bytes)", os.Getenv("GOMEMLIMIT"), limit)
		return
	}
	debug.SetMemoryLimit(120 << 20)
	log.Printf("[GC] Memory limit set to default %d bytes", 120<<20)
}

func startPprofServer() {
	addr := os.Getenv("PPROF_ADDR")
	if addr == "" {
		return
	}
	go func() {
		log.Printf("[pprof] listening on %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("[pprof] stopped: %v", err)
		}
	}()
}

func tuneGCForHotPath() {
	if raw := strings.TrimSpace(os.Getenv("GOGC")); raw != "" {
		switch strings.ToLower(raw) {
		case "off":
			debug.SetGCPercent(-1)
			log.Println("[GC] Disabled after startup via GOGC=off")
			return
		default:
			if v, err := strconv.Atoi(raw); err == nil {
				debug.SetGCPercent(v)
				log.Printf("[GC] Set GC percent to %d via GOGC", v)
				return
			}
			log.Printf("[GC] Ignoring invalid GOGC=%q", raw)
		}
	}

	switch strings.ToLower(os.Getenv("GC_MODE")) {
	case "off":
		debug.SetGCPercent(-1)
		log.Println("[GC] Disabled after startup")
	case "high":
		debug.SetGCPercent(1000)
		log.Println("[GC] High threshold mode after startup")
	}
}

func parseMemoryLimitEnv(raw string) (int64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}

	lower := strings.ToLower(raw)
	mult := int64(1)
	switch {
	case strings.HasSuffix(lower, "mib"):
		mult = 1 << 20
		lower = strings.TrimSpace(strings.TrimSuffix(lower, "mib"))
	case strings.HasSuffix(lower, "mb"):
		mult = 1000 * 1000
		lower = strings.TrimSpace(strings.TrimSuffix(lower, "mb"))
	case strings.HasSuffix(lower, "kib"):
		mult = 1 << 10
		lower = strings.TrimSpace(strings.TrimSuffix(lower, "kib"))
	case strings.HasSuffix(lower, "kb"):
		mult = 1000
		lower = strings.TrimSpace(strings.TrimSuffix(lower, "kb"))
	case strings.HasSuffix(lower, "gib"):
		mult = 1 << 30
		lower = strings.TrimSpace(strings.TrimSuffix(lower, "gib"))
	case strings.HasSuffix(lower, "gb"):
		mult = 1000 * 1000 * 1000
		lower = strings.TrimSpace(strings.TrimSuffix(lower, "gb"))
	case strings.HasSuffix(lower, "b"):
		lower = strings.TrimSpace(strings.TrimSuffix(lower, "b"))
	}

	v, err := strconv.ParseInt(lower, 10, 64)
	if err != nil || v <= 0 {
		return 0, false
	}
	return v * mult, true
}
