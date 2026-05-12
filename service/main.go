package main

import (
	"encoding/json"
	"log"
	"net"
	"os"
	goruntime "runtime"
	"runtime/debug"
	"service/handler"
	appruntime "service/runtime"

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
	debug.SetMemoryLimit(120 << 20)
	log.Print("Start main")
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

func tuneGCForHotPath() {
	switch os.Getenv("GC_MODE") {
	case "off":
		debug.SetGCPercent(-1)
		log.Println("[GC] Disabled after startup")
	case "high":
		debug.SetGCPercent(1000)
		log.Println("[GC] High threshold mode after startup")
	}
}
