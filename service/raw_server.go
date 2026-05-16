package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"service/handler"
	appruntime "service/runtime"
	"time"
)

var (
	rawReadTimeout  = 750 * time.Millisecond
	rawWriteTimeout = 750 * time.Millisecond
	rawIdleTimeout  = 10 * time.Second

	http200ReadyKeepAlive = []byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
	http200ReadyClose     = []byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
	http404KeepAlive      = []byte("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
	http404Close          = []byte("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
	http405KeepAlive      = []byte("HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
	http405Close          = []byte("HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
	http400KeepAlive      = []byte("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
	http400Close          = []byte("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
)

func responsePrefix(status string, keepAlive bool) []byte {
	switch status {
	case "200-ready":
		if keepAlive {
			return http200ReadyKeepAlive
		}
		return http200ReadyClose
	case "404":
		if keepAlive {
			return http404KeepAlive
		}
		return http404Close
	case "405":
		if keepAlive {
			return http405KeepAlive
		}
		return http405Close
	default:
		if keepAlive {
			return http400KeepAlive
		}
		return http400Close
	}
}

func writeStaticHTTP(w *bufio.Writer, status string, keepAlive bool) error {
	if _, err := w.Write(responsePrefix(status, keepAlive)); err != nil {
		return err
	}
	return w.Flush()
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + (n % 10))
		n /= 10
	}
	return string(buf[i:])
}

func headerValue(line []byte) []byte {
	idx := bytes.IndexByte(line, ':')
	if idx < 0 {
		return nil
	}
	v := bytes.TrimSpace(line[idx+1:])
	return bytes.TrimSuffix(v, []byte{'\r', '\n'})
}

func hasPrefix(line []byte, prefix string) bool {
	if len(line) < len(prefix) {
		return false
	}
	for i := 0; i < len(prefix); i++ {
		if line[i] != prefix[i] {
			return false
		}
	}
	return true
}

func parseIntBytes(b []byte) int {
	v := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			break
		}
		v = v*10 + int(c-'0')
	}
	return v
}

func clampFraudCountRaw(fraudCount int) int {
	if fraudCount < 0 {
		return 0
	}
	if fraudCount > 5 {
		return 5
	}
	return fraudCount
}

func writeFraudHTTP(w *bufio.Writer, fraudCount int, keepAlive bool) error {
	body := handler.FraudResponse(clampFraudCountRaw(fraudCount))
	connHeader := "keep-alive"
	if !keepAlive {
		connHeader = "close"
	}
	if _, err := w.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "); err != nil {
		return err
	}
	if _, err := w.WriteString(itoa(len(body))); err != nil {
		return err
	}
	if _, err := w.WriteString("\r\nConnection: "); err != nil {
		return err
	}
	if _, err := w.WriteString(connHeader); err != nil {
		return err
	}
	if _, err := w.WriteString("\r\n\r\n"); err != nil {
		return err
	}
	if _, err := w.Write(body); err != nil {
		return err
	}
	return w.Flush()
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	ne, ok := err.(net.Error)
	return ok && ne.Timeout()
}

func isCloseValue(v []byte) bool {
	return len(v) == 5 &&
		(v[0] == 'c' || v[0] == 'C') &&
		(v[1] == 'l' || v[1] == 'L') &&
		(v[2] == 'o' || v[2] == 'O') &&
		(v[3] == 's' || v[3] == 'S') &&
		(v[4] == 'e' || v[4] == 'E')
}

func serveRawConn(conn net.Conn, classifier *handler.Classifier) {
	defer conn.Close()

	r := bufio.NewReaderSize(conn, 4096)
	w := bufio.NewWriterSize(conn, 4096)
	bodyBuf := make([]byte, 4096)

	for {
		_ = conn.SetReadDeadline(time.Now().Add(rawIdleTimeout))

		reqLine, err := r.ReadSlice('\n')
		if err != nil {
			if err != io.EOF && !isTimeout(err) {
				_ = writeStaticHTTP(w, "400", false)
			}
			return
		}

		_ = conn.SetReadDeadline(time.Now().Add(rawReadTimeout))
		keepAlive := true
		contentLength := 0

		for {
			line, err := r.ReadSlice('\n')
			if err != nil {
				if isTimeout(err) || err == io.EOF {
					return
				}
				_ = writeStaticHTTP(w, "400", false)
				return
			}
			if len(line) == 2 && line[0] == '\r' && line[1] == '\n' {
				break
			}
			switch {
			case hasPrefix(line, "Content-Length:"):
				contentLength = parseIntBytes(headerValue(line))
			case hasPrefix(line, "Connection:"):
				v := headerValue(line)
				if isCloseValue(v) {
					keepAlive = false
				}
			}
		}

		switch {
		case hasPrefix(reqLine, "GET /ready "):
			_ = conn.SetWriteDeadline(time.Now().Add(rawWriteTimeout))
			if err := writeStaticHTTP(w, "200-ready", keepAlive); err != nil {
				return
			}
		case hasPrefix(reqLine, "POST /fraud-score "):
			if contentLength <= 0 || contentLength > len(bodyBuf) {
				_ = writeStaticHTTP(w, "400", false)
				return
			}
			if _, err := io.ReadFull(r, bodyBuf[:contentLength]); err != nil {
				if isTimeout(err) || err == io.EOF {
					return
				}
				_ = writeStaticHTTP(w, "400", false)
				return
			}
			fraudCount := classifier.FraudCount(bodyBuf[:contentLength])
			_ = conn.SetWriteDeadline(time.Now().Add(rawWriteTimeout))
			if err := writeFraudHTTP(w, fraudCount, keepAlive); err != nil {
				return
			}
		case hasPrefix(reqLine, "GET "):
			_ = conn.SetWriteDeadline(time.Now().Add(rawWriteTimeout))
			if err := writeStaticHTTP(w, "404", keepAlive); err != nil {
				return
			}
		default:
			_ = conn.SetWriteDeadline(time.Now().Add(rawWriteTimeout))
			if err := writeStaticHTTP(w, "405", keepAlive); err != nil {
				return
			}
		}

		if !keepAlive {
			return
		}
	}
}

func serveRawListener(ln net.Listener, classifier *handler.Classifier, logLabel string) {
	log.Printf("%s running on %s", logLabel, ln.Addr())
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("%s accept error: %v", logLabel, err)
			continue
		}
		go serveRawConn(conn, classifier)
	}
}

func serveRawUnix(rt *appruntime.RuntimeData, socketPath string) {
	_ = os.Remove(socketPath)

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("Error creating raw UNIX listener: %v", err)
	}
	if err := os.Chmod(socketPath, 0666); err != nil {
		log.Fatalf("Failed to set raw socket permissions: %v", err)
	}

	classifier := handler.NewClassifier(rt)
	serveRawListener(ln, classifier, "Raw service")
}

func serveRawTCP(rt *appruntime.RuntimeData, addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Error creating raw TCP listener: %v", err)
	}

	classifier := handler.NewClassifier(rt)
	serveRawListener(ln, classifier, "Raw TCP service")
}
