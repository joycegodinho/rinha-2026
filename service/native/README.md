Native service experiment

This directory is the first serious pivot away from Go for the request server.

Current design:
- C++ server (`server.cpp`) owns:
  - TCP listener
  - epoll event loop
  - HTTP parsing
  - keepalive handling
  - response writing
- Go bridge (`bridge/main.go`) owns:
  - runtime/bootstrap
  - current classifier
  - IVF search

Why this split:
- It lets us attack the network + parsing hot path immediately.
- We keep the working classifier/database logic while the native server matures.
- Once the server path proves itself, we can move more logic native.

Build targets:
- `make bridge`: builds the Go `c-archive`
- `make server`: builds the native executable
- `make all`: builds both

Runtime env:
- `SERVICE_ADDR` default `0.0.0.0:8081`
- `GC_MODE` forwarded into the Go bridge runtime
- `INDEX_PATH` supported through the existing Go runtime

Notes:
- This path is experimental and intentionally separate from the existing Go server.
- First goal is to beat the raw Go/UDS service on end-to-end p99.
