CXX ?= g++
GO ?= go

ROOT := ..
BUILD_DIR := build
BRIDGE_DIR := bridge

GOAMD64 ?= v3
CXXFLAGS ?= -O3 -DNDEBUG -std=c++20 -march=x86-64-v3 -mtune=native -fno-exceptions -fno-rtti
LDFLAGS ?= -pthread -ldl -lm

BRIDGE_A := $(BUILD_DIR)/libclassifier.a
SERVER := $(BUILD_DIR)/native-server
SERVER_SRCS := server.cpp native_vector.cpp

.PHONY: all bridge server clean

all: bridge server

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

bridge: $(BRIDGE_A)

$(BRIDGE_A): $(BUILD_DIR)
	cd $(ROOT) && CGO_ENABLED=1 GOAMD64=$(GOAMD64) $(GO) build -buildmode=c-archive -trimpath -buildvcs=false -o native/$(BRIDGE_A) ./native/$(BRIDGE_DIR)

server: $(SERVER)

$(SERVER): $(BRIDGE_A) $(SERVER_SRCS) native_vector.hpp
	$(CXX) $(CXXFLAGS) -I$(BUILD_DIR) $(SERVER_SRCS) $(BRIDGE_A) -o $(SERVER) $(LDFLAGS)

clean:
	rm -rf $(BUILD_DIR)
