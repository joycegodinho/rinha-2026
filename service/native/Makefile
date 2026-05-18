CXX ?= g++
GO ?= go
PURE_CPP ?= 0

ROOT := ..
BUILD_DIR := build
BRIDGE_DIR := bridge

GOAMD64 ?= v3
GO_PGO ?= auto
CXXFLAGS ?= -O3 -DNDEBUG -std=c++20 -march=x86-64-v3 -mtune=native -fno-exceptions -fno-rtti
LDFLAGS ?= -pthread -ldl -lm

BRIDGE_A := $(BUILD_DIR)/libclassifier.a
SERVER := $(BUILD_DIR)/native-server
SERVER_SRCS := server.cpp native_vector.cpp native_ivf.cpp
ifeq ($(PURE_CPP),1)
SERVER_LIBS :=
SERVER_DEPS := $(BUILD_DIR) $(SERVER_SRCS) native_vector.hpp native_ivf.hpp
CXXFLAGS += -DPURE_CPP_IVF
else
SERVER_LIBS := $(BRIDGE_A)
SERVER_DEPS := $(BRIDGE_A) $(SERVER_SRCS) native_vector.hpp native_ivf.hpp
endif

.PHONY: all bridge server clean

ifeq ($(PURE_CPP),1)
all: server
else
all: bridge server
endif

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

bridge: $(BRIDGE_A)

$(BRIDGE_A): $(BUILD_DIR)
	cd $(ROOT) && CGO_ENABLED=1 GOAMD64=$(GOAMD64) $(GO) build -pgo=$(GO_PGO) -buildmode=c-archive -trimpath -buildvcs=false -o native/$(BRIDGE_A) ./native/$(BRIDGE_DIR)

server: $(SERVER)

$(SERVER): $(SERVER_DEPS)
	$(CXX) $(CXXFLAGS) -I$(BUILD_DIR) $(SERVER_SRCS) $(SERVER_LIBS) -o $(SERVER) $(LDFLAGS)

clean:
	rm -rf $(BUILD_DIR)
