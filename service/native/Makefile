CXX ?= g++
GO ?= go
PURE_CPP ?= 0

ROOT := ..
BUILD_DIR := build
BRIDGE_DIR := bridge

GOAMD64 ?= v3
GO_PGO ?= auto
CXX_OPT ?= -Ofast
CXX_MARCH ?= haswell
CXX_TUNE ?= haswell
CXX_EXTRA ?= -fomit-frame-pointer -flto
LDFLAGS_EXTRA ?= -flto
CXXFLAGS ?= $(CXX_OPT) -DNDEBUG -std=c++20 -march=$(CXX_MARCH) -mtune=$(CXX_TUNE) -fno-exceptions -fno-rtti $(CXX_EXTRA)
LDFLAGS ?= -pthread -ldl -lm $(LDFLAGS_EXTRA)

BRIDGE_A := $(BUILD_DIR)/libclassifier.a
SERVER := $(BUILD_DIR)/native-server
ORACLE_POLICY := $(BUILD_DIR)/ivf-oracle-policy
FINAL_ORACLE := $(BUILD_DIR)/final-oracle-tsv
SERVER_SRCS := server.cpp native_vector.cpp native_ivf.cpp decision_tree.cpp decision_rescore_tree.cpp
ifeq ($(PURE_CPP),1)
SERVER_LIBS :=
SERVER_DEPS := $(BUILD_DIR) $(SERVER_SRCS) native_vector.hpp native_ivf.hpp decision_tree.hpp
CXXFLAGS += -DPURE_CPP_IVF
else
SERVER_LIBS := $(BRIDGE_A)
SERVER_DEPS := $(BRIDGE_A) $(SERVER_SRCS) native_vector.hpp native_ivf.hpp decision_tree.hpp
endif

.PHONY: all bridge server oracle-policy final-oracle-tsv clean

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

oracle-policy: $(ORACLE_POLICY)

$(ORACLE_POLICY): $(BUILD_DIR) ivf_oracle_policy.cpp native_ivf.cpp native_ivf.hpp decision_tree.hpp decision_rescore_tree.cpp
	$(CXX) $(CXXFLAGS) -I$(BUILD_DIR) ivf_oracle_policy.cpp native_ivf.cpp decision_rescore_tree.cpp -o $(ORACLE_POLICY) $(LDFLAGS)

final-oracle-tsv: $(FINAL_ORACLE)

$(FINAL_ORACLE): $(BUILD_DIR) final_oracle_tsv.cpp native_vector.cpp native_vector.hpp decision_tree.cpp
	$(CXX) $(CXXFLAGS) -I$(BUILD_DIR) final_oracle_tsv.cpp native_vector.cpp decision_tree.cpp -o $(FINAL_ORACLE) $(LDFLAGS)

clean:
	rm -rf $(BUILD_DIR)
