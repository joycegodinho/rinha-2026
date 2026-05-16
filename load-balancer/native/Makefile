CXX ?= g++

BUILD_DIR := build
SERVER := $(BUILD_DIR)/fd-lb
SRCS := fd_lb.cpp

CXXFLAGS ?= -O3 -DNDEBUG -std=c++20 -march=x86-64-v3 -mtune=native -fno-exceptions -fno-rtti
LDFLAGS ?=

.PHONY: all clean

all: $(SERVER)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(SERVER): $(SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(SRCS) -o $(SERVER) $(LDFLAGS)

clean:
	rm -rf $(BUILD_DIR)
