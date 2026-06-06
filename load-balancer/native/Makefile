CXX ?= g++

BUILD_DIR := build
SERVER := $(BUILD_DIR)/fd-lb
SRCS := fd_lb.cpp

CXX_OPT ?= -Ofast
CXX_MARCH ?= haswell
CXX_TUNE ?= haswell
CXX_EXTRA ?= -fomit-frame-pointer -flto
LDFLAGS_EXTRA ?= -flto
CXXFLAGS ?= $(CXX_OPT) -DNDEBUG -std=c++20 -march=$(CXX_MARCH) -mtune=$(CXX_TUNE) -fno-exceptions -fno-rtti -pthread $(CXX_EXTRA)
LDFLAGS ?= -pthread $(LDFLAGS_EXTRA)

.PHONY: all clean

all: $(SERVER)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(SERVER): $(SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(SRCS) -o $(SERVER) $(LDFLAGS)

clean:
	rm -rf $(BUILD_DIR)
