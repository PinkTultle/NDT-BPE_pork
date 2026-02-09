CXX ?= g++
AR ?= ar

CXXFLAGS ?= -std=c++17 -Wall -Wextra -g
CPPFLAGS += -Iinclude

SRC_DIR := src
BUILD_DIR := build
OBJ_DIR := $(BUILD_DIR)/obj
LIB_OUT_DIR := $(BUILD_DIR)/lib

LIBURING_DIR := third_party/liburing
LIBURING_INC := $(LIBURING_DIR)/src/include
LIBURING_LIB_DIR := $(LIBURING_DIR)/src
LIBURING_LIB := $(LIBURING_LIB_DIR)/liburing.a

# tokenizer_nonbloking/Makefile 의 토크나이저 의존성 설정을 동일 스타일로 반영.
TOKENIZERS_CPP_DIR := third_party/tokenizers-cpp
TOKENIZERS_LIB_DIR := $(TOKENIZERS_CPP_DIR)/build
TOKENIZER_LIBS := -L$(TOKENIZERS_LIB_DIR) -ltokenizers_cpp -ltokenizers_c -lpthread -ldl
TOKENIZER_RPATH := -Wl,-rpath,$(abspath $(TOKENIZERS_LIB_DIR))

CPPFLAGS += -I$(LIBURING_INC)

SRCS := $(wildcard $(SRC_DIR)/*.cpp)
OBJS := $(patsubst $(SRC_DIR)/%.cpp,$(OBJ_DIR)/%.o,$(SRCS))
TARGET := $(LIB_OUT_DIR)/libndt_compute.a

.PHONY: all clean deps print-tokenizer-deps

all: deps $(TARGET)

deps:
	@if [ ! -f "$(LIBURING_LIB)" ]; then \
		echo "error: missing $(LIBURING_LIB)"; \
		echo "hint: run ./build_compute.sh first"; \
		exit 1; \
	fi

$(TARGET): $(OBJS)
	@mkdir -p $(LIB_OUT_DIR)
	$(AR) rcs $@ $^

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(OBJ_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

print-tokenizer-deps:
	@echo "TOKENIZER_LIBS=$(TOKENIZER_LIBS)"
	@echo "TOKENIZER_RPATH=$(TOKENIZER_RPATH)"

clean:
	rm -rf $(BUILD_DIR)
