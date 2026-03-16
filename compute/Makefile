CXX ?= g++
AR ?= ar

CXXFLAGS ?= -std=c++17 -Wall -Wextra -g
CPPFLAGS += -Iinclude

PYARROW_ROOT ?= $(abspath ../scripts/.venv/lib/python3.12/site-packages/pyarrow)
ARROW_INC := $(PYARROW_ROOT)/include
ARROW_LIB := $(PYARROW_ROOT)/libarrow.so.2300
ARROW_CPPFLAGS := -I$(ARROW_INC)
ARROW_LDFLAGS := -Wl,-rpath,$(PYARROW_ROOT)

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

ARROW_SRC := $(SRC_DIR)/arrow_text_dump.cpp
ARROW_LIB_SRC := $(SRC_DIR)/arrow_text_dump_lib.cpp
ARROW_OBJ := $(OBJ_DIR)/arrow_text_dump.o
ARROW_LIB_OBJ := $(OBJ_DIR)/arrow_text_dump_lib.o
ARROW_BIN = $(BIN_DIR)/arrow_text_dump

EXTENT_INDEX_SRC := $(SRC_DIR)/extent_index_main.cpp
EXTENT_INDEX_OBJ := $(OBJ_DIR)/extent_index_main.o
EXTENT_INDEX_LIB_SRC := $(SRC_DIR)/extent-index.cpp
EXTENT_INDEX_LIB_OBJ := $(OBJ_DIR)/extent-index.o
EXTENT_INDEX_BIN := $(BIN_DIR)/extent_index

MAIN_SRC := $(SRC_DIR)/main.cpp
SRCS := $(filter-out $(MAIN_SRC) $(ARROW_SRC) $(ARROW_LIB_SRC) $(EXTENT_INDEX_SRC) $(EXTENT_INDEX_LIB_SRC),$(wildcard $(SRC_DIR)/*.cpp))
OBJS := $(patsubst $(SRC_DIR)/%.cpp,$(OBJ_DIR)/%.o,$(SRCS))
MAIN_OBJ := $(OBJ_DIR)/main.o
TARGET := $(LIB_OUT_DIR)/libndt_compute.a
BIN_DIR := $(BUILD_DIR)/bin
BIN_TARGET := $(BIN_DIR)/ndt_compute_main

.PHONY: all clean deps print-tokenizer-deps arrow_text_dump extent_index

all: deps $(TARGET) $(BIN_TARGET)

arrow_text_dump: $(ARROW_BIN)

extent_index: $(EXTENT_INDEX_BIN)

deps:
	@if [ ! -f "$(LIBURING_LIB)" ]; then \
		echo "error: missing $(LIBURING_LIB)"; \
		echo "hint: run ./build_compute.sh first"; \
		exit 1; \
	fi

$(TARGET): $(OBJS)
	@mkdir -p $(LIB_OUT_DIR)
	$(AR) rcs $@ $^

$(BIN_TARGET): $(TARGET) $(MAIN_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $(MAIN_OBJ) $(TARGET) $(LIBURING_LIB) -lpthread -ldl -o $@

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(OBJ_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

$(ARROW_OBJ): $(ARROW_SRC)
	@mkdir -p $(OBJ_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(ARROW_CPPFLAGS) -c $< -o $@

$(ARROW_LIB_OBJ): $(ARROW_LIB_SRC)
	@mkdir -p $(OBJ_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(ARROW_CPPFLAGS) -c $< -o $@

$(ARROW_BIN): $(ARROW_OBJ) $(ARROW_LIB_OBJ)
	@mkdir -p $(BIN_DIR)
	@if [ ! -f "$(ARROW_LIB)" ]; then \
		echo "error: missing $(ARROW_LIB)"; \
		echo "hint: ensure pyarrow is installed under scripts/.venv"; \
		exit 1; \
	fi
	$(CXX) $(CXXFLAGS) $(ARROW_OBJ) $(ARROW_LIB_OBJ) $(ARROW_LIB) -lpthread -ldl $(ARROW_LDFLAGS) -o $@
	@echo "built: $(ARROW_BIN)"

$(EXTENT_INDEX_OBJ): $(EXTENT_INDEX_SRC)
	@mkdir -p $(OBJ_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(ARROW_CPPFLAGS) -c $< -o $@

$(EXTENT_INDEX_LIB_OBJ): $(EXTENT_INDEX_LIB_SRC)
	@mkdir -p $(OBJ_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(ARROW_CPPFLAGS) -c $< -o $@

$(EXTENT_INDEX_BIN): $(EXTENT_INDEX_OBJ) $(EXTENT_INDEX_LIB_OBJ)
	@mkdir -p $(BIN_DIR)
	@if [ ! -f "$(ARROW_LIB)" ]; then \
		echo "error: missing $(ARROW_LIB)"; \
		echo "hint: ensure pyarrow is installed under scripts/.venv"; \
		exit 1; \
	fi
	$(CXX) $(CXXFLAGS) $(EXTENT_INDEX_OBJ) $(EXTENT_INDEX_LIB_OBJ) $(ARROW_LIB) -lpthread -ldl $(ARROW_LDFLAGS) -o $@
	@echo "built: $(EXTENT_INDEX_BIN)"

print-tokenizer-deps:
	@echo "TOKENIZER_LIBS=$(TOKENIZER_LIBS)"
	@echo "TOKENIZER_RPATH=$(TOKENIZER_RPATH)"

clean:
	rm -rf $(BUILD_DIR)
CXXFLAGS += $(shell python3-config --includes)
