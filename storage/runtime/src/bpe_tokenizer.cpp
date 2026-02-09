#include "bpe_tokenizer.h"

#include "tokenizers_cpp.h"
#include <nlohmann/json.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

namespace Runtime::BPE {
namespace fs = std::filesystem;
using json = nlohmann::json;

// 내부 전역 변수
static std::atomic<long long> g_tokenize_total_us{0};
static std::string g_model_path  = "../model/byte_level_bpe_model.json";
static std::string g_merges_path = "../model/merges.txt";

// 파일 로드 헬퍼
static std::string LoadFileBinary(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) throw std::runtime_error("File open failed: " + path);
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

// Impl 정의
struct BPETokenizer::Impl {
    std::unique_ptr<tokenizers::Tokenizer> tokenizer;

    Impl(const std::string& model_path, const std::string& merges_path) {
        if (!fs::exists(model_path) || !fs::exists(merges_path)) {
            throw std::runtime_error("Model files not found: " + model_path + " or " + merges_path);
        }

        std::string merges_blob = LoadFileBinary(merges_path);
        std::string json_blob   = LoadFileBinary(model_path);

        json j = json::parse(json_blob);
        std::string vocab_blob = j.at("model").at("vocab").dump();

        std::string added_token = R"({
            "[PAD]": 0, "[UNK]": 1, "[CLS]": 2, "[SEP]": 3, "[MASK]": 4
        })";

        tokenizer = tokenizers::Tokenizer::FromBlobByteLevelBPE(vocab_blob, merges_blob, added_token);
        if (!tokenizer) throw std::runtime_error("Tokenizer init returned null");
    }
};

void BPETokenizer::Init(std::string model_path, std::string merges_path) {
    g_model_path  = std::move(model_path);
    g_merges_path = std::move(merges_path);
}

BPETokenizer& BPETokenizer::Instance() {
    static BPETokenizer instance;
    return instance;
}

BPETokenizer::BPETokenizer()
    : impl_(std::make_unique<Impl>(g_model_path, g_merges_path)) {}

BPETokenizer::~BPETokenizer() = default;

std::vector<std::int32_t> BPETokenizer::Tokenize(std::string_view text) {
    auto t0 = std::chrono::steady_clock::now();

    // Encode가 vector<int32_t>를 반환한다고 가정 (네 기존 코드와 동일)
    std::vector<std::int32_t> token_ids = impl_->tokenizer->Encode(std::string(text));

    auto t1 = std::chrono::steady_clock::now();
    long long dt_us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

    long long total_us = g_tokenize_total_us.fetch_add(dt_us, std::memory_order_relaxed) + dt_us;

    std::cout << "[LOG] Time: " << dt_us << " us, Total: " << total_us << " us\n";
    return token_ids;
}

} // namespace Runtime::BPE
