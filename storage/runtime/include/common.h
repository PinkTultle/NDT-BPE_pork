#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include <sys/ipc.h>

constexpr key_t SHM_READ_KEY = 0x1000;
constexpr key_t SHM_WRITE_KEY = 0x1200;
constexpr key_t MSG_KEY = 1002;
constexpr std::size_t SHM_SIZE = 131072; // 128 KiB
constexpr std::size_t NUM_SLOTS = 64;
constexpr key_t STATS_SHM_KEY = 0x2000;
constexpr std::size_t STATS_SHM_SIZE = 4096;

struct BpeRuntimeStats {
    std::uint64_t magic = 0;
    std::uint32_t version = 1;
    std::uint32_t reserved = 0;

    std::uint64_t start_ts_us = 0;
    std::uint64_t last_ts_us = 0;
    std::uint64_t last_latency_us = 0;
    std::uint64_t last_req_id = 0;
    std::uint32_t last_slot = 0;
    std::uint32_t last_resp_bytes = 0;

    std::uint64_t req_total = 0;
    std::uint64_t resp_total = 0;
    std::uint64_t bytes_in = 0;
    std::uint64_t bytes_out = 0;

    std::array<std::uint64_t, NUM_SLOTS> per_slot_req{};
    std::array<std::uint64_t, NUM_SLOTS> per_slot_resp{};
    std::array<std::uint64_t, NUM_SLOTS> per_slot_bytes_in{};
    std::array<std::uint64_t, NUM_SLOTS> per_slot_bytes_out{};
};

struct __attribute__((packed)) bpe_msg_req {
    long msg_type;      // == 1
    std::uint32_t total_len;
    std::uint64_t req_id;
    std::uint32_t slot; // cdw13
};

struct __attribute__((packed)) bpe_msg_resp {
    long msg_type; // == 2
    std::uint32_t byte_size;
    std::uint64_t req_id;
    std::uint32_t slot;
};

enum {
    BPE_REQ_MSZ = static_cast<int>(sizeof(bpe_msg_req) - sizeof(long)),
    BPE_RESP_MSZ = static_cast<int>(sizeof(bpe_msg_resp) - sizeof(long)),
};

class ShmSlotWorker {
public:
    ShmSlotWorker() = default;
    ShmSlotWorker(std::uint32_t slot, char* read_ptr, char* write_ptr);

    std::uint32_t Process(std::uint32_t input_len) const;
    std::uint32_t slot() const { return slot_; }

private:
    std::uint32_t slot_ = 0;
    char* read_ptr_ = nullptr;
    char* write_ptr_ = nullptr;
};

class SharedMemorySlots {
public:
    SharedMemorySlots();
    ~SharedMemorySlots();

    SharedMemorySlots(const SharedMemorySlots&) = delete;
    SharedMemorySlots& operator=(const SharedMemorySlots&) = delete;

    std::vector<ShmSlotWorker> CreateWorkers() const;

private:
    std::array<int, NUM_SLOTS> read_ids_{};
    std::array<int, NUM_SLOTS> write_ids_{};
    std::array<char*, NUM_SLOTS> read_ptrs_{};
    std::array<char*, NUM_SLOTS> write_ptrs_{};
};

class MessageQueueDispatcher {
public:
    MessageQueueDispatcher(int msg_id, std::vector<ShmSlotWorker> workers);

    static int OpenQueue(key_t key = MSG_KEY, int flags = IPC_CREAT | 0660);
    void Run() const;

private:
    bool Receive(bpe_msg_req& req) const;
    void Send(const bpe_msg_resp& resp) const;

    int msg_id_ = -1;
    std::vector<ShmSlotWorker> workers_;
};
