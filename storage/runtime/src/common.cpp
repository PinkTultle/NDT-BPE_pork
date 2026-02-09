#include "common.h"
#include "bpe_tokenizer.h"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <stdexcept>
#include <sys/msg.h>
#include <sys/shm.h>
#include <utility>

ShmSlotWorker::ShmSlotWorker(std::uint32_t slot, char* read_ptr, char* write_ptr)
    : slot_(slot), read_ptr_(read_ptr), write_ptr_(write_ptr) {}

std::uint32_t ShmSlotWorker::Process(std::uint32_t input_len) const {
    if (read_ptr_ == nullptr || write_ptr_ == nullptr) {
        throw std::runtime_error("slot worker is not initialized");
    }

    const std::size_t safe_len = std::min<std::size_t>(input_len, SHM_SIZE);
    const std::string input_text(read_ptr_, read_ptr_ + safe_len);
    const std::vector<std::int32_t> token_ids =
        Runtime::BPE::BPETokenizer::Instance().Tokenize(input_text);

    auto* dest = reinterpret_cast<std::int32_t*>(write_ptr_);
    const std::size_t max_ids = SHM_SIZE / sizeof(std::int32_t);
    const std::size_t copy_count = std::min(token_ids.size(), max_ids);

    for (std::size_t i = 0; i < copy_count; ++i) {
        dest[i] = static_cast<std::int32_t>(token_ids[i]);
    }

    return static_cast<std::uint32_t>(copy_count * sizeof(std::int32_t));
}

SharedMemorySlots::SharedMemorySlots() {
    read_ids_.fill(-1);
    write_ids_.fill(-1);
    read_ptrs_.fill(nullptr);
    write_ptrs_.fill(nullptr);

    for (std::size_t s = 0; s < NUM_SLOTS; ++s) {
        read_ids_[s] = shmget(SHM_READ_KEY + static_cast<key_t>(s), SHM_SIZE, IPC_CREAT | 0660);
        write_ids_[s] = shmget(SHM_WRITE_KEY + static_cast<key_t>(s), SHM_SIZE, IPC_CREAT | 0660);
        if (read_ids_[s] < 0 || write_ids_[s] < 0) {
            throw std::runtime_error(std::string("shmget failed: ") + std::strerror(errno));
        }

        void* pr = shmat(read_ids_[s], nullptr, 0);
        void* pw = shmat(write_ids_[s], nullptr, 0);
        if (pr == reinterpret_cast<void*>(-1) || pw == reinterpret_cast<void*>(-1)) {
            throw std::runtime_error(std::string("shmat failed: ") + std::strerror(errno));
        }

        read_ptrs_[s] = static_cast<char*>(pr);
        write_ptrs_[s] = static_cast<char*>(pw);
    }
}

SharedMemorySlots::~SharedMemorySlots() {
    for (std::size_t s = 0; s < NUM_SLOTS; ++s) {
        if (read_ptrs_[s] != nullptr) {
            shmdt(read_ptrs_[s]);
            read_ptrs_[s] = nullptr;
        }
        if (write_ptrs_[s] != nullptr) {
            shmdt(write_ptrs_[s]);
            write_ptrs_[s] = nullptr;
        }
    }
}

std::vector<ShmSlotWorker> SharedMemorySlots::CreateWorkers() const {
    std::vector<ShmSlotWorker> workers;
    workers.reserve(NUM_SLOTS);
    for (std::size_t s = 0; s < NUM_SLOTS; ++s) {
        workers.emplace_back(static_cast<std::uint32_t>(s), read_ptrs_[s], write_ptrs_[s]);
    }
    return workers;
}

MessageQueueDispatcher::MessageQueueDispatcher(int msg_id, std::vector<ShmSlotWorker> workers)
    : msg_id_(msg_id), workers_(std::move(workers)) {}

int MessageQueueDispatcher::OpenQueue(key_t key, int flags) {
    return msgget(key, flags);
}

bool MessageQueueDispatcher::Receive(bpe_msg_req& req) const {
    const ssize_t n = msgrcv(msg_id_, &req, BPE_REQ_MSZ, 1, 0);
    if (n < 0) {
        std::perror("[BPE] msgrcv");
        return false;
    }
    return true;
}

void MessageQueueDispatcher::Send(const bpe_msg_resp& resp) const {
    if (msgsnd(msg_id_, &resp, BPE_RESP_MSZ, 0) < 0) {
        std::perror("[BPE] msgsnd");
    }
}

void MessageQueueDispatcher::Run() const {
    for (;;) {
        bpe_msg_req req{};
        if (!Receive(req)) {
            continue;
        }

        bpe_msg_resp resp{};
        resp.msg_type = 2;
        resp.req_id = req.req_id;
        resp.slot = req.slot;

        if (req.slot >= workers_.size()) {
            resp.byte_size = 0;
            Send(resp);
            continue;
        }

        const std::uint32_t total_len = std::min<std::uint32_t>(req.total_len, SHM_SIZE);
        resp.byte_size = workers_[req.slot].Process(total_len);
        Send(resp);
    }
}
