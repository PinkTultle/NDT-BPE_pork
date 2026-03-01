#pragma once

#include <cstddef>
#include <cstdint>

#if __has_include(<liburing.h>)
#include <liburing.h>
#elif __has_include(<liburing/io_uring.h>)
#include <liburing/io_uring.h>
#else
#error "liburing header not found; install liburing-dev"
#endif
#include <linux/io_uring.h>
#include <linux/nvme_ioctl.h>

// Fallback for older linux-libc-dev that lacks struct nvme_uring_cmd.
#ifndef NVME_URING_CMD_IO
struct nvme_uring_cmd {
    uint8_t  opcode;
    uint8_t  flags;
    uint16_t rsvd1;
    uint32_t nsid;
    uint32_t cdw2;
    uint32_t cdw3;
    uint64_t metadata;
    uint64_t addr;
    uint32_t metadata_len;
    uint32_t data_len;
    uint32_t cdw10;
    uint32_t cdw11;
    uint32_t cdw12;
    uint32_t cdw13;
    uint32_t cdw14;
    uint32_t cdw15;
    uint32_t timeout_ms;
    uint32_t rsvd2;
};
#endif

// nvme_ioctl.h 환경에 따라 NVME_URING_CMD_ADMIN/IO가 없을 수 있어 fallback
#ifndef NVME_URING_CMD_ADMIN
#define NVME_URING_CMD_ADMIN 1
#endif
#ifndef NVME_URING_CMD_IO
#define NVME_URING_CMD_IO 2
#endif

class Ring {
public:
    Ring() = default;
    Ring(const Ring&) = delete;
    Ring& operator=(const Ring&) = delete;

    Ring(Ring&& other) noexcept;
    Ring& operator=(Ring&& other) noexcept;

    ~Ring() noexcept;

    // 초기화 링:
    // queue_depth: SQ/CQ 크기
    // enable_cqe32: CQE 32바이트 사용 여부
    void init(unsigned queue_depth = 256, bool enable_cqe32 = true);

    // fixed buffer 등록(단일 버퍼)
    int register_fixed_buffer(void* buf, std::size_t len);
    int unregister_fixed_buffers();

    bool is_inited() const noexcept { return inited_; }
    bool cqe32_enabled() const noexcept { return cqe32_enabled_; }
    bool sqe128_enabled() const noexcept { return sqe128_enabled_; }
    bool fixed_buffers_registered() const noexcept { return buffers_registered_; }

    io_uring* raw() noexcept { return &ring_; }
    const io_uring* raw() const noexcept { return &ring_; }

private:
    io_uring ring_{};
    bool inited_ = false;
    bool buffers_registered_ = false;
    bool cqe32_enabled_ = false;
    bool sqe128_enabled_ = false;
};

// io-uring에서는 IORING_OP_URING_CMD passthrough만 사용한다.
struct NvmePassthruCmd {
    // 어떤 uring provider op로 보낼지: ADMIN 또는 IO
    uint32_t uring_op = NVME_URING_CMD_ADMIN;

    // NVMe command fields
    uint8_t  opcode = 0;
    uint8_t  flags  = 0;      // nvme_uring_cmd.flags (커널에서 제약 있는 경우가 있어 0 권장)
    uint16_t rsvd1  = 0;

    uint32_t nsid   = 0;

    // data buffer (optional)
    void*    data   = nullptr;
    uint32_t data_len = 0;

    // metadata (optional)
    void*    metadata = nullptr;
    uint32_t metadata_len = 0;

    // timeout (ms). 0이면 드라이버 기본
    uint32_t timeout_ms = 0;

    // cdw10~15
    uint32_t cdw10 = 0;
    uint32_t cdw11 = 0;
    uint32_t cdw12 = 0;
    uint32_t cdw13 = 0;
    uint32_t cdw14 = 0;
    uint32_t cdw15 = 0;

    // fixed buffer 사용할 때
    bool     use_fixed_buffer = false;
    uint16_t fixed_buf_index  = 0;
};


// NVMe uring_cmd 실행 결과
struct NvmeCompletion {
    // io_uring CQE
    int32_t  cqe_res  = 0;   // cqe->res
    int32_t  cqe_res2 = 0;   // cqe->res2 (드라이버가 status/errno로 사용)
    uint64_t user_data = 0;  // 요청 식별용

    // 편의: 성공 여부
    bool ok() const noexcept { return cqe_res2 == 0; }
};

// NVMe uring_cmd 제출 (fd는 상위에서 관리)
int submit_nvme_passthru(Ring& ring, int dev_fd, const NvmePassthruCmd& cmd,
                         NvmeCompletion& out, uint64_t user_data = 0);

// Backward-compatible wrapper.
int submit_nvme_cmd(Ring& ring, int dev_fd, const NvmePassthruCmd& cmd, NvmeCompletion& out,
                    uint64_t user_data = 0);
