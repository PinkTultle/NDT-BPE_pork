#include "io-uring.h"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <system_error>
#include <sys/uio.h>

namespace {

#ifndef IORING_SETUP_SQE128
#define IORING_SETUP_SQE128 (1U << 10)
#endif

#ifndef IORING_SETUP_CQE32
#define IORING_SETUP_CQE32 (1U << 11)
#endif

inline bool is_valid_uring_cmd_op(std::uint32_t op) {
    return op == NVME_URING_CMD_ADMIN || op == NVME_URING_CMD_IO;
}

inline void fill_nvme_cmd(const NvmePassthruCmd& cmd, nvme_uring_cmd& uc) {
    uc = {};
    uc.opcode = cmd.opcode;
    uc.flags = cmd.flags;
    uc.rsvd1 = cmd.rsvd1;
    uc.nsid = cmd.nsid;
    uc.metadata = reinterpret_cast<std::uint64_t>(cmd.metadata);
    uc.addr = reinterpret_cast<std::uint64_t>(cmd.data);
    uc.metadata_len = cmd.metadata_len;
    uc.data_len = cmd.data_len;
    uc.cdw10 = cmd.cdw10;
    uc.cdw11 = cmd.cdw11;
    uc.cdw12 = cmd.cdw12;
    uc.cdw13 = cmd.cdw13;
    uc.cdw14 = cmd.cdw14;
    uc.cdw15 = cmd.cdw15;
    uc.timeout_ms = cmd.timeout_ms;
}

} // namespace

Ring::Ring(Ring&& other) noexcept
    : ring_(other.ring_),
      inited_(other.inited_),
      buffers_registered_(other.buffers_registered_),
      cqe32_enabled_(other.cqe32_enabled_),
      sqe128_enabled_(other.sqe128_enabled_) {
    other.ring_ = {};
    other.inited_ = false;
    other.buffers_registered_ = false;
    other.cqe32_enabled_ = false;
    other.sqe128_enabled_ = false;
}

Ring& Ring::operator=(Ring&& other) noexcept {
    if (this == &other) {
        return *this;
    }

    if (inited_) {
        if (buffers_registered_) {
            io_uring_unregister_buffers(&ring_);
        }
        io_uring_queue_exit(&ring_);
    }

    ring_ = other.ring_;
    inited_ = other.inited_;
    buffers_registered_ = other.buffers_registered_;
    cqe32_enabled_ = other.cqe32_enabled_;
    sqe128_enabled_ = other.sqe128_enabled_;

    other.ring_ = {};
    other.inited_ = false;
    other.buffers_registered_ = false;
    other.cqe32_enabled_ = false;
    other.sqe128_enabled_ = false;
    return *this;
}

Ring::~Ring() noexcept {
    if (!inited_) {
        return;
    }
    if (buffers_registered_) {
        (void)io_uring_unregister_buffers(&ring_);
    }
    io_uring_queue_exit(&ring_);
}

void Ring::init(unsigned queue_depth, bool enable_cqe32) {
    if (inited_) {
        throw std::runtime_error("Ring already initialized");
    }

    io_uring_params p{};
    p.flags |= IORING_SETUP_SQE128; // NVMe uring_cmd payload requires SQE128.
    if (enable_cqe32) {
        p.flags |= IORING_SETUP_CQE32;
    }

    const int rc = io_uring_queue_init_params(queue_depth, &ring_, &p);
    if (rc < 0) {
        throw std::system_error(-rc, std::generic_category(),
                                "io_uring_queue_init_params failed");
    }

    inited_ = true;
    buffers_registered_ = false;
    sqe128_enabled_ = (p.flags & IORING_SETUP_SQE128) != 0;
    cqe32_enabled_ = (p.flags & IORING_SETUP_CQE32) != 0;
}

int Ring::register_fixed_buffer(void* buf, std::size_t len) {
    if (!inited_) {
        return -EINVAL;
    }
    if (buf == nullptr || len == 0) {
        return -EINVAL;
    }

    if (buffers_registered_) {
        const int urc = io_uring_unregister_buffers(&ring_);
        if (urc < 0) {
            return urc;
        }
        buffers_registered_ = false;
    }

    iovec iov{};
    iov.iov_base = buf;
    iov.iov_len = len;

    const int rc = io_uring_register_buffers(&ring_, &iov, 1);
    if (rc < 0) {
        return rc;
    }
    buffers_registered_ = true;
    return 0;
}

int Ring::unregister_fixed_buffers() {
    if (!inited_) {
        return -EINVAL;
    }
    if (!buffers_registered_) {
        return 0;
    }
    const int rc = io_uring_unregister_buffers(&ring_);
    if (rc < 0) {
        return rc;
    }
    buffers_registered_ = false;
    return 0;
}

int submit_nvme_passthru(Ring& ring, int dev_fd, const NvmePassthruCmd& cmd,
                         NvmeCompletion& out, uint64_t user_data) {
    if (!ring.is_inited()) {
        return -EINVAL;
    }
    if (dev_fd < 0) {
        return -EBADF;
    }
    if (!ring.sqe128_enabled()) {
        return -EINVAL;
    }
    if (!is_valid_uring_cmd_op(cmd.uring_op)) {
        return -EINVAL;
    }
    if (cmd.use_fixed_buffer && !ring.fixed_buffers_registered()) {
        return -EINVAL;
    }

    nvme_uring_cmd uc{};
    fill_nvme_cmd(cmd, uc);

    io_uring_sqe* sqe = io_uring_get_sqe(ring.raw());
    if (sqe == nullptr) {
        return -EAGAIN;
    }

    std::memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = IORING_OP_URING_CMD;
    sqe->fd = dev_fd;
    sqe->cmd_op = cmd.uring_op;
    sqe->user_data = user_data;

    if (cmd.use_fixed_buffer) {
        sqe->uring_cmd_flags |= IORING_URING_CMD_FIXED;
        sqe->buf_index = cmd.fixed_buf_index;
    }

    std::memcpy(sqe->cmd, &uc, sizeof(uc));

    int rc = io_uring_submit(ring.raw());
    if (rc < 0) {
        return rc;
    }
    if (rc == 0) {
        return -EIO;
    }

    io_uring_cqe* cqe = nullptr;
    rc = io_uring_wait_cqe(ring.raw(), &cqe);
    if (rc < 0) {
        return rc;
    }

    out.user_data = cqe->user_data;
    out.cqe_res = cqe->res;
    out.cqe_res2 = 0;
    if (ring.cqe32_enabled()) {
        const auto* ext = reinterpret_cast<const std::uint32_t*>(cqe->big_cqe);
        out.cqe_res2 = static_cast<std::int32_t>(ext[0]);
    }

    io_uring_cqe_seen(ring.raw(), cqe);
    return 0;
}

int submit_nvme_cmd(Ring& ring, int dev_fd, const NvmePassthruCmd& cmd, NvmeCompletion& out,
                    uint64_t user_data) {
    return submit_nvme_passthru(ring, dev_fd, cmd, out, user_data);
}
