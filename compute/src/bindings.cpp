#include <pybind11/pybind11.h>
#include <pybind11/stl.h>  // std::vector, std::string 자동 변환
#include <pybind11/functional.h>

#include "io-uring.h"       // Ring, submit_nvme_passthru
#include "fiemap_schedule.h" // convert_fiemap_to_nvme_segs

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

namespace py = pybind11;

namespace {

constexpr std::uint8_t kDefaultOpcode = 0xD4;
constexpr std::uint32_t kDefaultNsid = 1;
constexpr unsigned kDefaultQueueDepth = 256;

double now_us() {
    struct timespec ts {};
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0.0;
    }
    return static_cast<double>(ts.tv_sec) * 1e6 + static_cast<double>(ts.tv_nsec) / 1e3;
}

struct IoJob {
    std::uint64_t in_slba;
    std::uint64_t out_slba;
    std::uint32_t nblocks;
};

struct PendingInfo {
    std::uint64_t slba;
    std::uint64_t out_slba;
    std::uint32_t nblocks;
    void* buf;
    std::size_t buf_len;
};

void* alloc_io_buf(std::size_t len) {
    if (len == 0) {
        return nullptr;
    }
    void* ptr = nullptr;
    const std::size_t align = 4096;
    if (posix_memalign(&ptr, align, len) != 0) {
        return nullptr;
    }
    return ptr;
}

void free_io_buf(void* ptr) {
    free(ptr);
}

void fill_nvme_cmd(std::uint8_t opcode,
                   std::uint32_t nsid,
                   std::uint64_t slba,
                   std::uint32_t nblocks,
                   std::uint32_t slot,
                   std::uint64_t out_slba,
                   void* data,
                   std::uint32_t data_len,
                   nvme_uring_cmd& out) {
    out = {};
    out.opcode = opcode;
    out.nsid = nsid;
    out.addr = reinterpret_cast<std::uint64_t>(data);
    out.data_len = data_len;
    out.cdw10 = static_cast<std::uint32_t>(slba & 0xFFFFFFFFULL);
    out.cdw11 = static_cast<std::uint32_t>((slba >> 32) & 0xFFFFFFFFULL);
    out.cdw12 = (nblocks == 0) ? 0 : (nblocks - 1); // NLB-1
    out.cdw13 = slot;
    out.cdw14 = static_cast<std::uint32_t>(out_slba & 0xFFFFFFFFULL);
    out.cdw15 = (nblocks == 0) ? 0 : (nblocks - 1);
}

bool ensure_output_file(const std::string& path, std::size_t size_bytes, std::string& err) {
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) {
        err = std::string("open failed: ") + std::strerror(errno);
        return false;
    }
    if (size_bytes > 0) {
        const int rc = ::posix_fallocate(fd, 0, static_cast<off_t>(size_bytes));
        if (rc != 0) {
            err = std::string("posix_fallocate failed: ") + std::strerror(rc);
            ::close(fd);
            return false;
        }
    }
    ::close(fd);
    return true;
}

bool build_jobs(const std::vector<NvmeSeg>& in_segs,
                const std::vector<NvmeSeg>& out_segs,
                std::vector<IoJob>& jobs) {
    jobs.clear();
    if (in_segs.empty() || out_segs.empty()) {
        return false;
    }

    std::size_t in_idx = 0;
    std::size_t out_idx = 0;
    std::uint64_t in_lba = in_segs[0].slba;
    std::uint64_t out_lba = out_segs[0].slba;
    std::uint64_t in_rem = in_segs[0].nblocks;
    std::uint64_t out_rem = out_segs[0].nblocks;

    while (in_idx < in_segs.size()) {
        if (out_idx >= out_segs.size()) {
            return false;
        }
        if (in_rem == 0) {
            ++in_idx;
            if (in_idx >= in_segs.size()) break;
            in_lba = in_segs[in_idx].slba;
            in_rem = in_segs[in_idx].nblocks;
            continue;
        }
        if (out_rem == 0) {
            ++out_idx;
            if (out_idx >= out_segs.size()) break;
            out_lba = out_segs[out_idx].slba;
            out_rem = out_segs[out_idx].nblocks;
            continue;
        }

        const std::uint64_t chunk = std::min(in_rem, out_rem);
        jobs.push_back(IoJob{in_lba, out_lba, static_cast<std::uint32_t>(chunk)});
        in_lba += chunk;
        out_lba += chunk;
        in_rem -= chunk;
        out_rem -= chunk;
    }

    return (in_idx >= in_segs.size() && in_rem == 0);
}

} // namespace

py::dict tokenize_to_nvme(const std::string& dev_path,
                          const std::string& input_path,
                          const std::string& output_path,
                          std::uint8_t opcode,
                          std::uint32_t nsid,
                          unsigned queue_depth,
                          std::size_t max_inflight,
                          std::size_t slots,
                          std::size_t max_blocks_per_seg,
                          std::size_t max_extents,
                          bool admin,
                          bool verbose) {
    py::gil_scoped_release release;

    const std::string out_path = output_path.empty() ? (input_path + ".bin") : output_path;
    const std::size_t inflight = (max_inflight == 0) ? slots : max_inflight;
    if (inflight == 0) {
        throw std::runtime_error("max_inflight must be > 0");
    }

    std::vector<NvmeSeg> in_segs;
    std::size_t total_bytes = 0;
    fiemap_schedule::convert_fiemap_to_nvme_segs(input_path,
                                                 in_segs,
                                                 total_bytes,
                                                 max_blocks_per_seg,
                                                 max_extents);
    if (in_segs.empty()) {
        throw std::runtime_error("no input segments found");
    }

    std::string err;
    if (!ensure_output_file(out_path, total_bytes, err)) {
        throw std::runtime_error(err);
    }

    std::vector<NvmeSeg> out_segs;
    std::size_t out_bytes = 0;
    fiemap_schedule::convert_fiemap_to_nvme_segs(out_path,
                                                 out_segs,
                                                 out_bytes,
                                                 max_blocks_per_seg,
                                                 max_extents);
    if (out_bytes < total_bytes) {
        throw std::runtime_error("output file too small for input");
    }

    std::vector<IoJob> jobs;
    if (!build_jobs(in_segs, out_segs, jobs)) {
        throw std::runtime_error("failed to map input to output segments");
    }

    const int dev_fd = ::open(dev_path.c_str(), O_RDWR | O_CLOEXEC);
    if (dev_fd < 0) {
        throw std::runtime_error(std::string("open dev failed: ") + std::strerror(errno));
    }

    Ring ring;
    ring.init(queue_depth, true);
    const std::uint32_t cmd_op = admin ? NVME_URING_CMD_ADMIN : NVME_URING_CMD_IO;
    const std::size_t submit_batch = std::max<std::size_t>(1, inflight / 2);

    std::unordered_map<std::uint64_t, PendingInfo> pending;
    pending.reserve(inflight);

    std::size_t inflight_cnt = 0;
    std::size_t queued = 0;
    std::uint64_t next_id = 1;
    std::uint32_t slot_rr = 0;
    std::size_t errors = 0;

    const double t0 = now_us();

    auto free_all_pending = [&]() {
        for (auto& kv : pending) {
            free_io_buf(kv.second.buf);
        }
        pending.clear();
    };

    auto submit_one = [&](const IoJob& job) -> int {
        io_uring_sqe* sqe = io_uring_get_sqe(ring.raw());
        if (!sqe) {
            return -EAGAIN;
        }

        const std::size_t buf_len = static_cast<std::size_t>(job.nblocks) * FIEMAP_LBA_BYTES;
        void* buf = alloc_io_buf(buf_len);
        if (buf == nullptr) {
            return -ENOMEM;
        }

        const std::uint32_t slot = static_cast<std::uint32_t>(
            slots == 0 ? 0 : (slot_rr % slots));
        ++slot_rr;

        nvme_uring_cmd uc{};
        fill_nvme_cmd(opcode, nsid, job.in_slba, job.nblocks, slot, job.out_slba,
                      buf, static_cast<std::uint32_t>(buf_len), uc);

        std::memset(sqe, 0, sizeof(*sqe));
        sqe->opcode = IORING_OP_URING_CMD;
        sqe->fd = dev_fd;
        sqe->cmd_op = cmd_op;
        sqe->user_data = next_id;
        std::memcpy(sqe->cmd, &uc, sizeof(uc));

        pending.emplace(next_id, PendingInfo{job.in_slba, job.out_slba, job.nblocks, buf, buf_len});
        ++next_id;
        ++queued;
        ++inflight_cnt;
        return 0;
    };

    auto submit_queued = [&]() -> bool {
        if (queued == 0) {
            return true;
        }
        const int rc = io_uring_submit(ring.raw());
        if (rc < 0) {
            if (verbose) {
                std::fprintf(stderr, "io_uring_submit failed: %s\n", std::strerror(-rc));
            }
            return false;
        }
        queued = 0;
        return true;
    };

    auto reap_one = [&]() -> bool {
        io_uring_cqe* cqe = nullptr;
        const int rc = io_uring_wait_cqe(ring.raw(), &cqe);
        if (rc < 0) {
            if (verbose) {
                std::fprintf(stderr, "io_uring_wait_cqe failed: %s\n", std::strerror(-rc));
            }
            return false;
        }

        bool nvme_status_err = false;
        if (ring.cqe32_enabled()) {
            const auto* ext = reinterpret_cast<const std::uint32_t*>(cqe->big_cqe);
            const std::uint32_t dw3 = ext[3];
            const std::uint16_t status_field = static_cast<std::uint16_t>((dw3 >> 17) & 0xFFFF);
            nvme_status_err = (status_field != 0);
        }

        if (cqe->res < 0 || nvme_status_err) {
            ++errors;
        }

        const auto it = pending.find(cqe->user_data);
        if (it != pending.end()) {
            free_io_buf(it->second.buf);
            pending.erase(it);
        }
        io_uring_cqe_seen(ring.raw(), cqe);
        if (inflight_cnt > 0) {
            --inflight_cnt;
        }
        return true;
    };

    bool ok = true;
    for (const auto& job : jobs) {
        while (inflight_cnt >= inflight) {
            if (!submit_queued()) { ok = false; break; }
            if (!reap_one()) { ok = false; break; }
        }
        if (!ok) break;

        int rc = submit_one(job);
        if (rc == -EAGAIN) {
            if (!submit_queued()) { ok = false; break; }
            if (!reap_one()) { ok = false; break; }
            rc = submit_one(job);
        }
        if (rc < 0) { ok = false; break; }

        if (queued >= submit_batch) {
            if (!submit_queued()) { ok = false; break; }
        }
    }

    if (ok && !submit_queued()) ok = false;
    while (ok && inflight_cnt > 0) {
        if (!reap_one()) { ok = false; break; }
    }

    free_all_pending();
    ::close(dev_fd);

    if (!ok) {
        throw std::runtime_error("tokenize_to_nvme failed");
    }

    const double t1 = now_us();
    py::dict result;
    result["segments"] = jobs.size();
    result["total_bytes"] = total_bytes;
    result["errors"] = errors;
    result["elapsed_us"] = t1 - t0;
    result["out_path"] = out_path;
    return result;
}

PYBIND11_MODULE(ndt_compute, m) {
    m.doc() = "NDT-BPE compute bindings (FIEMAP + NVMe io_uring).";

    m.def(
        "tokenize_to_nvme",
        &tokenize_to_nvme,
        py::arg("dev_path"),
        py::arg("input_path"),
        py::arg("output_path") = "",
        py::arg("opcode") = kDefaultOpcode,
        py::arg("nsid") = kDefaultNsid,
        py::arg("queue_depth") = kDefaultQueueDepth,
        py::arg("max_inflight") = 0,
        py::arg("slots") = 4,
        py::arg("max_blocks_per_seg") = FIEMAP_MDTS_BLOCKS,
        py::arg("max_extents") = FIEMAP_MAX_EXTENTS,
        py::arg("admin") = false,
        py::arg("verbose") = false,
        "Run FIEMAP -> NVMe io_uring submit pipeline. Returns stats dict."
    );
}
