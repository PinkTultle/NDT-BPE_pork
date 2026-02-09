#include "../include/fiemap_schedule.h"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <system_error>
#include <vector>

#include <fcntl.h>
#include <linux/fiemap.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <unistd.h>

namespace {

class ScopedFd {
public:
    explicit ScopedFd(int fd) : fd_(fd) {}
    ~ScopedFd() {
        if (fd_ >= 0) {
            (void)::close(fd_);
        }
    }
    int get() const noexcept { return fd_; }

private:
    int fd_ = -1;
};

std::uint64_t div_round_up(std::uint64_t n, std::uint64_t d) {
    return (n + d - 1) / d;
}

} // namespace

void fiemap_schedule::convert_fiemap_to_nvme_segs(const std::string& filepath,
                                                  std::vector<NvmeSeg>& out_segs,
                                                  std::size_t& out_length_bytes) {
    convert_fiemap_to_nvme_segs(filepath,
                                out_segs,
                                out_length_bytes,
                                FIEMAP_MDTS_BLOCKS,
                                FIEMAP_MAX_EXTENTS);
}

void fiemap_schedule::convert_fiemap_to_nvme_segs(const std::string& filepath,
                                                  std::vector<NvmeSeg>& out_segs,
                                                  std::size_t& out_length_bytes,
                                                  std::size_t max_blocks_per_seg,
                                                  std::size_t max_extents) {
    if (max_blocks_per_seg == 0) {
        throw std::invalid_argument("max_blocks_per_seg must be > 0");
    }
    if (max_extents == 0) {
        throw std::invalid_argument("max_extents must be > 0");
    }

    const int fd = ::open(filepath.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        throw std::system_error(errno, std::generic_category(), "open failed: " + filepath);
    }
    ScopedFd scoped_fd(fd);

    const std::size_t fm_bytes =
        sizeof(struct fiemap) + max_extents * sizeof(struct fiemap_extent);
    std::vector<std::uint8_t> fm_storage(fm_bytes, 0);
    auto* fm = reinterpret_cast<struct fiemap*>(fm_storage.data());

    fm->fm_start = 0;
    fm->fm_length = ~0ULL;
    fm->fm_flags = 0;
    fm->fm_extent_count = static_cast<std::uint32_t>(max_extents);

    if (::ioctl(scoped_fd.get(), FS_IOC_FIEMAP, fm) == -1) {
        throw std::system_error(errno, std::generic_category(), "FS_IOC_FIEMAP failed");
    }

    out_segs.clear();
    out_length_bytes = 0;
    out_segs.reserve(fm->fm_mapped_extents);

    const auto* extents = reinterpret_cast<const struct fiemap_extent*>(fm->fm_extents);
    for (std::uint32_t i = 0; i < fm->fm_mapped_extents; ++i) {
        const auto& ext = extents[i];
        if (ext.fe_length == 0) {
            continue;
        }

        const std::uint64_t start_lba = ext.fe_physical / FIEMAP_LBA_BYTES;
        const std::uint64_t extent_blocks = div_round_up(ext.fe_length, FIEMAP_LBA_BYTES);
        if (extent_blocks == 0) {
            continue;
        }

        std::uint64_t remaining = extent_blocks;
        std::uint64_t lba = start_lba;

        while (remaining > 0) {
            const std::uint64_t chunk = std::min<std::uint64_t>(remaining, max_blocks_per_seg);
            out_segs.push_back(NvmeSeg{lba, static_cast<std::uint32_t>(chunk)});

            lba += chunk;
            remaining -= chunk;
            out_length_bytes += static_cast<std::size_t>(chunk * FIEMAP_LBA_BYTES);
        }
    }
}
