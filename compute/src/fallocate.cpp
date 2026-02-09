#include "../include/fallocate.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <unistd.h>
#include <sys/mman.h>

namespace {

constexpr std::size_t round_up(std::size_t x, std::size_t unit) noexcept {
    return (unit == 0) ? x : ((x + unit - 1) / unit) * unit;
}

} // namespace

ffilesystem::~ffilesystem() {
    free_metadata();    
}

void ffilesystem::allocate_metadata(bool zero) {
    if (metadata_ptr_ != nullptr) {
        if (zero) {
            std::memset(metadata_ptr_, 0, metadata_size_);
            (void)::msync(metadata_ptr_, metadata_size_, MS_SYNC);
        }
        return;
    }

    metadata_size_ = round_up(MDTS_BYTES, ALIGN_BYTES);
    const int fd = ::open(metadata_file_path_.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) {
        throw std::system_error(errno, std::generic_category(), "open metadata file failed");
    }

    const int alloc_rc = ::posix_fallocate(fd, 0, static_cast<off_t>(metadata_size_));
    if (alloc_rc != 0) {
        ::close(fd);
        throw std::system_error(alloc_rc, std::generic_category(), "posix_fallocate failed");
    }

    void* ptr = ::mmap(nullptr, metadata_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) {
        const int saved_errno = errno;
        ::close(fd);
        throw std::system_error(saved_errno, std::generic_category(), "mmap metadata failed");
    }

    metadata_fd_ = fd;
    metadata_ptr_ = ptr;

    if (zero) {
        std::memset(metadata_ptr_, 0, metadata_size_);
        (void)::msync(metadata_ptr_, metadata_size_, MS_SYNC);
    }
}

void ffilesystem::free_metadata() noexcept {
    if (metadata_ptr_ != nullptr) {
        (void)::munmap(metadata_ptr_, metadata_size_);
        metadata_ptr_ = nullptr;
    }

    if (metadata_fd_ >= 0) {
        (void)::close(metadata_fd_);
        metadata_fd_ = -1;
    }

    metadata_size_ = 0;
}

void ffilesystem::advocate_metadata(std::size_t new_size, bool zero) {
    if (new_size == 0) {
        free_metadata();
        return;
    }

    const std::size_t rounded = round_up(new_size, ALIGN_BYTES);

    if (metadata_ptr_ == nullptr) {
        metadata_size_ = rounded;
        const int fd = ::open(metadata_file_path_.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
        if (fd < 0) {
            throw std::system_error(errno, std::generic_category(), "open metadata file failed");
        }

        const int alloc_rc = ::posix_fallocate(fd, 0, static_cast<off_t>(metadata_size_));
        if (alloc_rc != 0) {
            ::close(fd);
            throw std::system_error(alloc_rc, std::generic_category(), "posix_fallocate failed");
        }

        void* ptr = ::mmap(nullptr, metadata_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            const int saved_errno = errno;
            ::close(fd);
            throw std::system_error(saved_errno, std::generic_category(), "mmap metadata failed");
        }

        metadata_fd_ = fd;
        metadata_ptr_ = ptr;
        if (zero) {
            std::memset(metadata_ptr_, 0, metadata_size_);
            (void)::msync(metadata_ptr_, metadata_size_, MS_SYNC);
        }
        return;
    }

    if (metadata_fd_ < 0) {
        throw std::logic_error("metadata fd is invalid");
    }

    if (rounded == metadata_size_) {
        if (zero) {
            std::memset(metadata_ptr_, 0, metadata_size_);
            (void)::msync(metadata_ptr_, metadata_size_, MS_SYNC);
        }
        return;
    }

    const std::size_t old_size = metadata_size_;

    if (rounded > metadata_size_) {
        const int alloc_rc = ::posix_fallocate(metadata_fd_, 0, static_cast<off_t>(rounded));
        if (alloc_rc != 0) {
            throw std::system_error(alloc_rc, std::generic_category(), "posix_fallocate failed");
        }
    } else {
        if (::ftruncate(metadata_fd_, static_cast<off_t>(rounded)) != 0) {
            throw std::system_error(errno, std::generic_category(), "ftruncate failed");
        }
    }

    if (metadata_ptr_ != nullptr) {
        (void)::msync(metadata_ptr_, metadata_size_, MS_SYNC);
        (void)::munmap(metadata_ptr_, metadata_size_);
        metadata_ptr_ = nullptr;
    }

    void* ptr = ::mmap(nullptr, rounded, PROT_READ | PROT_WRITE, MAP_SHARED, metadata_fd_, 0);
    if (ptr == MAP_FAILED) {
        const int saved_errno = errno;
        throw std::system_error(saved_errno, std::generic_category(), "mmap metadata failed");
    }

    metadata_ptr_ = ptr;
    metadata_size_ = rounded;

    if (zero && rounded > old_size) {
        std::memset(static_cast<char*>(metadata_ptr_) + old_size, 0, rounded - old_size);
        (void)::msync(metadata_ptr_, metadata_size_, MS_SYNC);
    }
}

void ffilesystem::convert_metadata_to_nvme_cmds(std::vector<NvmePassthruCmd>& out_cmds) const {
    out_cmds.clear();

    if (metadata_ptr_ == nullptr) {
        throw std::logic_error("metadata is not allocated");
    }

    out_cmds.reserve(metadata_segs_.size());
    std::size_t offset = 0;

    for (const auto& seg : metadata_segs_) {
        if (seg.nblocks == 0) {
            continue;
        }

        const std::size_t bytes = static_cast<std::size_t>(seg.nblocks) * LBA_BYTES;
        if (offset >= metadata_size_) {
            break;
        }

        NvmePassthruCmd cmd{};
        cmd.cdw10 = static_cast<std::uint32_t>(seg.slba & 0xFFFFFFFFULL);
        cmd.cdw11 = static_cast<std::uint32_t>((seg.slba >> 32) & 0xFFFFFFFFULL);
        cmd.cdw12 = seg.nblocks;
        cmd.data = static_cast<char*>(metadata_ptr_) + offset;
        cmd.data_len = static_cast<std::uint32_t>(std::min(bytes, metadata_size_ - offset));

        out_cmds.push_back(cmd);
        offset += bytes;
    }
}

void ffilesystem::reallocate_metadata(bool zero) {
    free_metadata();
    allocate_metadata(zero);
}

void ffilesystem::flush_metadata() {
    if (metadata_ptr_ == nullptr) {
        throw std::logic_error("metadata is not allocated");
    }
    if (::msync(metadata_ptr_, metadata_size_, MS_SYNC) != 0) {
        throw std::system_error(errno, std::generic_category(), "msync failed");
    }
}

void ffilesystem::load_metadata() {
    if (metadata_ptr_ != nullptr) {
        return;
    }
    allocate_metadata(false);
}

void ffilesystem::set_metadata_segments(std::vector<NvmeSeg> segs) {
    metadata_segs_ = std::move(segs);
}
