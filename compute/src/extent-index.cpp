#include "extent-index.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <system_error>

#include <fcntl.h>
#include <linux/fiemap.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <unistd.h>

namespace {

struct FileExtent {
    std::uint64_t logical = 0;
    std::uint64_t physical = 0;
    std::uint64_t length = 0;
};

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

void set_error(std::string* error, const std::string& msg) {
    if (error) {
        *error = msg;
    }
}

std::uint64_t div_round_up(std::uint64_t n, std::uint64_t d) {
    return (n + d - 1) / d;
}

bool load_fiemap_extents(const std::string& path,
                         std::size_t max_extents,
                         std::vector<FileExtent>* out,
                         std::string* error) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        set_error(error, std::string("open failed: ") + std::strerror(errno));
        return false;
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
        set_error(error, std::string("FS_IOC_FIEMAP failed: ") + std::strerror(errno));
        return false;
    }

    out->clear();
    out->reserve(fm->fm_mapped_extents);

    const auto* extents = reinterpret_cast<const struct fiemap_extent*>(fm->fm_extents);
    for (std::uint32_t i = 0; i < fm->fm_mapped_extents; ++i) {
        const auto& ext = extents[i];
        if (ext.fe_length == 0) {
            continue;
        }
        out->push_back(FileExtent{
            static_cast<std::uint64_t>(ext.fe_logical),
            static_cast<std::uint64_t>(ext.fe_physical),
            static_cast<std::uint64_t>(ext.fe_length),
        });
    }
    return true;
}

bool map_range_to_lba_extents(const std::vector<FileExtent>& extents,
                              const FileByteRange& range,
                              std::vector<LbaExtent>* out,
                              std::string* error) {
    const std::uint64_t range_start = range.offset;
    const std::uint64_t range_end = range.offset + range.length;
    if (range.length == 0) {
        return true;
    }

    out->clear();
    std::uint64_t covered = 0;

    for (const auto& ext : extents) {
        const std::uint64_t ext_start = ext.logical;
        const std::uint64_t ext_end = ext.logical + ext.length;
        if (ext_end <= range_start || ext_start >= range_end) {
            continue;
        }
        const std::uint64_t overlap_start = std::max(range_start, ext_start);
        const std::uint64_t overlap_end = std::min(range_end, ext_end);
        const std::uint64_t overlap_len = overlap_end - overlap_start;
        if (overlap_len == 0) {
            continue;
        }

        const std::uint64_t phys = ext.physical + (overlap_start - ext.logical);
        const std::uint64_t slba = phys / 512ULL;
        const std::uint64_t end_phys = phys + overlap_len;
        const std::uint64_t end_slba = div_round_up(end_phys, 512ULL);
        const std::uint64_t nblocks64 = end_slba - slba;
        if (nblocks64 == 0 || nblocks64 > 0xFFFFFFFFULL) {
            set_error(error, "computed nblocks is invalid");
            return false;
        }

        out->push_back(LbaExtent{
            slba,
            static_cast<std::uint32_t>(nblocks64),
        });
        covered += overlap_len;
    }

    if (covered < range.length) {
        set_error(error, "range not fully covered by FIEMAP extents (sparse or hole)");
        return false;
    }
    return true;
}

struct Reader {
    enum class Kind { kFile, kStream };
    Kind kind;
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> file_reader;
    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> stream_reader;
    std::shared_ptr<arrow::Schema> schema;
};

arrow::Result<Reader> open_reader(const std::shared_ptr<arrow::io::MemoryMappedFile>& file) {
    auto file_result = arrow::ipc::RecordBatchFileReader::Open(file);
    if (file_result.ok()) {
        Reader out{Reader::Kind::kFile, file_result.ValueOrDie(), nullptr,
                   file_result.ValueOrDie()->schema()};
        return out;
    }

    auto stream_result = arrow::ipc::RecordBatchStreamReader::Open(file);
    if (!stream_result.ok()) {
        return stream_result.status();
    }
    Reader out{Reader::Kind::kStream, nullptr, stream_result.ValueOrDie(),
               stream_result.ValueOrDie()->schema()};
    return out;
}

arrow::Result<int> get_column_index(const std::shared_ptr<arrow::Schema>& schema,
                                    const std::string& name) {
    const int idx = schema->GetFieldIndex(name);
    if (idx < 0) {
        return arrow::Status::Invalid("column not found: ", name);
    }
    return idx;
}

bool buffer_to_file_range(const std::shared_ptr<arrow::Buffer>& buf,
                          const std::uint8_t* base,
                          std::size_t base_len,
                          FileByteRange* out) {
    if (!buf || buf->size() == 0) {
        out->offset = 0;
        out->length = 0;
        return true;
    }
    const auto* data = buf->data();
    if (data < base || data + buf->size() > base + base_len) {
        return false;
    }
    out->offset = static_cast<std::uint64_t>(data - base);
    out->length = static_cast<std::uint64_t>(buf->size());
    return true;
}

bool string_array_value_range(const std::shared_ptr<arrow::Array>& array,
                              const FileByteRange& values_buffer_range,
                              std::int64_t rows_to_take,
                              FileByteRange* out) {
    if (rows_to_take <= 0) {
        out->offset = 0;
        out->length = 0;
        return true;
    }

    const auto& data = array->data();
    const std::int64_t offset = data->offset;
    const std::int64_t start_index = offset;
    const std::int64_t end_index = offset + rows_to_take;

    if (array->type_id() == arrow::Type::STRING) {
        const auto* offsets =
            reinterpret_cast<const std::int32_t*>(data->buffers[1]->data());
        const std::int64_t start = offsets[start_index];
        const std::int64_t end = offsets[end_index];
        out->offset = values_buffer_range.offset + static_cast<std::uint64_t>(start);
        out->length = static_cast<std::uint64_t>(end - start);
        return true;
    }
    if (array->type_id() == arrow::Type::LARGE_STRING) {
        const auto* offsets =
            reinterpret_cast<const std::int64_t*>(data->buffers[1]->data());
        const std::int64_t start = offsets[start_index];
        const std::int64_t end = offsets[end_index];
        out->offset = values_buffer_range.offset + static_cast<std::uint64_t>(start);
        out->length = static_cast<std::uint64_t>(end - start);
        return true;
    }
    return false;
}

} // namespace

bool BuildArrowTextExtentIndex(const std::string& arrow_path,
                               const ExtentIndexOptions& options,
                               ExtentIndex* out_index,
                               std::string* error) {
    if (!out_index) {
        set_error(error, "out_index is null");
        return false;
    }

    const auto mm_result = arrow::io::MemoryMappedFile::Open(
        arrow_path, arrow::io::FileMode::READ);
    if (!mm_result.ok()) {
        set_error(error, "MemoryMappedFile::Open failed: " + mm_result.status().ToString());
        return false;
    }
    auto mm = mm_result.ValueOrDie();

    auto size_result = mm->GetSize();
    if (!size_result.ok()) {
        set_error(error, "GetSize failed: " + size_result.status().ToString());
        return false;
    }
    const std::size_t file_size = static_cast<std::size_t>(size_result.ValueOrDie());

    auto base_buf_result = mm->ReadAt(0, static_cast<int64_t>(file_size));
    if (!base_buf_result.ok()) {
        set_error(error, "ReadAt(0) failed: " + base_buf_result.status().ToString());
        return false;
    }
    auto base_buf = base_buf_result.ValueOrDie();
    const auto* base_ptr = base_buf->data();

    auto reader_result = open_reader(mm);
    if (!reader_result.ok()) {
        set_error(error, "Failed to open Arrow reader: " + reader_result.status().ToString());
        return false;
    }
    auto reader = reader_result.ValueOrDie();

    auto col_idx_result = get_column_index(reader.schema, options.column);
    if (!col_idx_result.ok()) {
        set_error(error, col_idx_result.status().ToString());
        return false;
    }
    const int col_idx = col_idx_result.ValueOrDie();

    std::vector<FileExtent> extents;
    if (!load_fiemap_extents(arrow_path, options.max_extents, &extents, error)) {
        return false;
    }

    out_index->arrow_path = arrow_path;
    out_index->column = options.column;
    out_index->buffers.clear();

    std::int64_t remaining_rows = options.max_rows;
    auto process_batch = [&](const std::shared_ptr<arrow::RecordBatch>& batch,
                             std::int64_t batch_index) -> bool {
        if (remaining_rows == 0) {
            return true;
        }
        auto array = batch->column(col_idx);
        if (!array) {
            set_error(error, "Column array missing in batch");
            return false;
        }
        if (array->type_id() != arrow::Type::STRING &&
            array->type_id() != arrow::Type::LARGE_STRING) {
            set_error(error, "Unsupported column type: " + array->type()->ToString());
            return false;
        }

        const std::int64_t rows_in_batch = array->length();
        const std::int64_t rows_to_take =
            (remaining_rows < 0) ? rows_in_batch
                                 : std::min<std::int64_t>(rows_in_batch, remaining_rows);

        FileByteRange values_range{};
        if (!buffer_to_file_range(array->data()->buffers[2], base_ptr, file_size, &values_range)) {
            set_error(error,
                      "Buffer does not map into mmapped file; cannot compute file offsets");
            return false;
        }

        FileByteRange data_range{};
        if (!string_array_value_range(array, values_range, rows_to_take, &data_range)) {
            set_error(error, "Failed to compute string data range");
            return false;
        }

        TextBufferExtent entry{};
        entry.batch_index = batch_index;
        entry.num_rows = rows_to_take;
        entry.data_range = data_range;
        if (!map_range_to_lba_extents(extents, data_range, &entry.lba_extents, error)) {
            return false;
        }

        out_index->buffers.push_back(std::move(entry));

        if (remaining_rows > 0) {
            remaining_rows -= rows_to_take;
        }
        return true;
    };

    if (reader.kind == Reader::Kind::kFile) {
        for (int i = 0; i < reader.file_reader->num_record_batches(); ++i) {
            auto batch_result = reader.file_reader->ReadRecordBatch(i);
            if (!batch_result.ok()) {
                set_error(error,
                          "Failed to read record batch: " + batch_result.status().ToString());
                return false;
            }
            if (!process_batch(batch_result.ValueOrDie(), i)) {
                return false;
            }
            if (remaining_rows == 0) break;
        }
    } else {
        std::int64_t batch_index = 0;
        for (;;) {
            auto batch_result = reader.stream_reader->ReadNext();
            if (!batch_result.ok()) {
                set_error(error,
                          "Failed to read record batch: " + batch_result.status().ToString());
                return false;
            }
            auto batch_with_meta = batch_result.ValueOrDie();
            if (!batch_with_meta.batch) {
                break;
            }
            if (!process_batch(batch_with_meta.batch, batch_index)) {
                return false;
            }
            ++batch_index;
            if (remaining_rows == 0) break;
        }
    }

    return true;
}
