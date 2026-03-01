#include "arrow_text_dump.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <fstream>
#include <string_view>

namespace {

struct Reader {
    enum class Kind { kFile, kStream };
    Kind kind;
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> file_reader;
    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> stream_reader;
    std::shared_ptr<arrow::Schema> schema;
};

void set_error(std::string* error, const std::string& msg) {
    if (error) {
        *error = msg;
    }
}

arrow::Result<Reader> open_reader(const std::string& path) {
    ARROW_ASSIGN_OR_RAISE(auto infile, arrow::io::ReadableFile::Open(path));
    auto file_result = arrow::ipc::RecordBatchFileReader::Open(infile);
    if (file_result.ok()) {
        Reader out{Reader::Kind::kFile, file_result.ValueOrDie(), nullptr,
                   file_result.ValueOrDie()->schema()};
        return out;
    }

    ARROW_ASSIGN_OR_RAISE(auto stream, arrow::io::ReadableFile::Open(path));
    auto stream_result = arrow::ipc::RecordBatchStreamReader::Open(stream);
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

} // namespace

bool DumpArrowText(const std::string& input_path,
                   const std::string& output_path,
                   const std::string& column,
                   const std::string& index_path,
                   const std::string& delimiter,
                   std::int64_t max_rows,
                   ArrowDumpStats* stats,
                   std::string* error) {
    if (stats) {
        *stats = ArrowDumpStats{};
    }

    auto reader_result = open_reader(input_path);
    if (!reader_result.ok()) {
        set_error(error, "Failed to open arrow file: " + reader_result.status().ToString());
        return false;
    }
    auto reader = reader_result.ValueOrDie();

    auto col_idx_result = get_column_index(reader.schema, column);
    if (!col_idx_result.ok()) {
        set_error(error, col_idx_result.status().ToString());
        return false;
    }
    const int col_idx = col_idx_result.ValueOrDie();

    std::ofstream out(output_path, std::ios::binary);
    if (!out) {
        set_error(error, "Failed to open output file: " + output_path);
        return false;
    }

    std::ofstream idx;
    if (!index_path.empty()) {
        idx.open(index_path, std::ios::binary);
        if (!idx) {
            set_error(error, "Failed to open index file: " + index_path);
            return false;
        }
    }

    std::uint64_t offset = 0;
    std::int64_t remaining = max_rows;
    std::int64_t rows = 0;

    auto write_row = [&](std::string_view view) {
        out.write(view.data(), static_cast<std::streamsize>(view.size()));
        out.write(delimiter.data(), static_cast<std::streamsize>(delimiter.size()));
        if (idx) {
            idx << offset << "\t" << view.size() << "\n";
        }
        offset += static_cast<std::uint64_t>(view.size() + delimiter.size());
        ++rows;
        if (remaining > 0) {
            --remaining;
        }
    };

    auto process_array = [&](const std::shared_ptr<arrow::Array>& array) -> bool {
        const auto type_id = array->type_id();
        if (type_id == arrow::Type::STRING) {
            auto str_array = std::static_pointer_cast<arrow::StringArray>(array);
            for (int64_t r = 0; r < str_array->length(); ++r) {
                if (remaining == 0) break;
                std::string_view view;
                if (!str_array->IsNull(r)) {
                    view = str_array->GetView(r);
                }
                write_row(view);
            }
        } else if (type_id == arrow::Type::LARGE_STRING) {
            auto str_array = std::static_pointer_cast<arrow::LargeStringArray>(array);
            for (int64_t r = 0; r < str_array->length(); ++r) {
                if (remaining == 0) break;
                std::string_view view;
                if (!str_array->IsNull(r)) {
                    view = str_array->GetView(r);
                }
                write_row(view);
            }
        } else if (type_id == arrow::Type::BINARY) {
            auto bin_array = std::static_pointer_cast<arrow::BinaryArray>(array);
            for (int64_t r = 0; r < bin_array->length(); ++r) {
                if (remaining == 0) break;
                std::string_view view;
                if (!bin_array->IsNull(r)) {
                    view = bin_array->GetView(r);
                }
                write_row(view);
            }
        } else if (type_id == arrow::Type::LARGE_BINARY) {
            auto bin_array = std::static_pointer_cast<arrow::LargeBinaryArray>(array);
            for (int64_t r = 0; r < bin_array->length(); ++r) {
                if (remaining == 0) break;
                std::string_view view;
                if (!bin_array->IsNull(r)) {
                    view = bin_array->GetView(r);
                }
                write_row(view);
            }
        } else {
            set_error(error, "Unsupported column type: " + array->type()->ToString());
            return false;
        }
        return true;
    };

    auto process_batch = [&](const std::shared_ptr<arrow::RecordBatch>& batch) -> bool {
        auto array = batch->column(col_idx);
        if (!array) {
            set_error(error, "Column array missing in batch");
            return false;
        }
        return process_array(array);
    };

    if (reader.kind == Reader::Kind::kFile) {
        for (int i = 0; i < reader.file_reader->num_record_batches(); ++i) {
            auto batch_result = reader.file_reader->ReadRecordBatch(i);
            if (!batch_result.ok()) {
                set_error(error,
                          "Failed to read record batch: " + batch_result.status().ToString());
                return false;
            }
            if (!process_batch(batch_result.ValueOrDie())) {
                return false;
            }
            if (remaining == 0) break;
        }
    } else {
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
            if (!process_batch(batch_with_meta.batch)) {
                return false;
            }
            if (remaining == 0) break;
        }
    }

    if (stats) {
        stats->rows = rows;
        stats->output_bytes = offset;
    }
    return true;
}
