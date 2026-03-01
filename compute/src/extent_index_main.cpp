#include "extent-index.h"

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>

namespace {

struct Options {
    std::string input_path;
    std::string output_path;
    std::string column = "text";
    std::int64_t max_rows = -1;
    std::size_t max_extents = 128;
};

void print_usage(const char* prog) {
    std::cerr
        << "Usage: " << prog << " <input.arrow> <output.json> [options]\n"
        << "Options:\n"
        << "  --column <name>       Column name to index (default: text)\n"
        << "  --max-rows <n>        Limit rows (default: -1, all)\n"
        << "  --max-extents <n>     Max FIEMAP extents to query (default: 128)\n"
        << "  --help                Show this message\n";
}

bool parse_i64(const char* s, std::int64_t& out) {
    if (s == nullptr || *s == '\0') {
        return false;
    }
    char* end = nullptr;
    errno = 0;
    const long long v = std::strtoll(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0') {
        return false;
    }
    out = static_cast<std::int64_t>(v);
    return true;
}

bool parse_u64(const char* s, std::size_t& out) {
    if (s == nullptr || *s == '\0') {
        return false;
    }
    char* end = nullptr;
    errno = 0;
    const unsigned long long v = std::strtoull(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0') {
        return false;
    }
    out = static_cast<std::size_t>(v);
    return true;
}

bool parse_args(int argc, char** argv, Options& opt) {
    if (argc < 3) {
        return false;
    }
    int positional = 0;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--help") {
            return false;
        } else if (arg == "--column" && i + 1 < argc) {
            opt.column = argv[++i];
        } else if (arg == "--max-rows" && i + 1 < argc) {
            if (!parse_i64(argv[++i], opt.max_rows)) {
                std::cerr << "Invalid max-rows\n";
                return false;
            }
        } else if (arg == "--max-extents" && i + 1 < argc) {
            if (!parse_u64(argv[++i], opt.max_extents) || opt.max_extents == 0) {
                std::cerr << "Invalid max-extents\n";
                return false;
            }
        } else if (!arg.empty() && arg[0] == '-') {
            std::cerr << "Unknown option: " << arg << "\n";
            return false;
        } else {
            if (positional == 0) {
                opt.input_path = arg;
            } else if (positional == 1) {
                opt.output_path = arg;
            } else {
                std::cerr << "Too many positional arguments\n";
                return false;
            }
            ++positional;
        }
    }
    return (!opt.input_path.empty() && !opt.output_path.empty());
}

void write_json_string(std::ostream& out, const std::string& s) {
    out << "\"";
    for (char c : s) {
        switch (c) {
        case '\\':
            out << "\\\\";
            break;
        case '"':
            out << "\\\"";
            break;
        case '\n':
            out << "\\n";
            break;
        case '\r':
            out << "\\r";
            break;
        case '\t':
            out << "\\t";
            break;
        default:
            out << c;
            break;
        }
    }
    out << "\"";
}

bool write_extent_index_json(const ExtentIndex& index, const std::string& path, std::string* error) {
    std::ofstream out(path, std::ios::binary);
    if (!out) {
        if (error) {
            *error = "Failed to open output JSON: " + path;
        }
        return false;
    }

    out << "{\n";
    out << "  \"arrow_path\": ";
    write_json_string(out, index.arrow_path);
    out << ",\n";
    out << "  \"column\": ";
    write_json_string(out, index.column);
    out << ",\n";
    out << "  \"buffers\": [\n";

    for (std::size_t i = 0; i < index.buffers.size(); ++i) {
        const auto& buf = index.buffers[i];
        out << "    {\n";
        out << "      \"batch_index\": " << buf.batch_index << ",\n";
        out << "      \"num_rows\": " << buf.num_rows << ",\n";
        out << "      \"data_range\": {\"offset\": " << buf.data_range.offset
            << ", \"length\": " << buf.data_range.length << "},\n";
        out << "      \"lba_extents\": [\n";
        for (std::size_t j = 0; j < buf.lba_extents.size(); ++j) {
            const auto& ext = buf.lba_extents[j];
            out << "        {\"slba\": " << ext.slba
                << ", \"nblocks\": " << ext.nblocks << "}";
            if (j + 1 < buf.lba_extents.size()) {
                out << ",";
            }
            out << "\n";
        }
        out << "      ]\n";
        out << "    }";
        if (i + 1 < index.buffers.size()) {
            out << ",";
        }
        out << "\n";
    }

    out << "  ]\n";
    out << "}\n";
    return true;
}

} // namespace

int main(int argc, char** argv) {
    Options opt;
    if (!parse_args(argc, argv, opt)) {
        print_usage(argv[0]);
        return 1;
    }

    ExtentIndexOptions options;
    options.column = opt.column;
    options.max_rows = opt.max_rows;
    options.max_extents = opt.max_extents;

    ExtentIndex index;
    std::string error;
    if (!BuildArrowTextExtentIndex(opt.input_path, options, &index, &error)) {
        std::cerr << "extent-index build failed: " << error << "\n";
        return 1;
    }

    if (!write_extent_index_json(index, opt.output_path, &error)) {
        std::cerr << "extent-index write failed: " << error << "\n";
        return 1;
    }

    std::cout << "extent-index written: " << opt.output_path << "\n";
    std::cout << "buffers: " << index.buffers.size() << "\n";
    return 0;
}
