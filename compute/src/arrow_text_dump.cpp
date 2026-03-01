#include "arrow_text_dump.h"

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

namespace {

struct Options {
    std::string input_path;
    std::string output_path;
    std::string index_path;
    std::string column = "text";
    std::string delimiter = "\n";
    std::int64_t max_rows = -1;
};

void print_usage(const char* prog) {
    std::cerr
        << "Usage: " << prog << " <input.arrow> <output.txt> [options]\n"
        << "Options:\n"
        << "  --column <name>     Column name to extract (default: text)\n"
        << "  --index <path>      Write index file (offset\\tlen per row)\n"
        << "  --delimiter <str>   Delimiter between rows (default: \\n)\n"
        << "  --max-rows <n>      Limit rows for debugging\n"
        << "  --help              Show this message\n";
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

std::string decode_escapes(const std::string& input) {
    std::string out;
    out.reserve(input.size());
    for (std::size_t i = 0; i < input.size(); ++i) {
        if (input[i] != '\\' || i + 1 >= input.size()) {
            out.push_back(input[i]);
            continue;
        }
        const char next = input[i + 1];
        switch (next) {
        case 'n':
            out.push_back('\n');
            break;
        case 't':
            out.push_back('\t');
            break;
        case 'r':
            out.push_back('\r');
            break;
        case '0':
            out.push_back('\0');
            break;
        case '\\':
            out.push_back('\\');
            break;
        default:
            out.push_back(next);
            break;
        }
        ++i;
    }
    return out;
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
        } else if (arg == "--index" && i + 1 < argc) {
            opt.index_path = argv[++i];
        } else if (arg == "--delimiter" && i + 1 < argc) {
            opt.delimiter = decode_escapes(argv[++i]);
        } else if (arg == "--max-rows" && i + 1 < argc) {
            if (!parse_i64(argv[++i], opt.max_rows) || opt.max_rows <= 0) {
                std::cerr << "Invalid max-rows\n";
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

} // namespace

int main(int argc, char** argv) {
    Options opt;
    if (!parse_args(argc, argv, opt)) {
        print_usage(argv[0]);
        return 1;
    }

    std::string error;
    ArrowDumpStats stats;
    if (!DumpArrowText(opt.input_path,
                       opt.output_path,
                       opt.column,
                       opt.index_path,
                       opt.delimiter,
                       opt.max_rows,
                       &stats,
                       &error)) {
        std::cerr << error << "\n";
        return 1;
    }

    return 0;
}
