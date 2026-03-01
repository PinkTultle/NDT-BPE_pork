#pragma once

#include <cstdint>
#include <string>

struct ArrowDumpStats {
    std::int64_t rows = 0;
    std::uint64_t output_bytes = 0;
};

bool DumpArrowText(const std::string& input_path,
                   const std::string& output_path,
                   const std::string& column,
                   const std::string& index_path,
                   const std::string& delimiter,
                   std::int64_t max_rows,
                   ArrowDumpStats* stats,
                   std::string* error);
