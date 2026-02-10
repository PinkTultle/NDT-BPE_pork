#include "common.h"

#include <algorithm>
#include <atomic>
#include <csignal>
#include <cstdint>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <sys/ipc.h>
#include <sys/shm.h>

namespace {

constexpr std::uint64_t kStatsMagic = 0x4250455354415431ULL; // "BPESTAT1"

volatile std::sig_atomic_t g_stop = 0;

void handle_sigint(int) {
    g_stop = 1;
}

std::uint64_t load_u64(const std::uint64_t* p) {
    return __atomic_load_n(p, __ATOMIC_RELAXED);
}

std::uint32_t load_u32(const std::uint32_t* p) {
    return __atomic_load_n(p, __ATOMIC_RELAXED);
}

double bytes_to_mb(std::uint64_t bytes) {
    return static_cast<double>(bytes) / (1024.0 * 1024.0);
}

void print_usage(const char* prog) {
    std::printf("Usage: %s [--interval-ms N] [--cpu-cores 0-5]\n", prog);
}

struct CpuTimes {
    std::uint64_t user = 0;
    std::uint64_t nice = 0;
    std::uint64_t system = 0;
    std::uint64_t idle = 0;
    std::uint64_t iowait = 0;
    std::uint64_t irq = 0;
    std::uint64_t softirq = 0;
    std::uint64_t steal = 0;
};

bool read_proc_stat(std::vector<CpuTimes>& out) {
    std::ifstream in("/proc/stat");
    if (!in.is_open()) {
        return false;
    }
    std::string line;
    while (std::getline(in, line)) {
        if (line.rfind("cpu", 0) != 0 || line.size() < 4 || !std::isdigit(line[3])) {
            continue;
        }
        std::istringstream iss(line);
        std::string label;
        iss >> label;
        int cpu_id = std::atoi(label.c_str() + 3);
        if (cpu_id < 0) {
            continue;
        }
        CpuTimes t{};
        iss >> t.user >> t.nice >> t.system >> t.idle >> t.iowait >> t.irq >> t.softirq >> t.steal;
        if (cpu_id >= static_cast<int>(out.size())) {
            out.resize(cpu_id + 1);
        }
        out[cpu_id] = t;
    }
    return true;
}

std::vector<int> parse_cores(const std::string& spec) {
    std::set<int> cores;
    std::istringstream ss(spec);
    std::string token;
    while (std::getline(ss, token, ',')) {
        if (token.empty()) continue;
        auto dash = token.find('-');
        if (dash == std::string::npos) {
            cores.insert(std::atoi(token.c_str()));
            continue;
        }
        int start = std::atoi(token.substr(0, dash).c_str());
        int end = std::atoi(token.substr(dash + 1).c_str());
        if (end < start) std::swap(start, end);
        for (int c = start; c <= end; ++c) {
            cores.insert(c);
        }
    }
    return std::vector<int>(cores.begin(), cores.end());
}

double cpu_usage(const CpuTimes& prev, const CpuTimes& cur) {
    const std::uint64_t prev_idle = prev.idle + prev.iowait;
    const std::uint64_t cur_idle = cur.idle + cur.iowait;
    const std::uint64_t prev_total = prev.user + prev.nice + prev.system + prev.idle +
                                     prev.iowait + prev.irq + prev.softirq + prev.steal;
    const std::uint64_t cur_total = cur.user + cur.nice + cur.system + cur.idle +
                                    cur.iowait + cur.irq + cur.softirq + cur.steal;
    const std::uint64_t total_delta = cur_total - prev_total;
    const std::uint64_t idle_delta = cur_idle - prev_idle;
    if (total_delta == 0) {
        return 0.0;
    }
    const double busy = static_cast<double>(total_delta - idle_delta);
    return 100.0 * busy / static_cast<double>(total_delta);
}

} // namespace

int main(int argc, char** argv) {
    int interval_ms = 1000;
    std::vector<int> cpu_cores = parse_cores("0-5");
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--interval-ms") == 0 && i + 1 < argc) {
            interval_ms = std::max(100, std::atoi(argv[++i]));
        } else if (std::strcmp(argv[i], "--cpu-cores") == 0 && i + 1 < argc) {
            cpu_cores = parse_cores(argv[++i]);
        } else if (std::strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            print_usage(argv[0]);
            return 1;
        }
    }

    const int id = shmget(STATS_SHM_KEY, STATS_SHM_SIZE, 0660);
    if (id < 0) {
        std::perror("[BPE-MON] stats shmget");
        return 1;
    }

    void* ptr = shmat(id, nullptr, SHM_RDONLY);
    if (ptr == reinterpret_cast<void*>(-1)) {
        std::perror("[BPE-MON] stats shmat");
        return 1;
    }

    auto* stats = reinterpret_cast<BpeRuntimeStats*>(ptr);
    if (load_u64(&stats->magic) != kStatsMagic) {
        std::fprintf(stderr, "[BPE-MON] stats magic mismatch (not initialized)\n");
        return 1;
    }

    std::signal(SIGINT, handle_sigint);
    std::signal(SIGTERM, handle_sigint);

    std::uint64_t prev_req = 0;
    std::uint64_t prev_resp = 0;
    std::uint64_t prev_in = 0;
    std::uint64_t prev_out = 0;
    std::vector<CpuTimes> prev_cpu;
    std::vector<CpuTimes> cur_cpu;

    while (!g_stop) {
        cur_cpu.clear();
        const bool have_cpu = read_proc_stat(cur_cpu);
        const std::uint64_t req_total = load_u64(&stats->req_total);
        const std::uint64_t resp_total = load_u64(&stats->resp_total);
        const std::uint64_t bytes_in = load_u64(&stats->bytes_in);
        const std::uint64_t bytes_out = load_u64(&stats->bytes_out);
        const std::uint64_t start_ts = load_u64(&stats->start_ts_us);
        const std::uint64_t last_ts = load_u64(&stats->last_ts_us);
        const std::uint64_t last_latency = load_u64(&stats->last_latency_us);
        const std::uint64_t last_req_id = load_u64(&stats->last_req_id);
        const std::uint32_t last_slot = load_u32(&stats->last_slot);
        const std::uint32_t last_resp_bytes = load_u32(&stats->last_resp_bytes);

        const double delta_req = static_cast<double>(req_total - prev_req) * 1000.0 / interval_ms;
        const double delta_resp = static_cast<double>(resp_total - prev_resp) * 1000.0 / interval_ms;
        const double delta_in_mb = bytes_to_mb(bytes_in - prev_in) * 1000.0 / interval_ms;
        const double delta_out_mb = bytes_to_mb(bytes_out - prev_out) * 1000.0 / interval_ms;

        std::printf("\033[2J\033[H");
        std::printf("BPE Runtime Monitor (interval=%dms)\n", interval_ms);
        std::printf("------------------------------------------------------------\n");
        std::printf("Totals   req=%" PRIu64 "  resp=%" PRIu64 "  in=%.2fMB  out=%.2fMB\n",
                    req_total, resp_total, bytes_to_mb(bytes_in), bytes_to_mb(bytes_out));
        std::printf("Rates    req/s=%.1f  resp/s=%.1f  in=%.2fMB/s  out=%.2fMB/s\n",
                    delta_req, delta_resp, delta_in_mb, delta_out_mb);
        std::printf("Last     req_id=%" PRIu64 " slot=%u resp_bytes=%u latency_us=%" PRIu64 "\n",
                    last_req_id, last_slot, last_resp_bytes, last_latency);
        std::printf("TS       start_us=%" PRIu64 " last_us=%" PRIu64 "\n",
                    start_ts, last_ts);
        if (have_cpu && !cpu_cores.empty()) {
            std::printf("CPU      ");
            for (std::size_t i = 0; i < cpu_cores.size(); ++i) {
                const int core = cpu_cores[i];
                double usage = 0.0;
                if (!prev_cpu.empty() &&
                    core >= 0 &&
                    core < static_cast<int>(prev_cpu.size()) &&
                    core < static_cast<int>(cur_cpu.size())) {
                    usage = cpu_usage(prev_cpu[core], cur_cpu[core]);
                }
                std::printf("%d=%.0f%%", core, usage);
                if (i + 1 < cpu_cores.size()) {
                    std::printf(" ");
                }
            }
            std::printf("\n");
        }
        std::printf("------------------------------------------------------------\n");
        std::printf("Per-slot:\n");
        for (std::size_t s = 0; s < NUM_SLOTS; ++s) {
            const std::uint64_t s_req = load_u64(&stats->per_slot_req[s]);
            const std::uint64_t s_resp = load_u64(&stats->per_slot_resp[s]);
            const std::uint64_t s_in = load_u64(&stats->per_slot_bytes_in[s]);
            const std::uint64_t s_out = load_u64(&stats->per_slot_bytes_out[s]);
            if (s_req == 0 && s_resp == 0) {
                continue;
            }
            std::printf("  slot %2zu: req=%" PRIu64 " resp=%" PRIu64 " in=%.2fMB out=%.2fMB\n",
                        s, s_req, s_resp, bytes_to_mb(s_in), bytes_to_mb(s_out));
        }
        std::fflush(stdout);

        prev_req = req_total;
        prev_resp = resp_total;
        prev_in = bytes_in;
        prev_out = bytes_out;
        if (have_cpu) {
            prev_cpu = cur_cpu;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
    }

    return 0;
}
