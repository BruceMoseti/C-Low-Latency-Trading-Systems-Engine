#pragma once

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <pthread.h>
#include <sched.h>

#include <cstdio>
#include <cstdlib>
#include <string>

namespace llte {

// Pinning keeps a hot thread on one core so it stops paying migration and
// cold-cache costs. Returns false when the core is unavailable to this process,
// which happens routinely under a restricted cpuset.
//
// [[nodiscard]] on purpose: every latency number this project reports is
// qualified as "pinned", so a failure that is discarded turns the label into a
// claim nothing checks.
[[nodiscard]] inline bool pin_to_cpu(int cpu) {
    if (cpu < 0) {
        return true;
    }
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    return ::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set) == 0;
}

// The cores this thread is actually allowed to run on, so a log can state what
// happened rather than what was requested.
inline std::string current_affinity() {
    cpu_set_t set;
    CPU_ZERO(&set);
    if (::pthread_getaffinity_np(::pthread_self(), sizeof(set), &set) != 0) {
        return "unknown";
    }
    std::string cores;
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (CPU_ISSET(cpu, &set)) {
            if (!cores.empty()) {
                cores += ",";
            }
            cores += std::to_string(cpu);
        }
    }
    return cores.empty() ? "none" : cores;
}

// Pins and says so if it could not. Use where an unpinned run is degraded but
// still useful; benchmarks should treat failure as fatal instead.
inline bool pin_to_cpu_or_warn(int cpu, const char* who) {
    if (pin_to_cpu(cpu)) {
        return true;
    }
    std::fprintf(stderr, "%s: could not pin to cpu %d (allowed: %s); running unpinned\n", who,
                 cpu, current_affinity().c_str());
    return false;
}

// Stops the run when pinning was asked for and did not happen. A benchmark that
// reports "pinned" numbers while silently running unpinned is worse than one
// that refuses to run, and a restricted cpuset makes that a routine occurrence.
inline void require_pinned(int cpu, const char* who) {
    if (!pin_to_cpu(cpu)) {
        std::fprintf(stderr, "fatal: could not pin %s to cpu %d (allowed: %s)\n", who, cpu,
                     current_affinity().c_str());
        std::abort();
    }
}

}  // namespace llte
