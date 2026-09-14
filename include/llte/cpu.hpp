#pragma once

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <pthread.h>
#include <sched.h>

namespace llte {

// Pinning keeps a hot thread on one core so it stops paying migration and
// cold-cache costs. Returns false when the core is unavailable to this process.
inline bool pin_to_cpu(int cpu) {
    if (cpu < 0) {
        return true;
    }
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    return ::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set) == 0;
}

}  // namespace llte
