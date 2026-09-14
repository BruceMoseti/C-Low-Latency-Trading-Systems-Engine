#pragma once

#include <atomic>
#include <cstdint>
#include <string>

#include "llte/pipeline.hpp"
#include "llte/spsc_queue.hpp"

namespace llte {

inline constexpr std::size_t kIpcQueueCapacity = 1u << 16;
inline constexpr std::uint32_t kChannelReadyMagic = 0x52454459;  // 'REDY'

using EventQueue = SpscQueue<PipelineEvent, kIpcQueueCapacity>;

// What actually lives in the shared mapping. The queue is embedded by value so
// both processes address the same bytes.
struct ShmChannel {
    std::atomic<std::uint32_t> ready;
    std::atomic<std::uint32_t> producer_done;
    std::atomic<std::uint64_t> produced_count;
    EventQueue queue;
};

// RAII wrapper over shm_open/ftruncate/mmap. The creator constructs the channel
// and publishes `ready`; the attacher spins until it sees that flag.
class SharedMemoryQueue {
public:
    SharedMemoryQueue() = default;
    ~SharedMemoryQueue();

    SharedMemoryQueue(const SharedMemoryQueue&) = delete;
    SharedMemoryQueue& operator=(const SharedMemoryQueue&) = delete;

    // Creates (and zeroes) the segment, then constructs the channel in place.
    bool create(const std::string& name, std::string& error);

    // Attaches to an existing segment, waiting up to `timeout_ms` for `ready`.
    bool attach(const std::string& name, int timeout_ms, std::string& error);

    ShmChannel* channel() { return channel_; }

private:
    void close();

    std::string name_;
    bool owner_ = false;
    int fd_ = -1;
    void* region_ = nullptr;
    ShmChannel* channel_ = nullptr;
};

}  // namespace llte
