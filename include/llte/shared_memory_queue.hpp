#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <type_traits>

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
    // Identifies the process that created this channel, so a destructor can tell
    // "my segment" from "a segment that happens to have my name" before unlinking.
    std::atomic<std::uint32_t> creator_pid;
    std::atomic<std::uint64_t> produced_count;
    EventQueue queue;
};

// A std::atomic that is not lock-free falls back to a lock held in whichever
// process's memory it was constructed in, which would silently fail to
// synchronize across the mapping. Cross-process use is only sound if these are
// genuinely lock-free, so require it at compile time rather than hope.
static_assert(std::atomic<std::size_t>::is_always_lock_free,
              "the ring's indexes must be lock-free to work across processes");
static_assert(std::atomic<std::uint32_t>::is_always_lock_free);
static_assert(std::atomic<std::uint64_t>::is_always_lock_free);
static_assert(std::is_trivially_destructible_v<ShmChannel>,
              "the channel outlives the process that created it");

// RAII wrapper over shm_open/ftruncate/mmap. The creator constructs the channel
// and publishes `ready`; the attacher spins until it sees that flag.
class SharedMemoryQueue {
public:
    SharedMemoryQueue() = default;
    ~SharedMemoryQueue();

    SharedMemoryQueue(const SharedMemoryQueue&) = delete;
    SharedMemoryQueue& operator=(const SharedMemoryQueue&) = delete;

    // Creates (and zeroes) the segment, then constructs the channel in place.
    // Fails if the name is already taken, which is how a second producer on one
    // channel is caught; `takeover` removes an existing segment first and is only
    // for clearing the remains of a crashed run.
    bool create(const std::string& name, std::string& error, bool takeover = false);

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
