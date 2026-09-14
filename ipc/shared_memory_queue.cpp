#include "llte/shared_memory_queue.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <ctime>
#include <new>

namespace llte {
namespace {

std::string errno_message(const char* what) {
    return std::string(what) + ": " + std::strerror(errno);
}

void sleep_ms(int ms) {
    timespec ts{ms / 1000, static_cast<long>(ms % 1000) * 1'000'000L};
    ::nanosleep(&ts, nullptr);
}

}  // namespace

SharedMemoryQueue::~SharedMemoryQueue() { close(); }

bool SharedMemoryQueue::create(const std::string& name, std::string& error, bool takeover) {
    // Unlinking first would defeat O_EXCL entirely, and the ring is
    // single-producer: a second producer on one channel is a correctness failure,
    // so it has to be refused rather than quietly allowed to share.
    if (takeover) {
        ::shm_unlink(name.c_str());
    }

    fd_ = ::shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd_ < 0) {
        if (errno == EEXIST) {
            error = "shared segment " + name +
                    " already exists; another producer owns it. Pass --force-shm to "
                    "reclaim it after a crash.";
        } else {
            error = errno_message("shm_open");
        }
        return false;
    }
    if (::ftruncate(fd_, sizeof(ShmChannel)) < 0) {
        error = errno_message("ftruncate");
        close();
        return false;
    }
    region_ = ::mmap(nullptr, sizeof(ShmChannel), PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (region_ == MAP_FAILED) {
        region_ = nullptr;
        error = errno_message("mmap");
        close();
        return false;
    }

    name_ = name;
    owner_ = true;
    channel_ = new (region_) ShmChannel();
    channel_->producer_done.store(0, std::memory_order_relaxed);
    channel_->produced_count.store(0, std::memory_order_relaxed);
    channel_->creator_pid.store(static_cast<std::uint32_t>(::getpid()),
                                std::memory_order_relaxed);
    channel_->ready.store(kChannelReadyMagic, std::memory_order_release);
    return true;
}

bool SharedMemoryQueue::attach(const std::string& name, int timeout_ms, std::string& error) {
    // shm_open() makes the segment visible before ftruncate() gives it a size,
    // so a consumer that wins the race sees a zero-length file. Keep retrying
    // until the producer has sized it rather than treating that as a mismatch.
    const int interval_ms = 10;
    bool sized = false;
    for (int waited = 0; waited <= timeout_ms && !sized; waited += interval_ms) {
        fd_ = ::shm_open(name.c_str(), O_RDWR, 0600);
        if (fd_ >= 0) {
            struct stat st {};
            if (::fstat(fd_, &st) == 0 &&
                static_cast<std::size_t>(st.st_size) == sizeof(ShmChannel)) {
                sized = true;
                break;
            }
            ::close(fd_);
            fd_ = -1;
        }
        sleep_ms(interval_ms);
    }
    if (!sized) {
        error = "timed out waiting for the producer to create the shared segment";
        close();
        return false;
    }

    region_ = ::mmap(nullptr, sizeof(ShmChannel), PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (region_ == MAP_FAILED) {
        region_ = nullptr;
        error = errno_message("mmap");
        close();
        return false;
    }

    name_ = name;
    owner_ = false;
    auto* candidate = static_cast<ShmChannel*>(region_);
    for (int waited = 0; waited <= timeout_ms; waited += interval_ms) {
        if (candidate->ready.load(std::memory_order_acquire) == kChannelReadyMagic) {
            channel_ = candidate;
            return true;
        }
        sleep_ms(interval_ms);
    }

    error = "timed out waiting for the producer to initialize the channel";
    close();
    return false;
}

void SharedMemoryQueue::close() {
    // shm_unlink removes by name, not by identity, so check the mapping still
    // belongs to this process before destroying it. Otherwise a producer exiting
    // can delete a segment that a later producer created under the same name.
    const bool mine =
        owner_ && channel_ != nullptr &&
        channel_->creator_pid.load(std::memory_order_relaxed) ==
            static_cast<std::uint32_t>(::getpid());

    if (region_ != nullptr) {
        ::munmap(region_, sizeof(ShmChannel));
        region_ = nullptr;
    }
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    if (mine && !name_.empty()) {
        ::shm_unlink(name_.c_str());
    }
    channel_ = nullptr;
}

}  // namespace llte
