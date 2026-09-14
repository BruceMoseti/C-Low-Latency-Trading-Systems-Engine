#pragma once

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>

namespace llte {

// TCP is a byte stream: a single read or write can return early, so both sides
// of the recovery protocol must loop until the whole frame has moved.

enum class FrameResult {
    Ok,
    // Nothing was read before the socket's receive timeout expired. Distinct from
    // an error on purpose: a connection that is merely idle between requests must
    // not be torn down, which is what happens if a timeout is treated as failure.
    IdleTimeout,
    Closed,
    Error,
};

inline FrameResult read_frame(int fd, void* buffer, std::size_t length) {
    auto* cursor = static_cast<unsigned char*>(buffer);
    std::size_t remaining = length;
    while (remaining > 0) {
        const ssize_t moved = ::recv(fd, cursor, remaining, 0);
        if (moved == 0) {
            return FrameResult::Closed;
        }
        if (moved < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Mid-frame the peer has stalled and the frame is unusable; with
                // nothing read yet the connection is simply quiet.
                return remaining == length ? FrameResult::IdleTimeout : FrameResult::Error;
            }
            return FrameResult::Error;
        }
        cursor += moved;
        remaining -= static_cast<std::size_t>(moved);
    }
    return FrameResult::Ok;
}

inline bool read_exact(int fd, void* buffer, std::size_t length) {
    return read_frame(fd, buffer, length) == FrameResult::Ok;
}

inline bool write_exact(int fd, const void* buffer, std::size_t length) {
    const auto* cursor = static_cast<const unsigned char*>(buffer);
    std::size_t remaining = length;
    while (remaining > 0) {
        const ssize_t moved = ::send(fd, cursor, remaining, MSG_NOSIGNAL);
        if (moved <= 0) {
            if (moved < 0 && (errno == EINTR)) {
                continue;
            }
            return false;
        }
        cursor += moved;
        remaining -= static_cast<std::size_t>(moved);
    }
    return true;
}

}  // namespace llte
