#include "llte/recovery_server.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "llte/protocol.hpp"
#include "llte/socket_io.hpp"

namespace llte {
namespace {

std::string errno_message(const char* what) {
    return std::string(what) + ": " + std::strerror(errno);
}

}  // namespace

HistoryStore::HistoryStore(std::size_t capacity) : ring_(capacity) {}

void HistoryStore::append(const MarketMessage& message) {
    std::lock_guard<std::mutex> guard(mutex_);
    ring_[message.sequence_number % ring_.size()] = message;
    if (empty_) {
        oldest_sequence_ = message.sequence_number;
        empty_ = false;
    }
    newest_sequence_ = message.sequence_number;
    if (newest_sequence_ - oldest_sequence_ + 1 > ring_.size()) {
        oldest_sequence_ = newest_sequence_ - ring_.size() + 1;
    }
}

std::uint32_t HistoryStore::fetch(std::uint64_t from, std::uint64_t to, MarketMessage* out,
                                  std::uint32_t max_messages) const {
    std::lock_guard<std::mutex> guard(mutex_);
    if (empty_) {
        return 0;
    }
    std::uint32_t count = 0;
    for (std::uint64_t sequence = from; sequence <= to && count < max_messages; ++sequence) {
        if (sequence < oldest_sequence_ || sequence > newest_sequence_) {
            continue;
        }
        out[count++] = ring_[sequence % ring_.size()];
    }
    return count;
}

RecoveryServer::~RecoveryServer() { stop(); }

bool RecoveryServer::start(std::uint16_t port, std::string& error) {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        error = errno_message("socket");
        return false;
    }

    const int reuse = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    if (::bind(listen_fd_, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0) {
        error = errno_message("bind");
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }
    if (::listen(listen_fd_, 8) < 0) {
        error = errno_message("listen");
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    // accept() honours SO_RCVTIMEO, which gives the thread a chance to observe
    // the stop flag instead of blocking forever.
    timeval timeout{0, 100 * 1000};
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    running_.store(true, std::memory_order_release);
    thread_ = std::thread(&RecoveryServer::run, this);
    return true;
}

void RecoveryServer::stop() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        return;
    }
    if (thread_.joinable()) {
        thread_.join();
    }
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
}

void RecoveryServer::run() {
    std::vector<std::thread> sessions;
    while (running_.load(std::memory_order_acquire)) {
        const int client_fd = ::accept(listen_fd_, nullptr, nullptr);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                continue;
            }
            break;
        }
        // One thread per subscriber. Serving them one at a time would mean a
        // second subscriber is never answered while the first holds the socket
        // open, which is the normal case for a fan-out feed.
        if (sessions.size() >= kMaxSessions) {
            ::close(client_fd);
            rejected_sessions_.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        sessions.emplace_back([this, client_fd] {
            serve_connection(client_fd);
            ::close(client_fd);
        });
    }
    for (std::thread& session : sessions) {
        if (session.joinable()) {
            session.join();
        }
    }
}

void RecoveryServer::serve_connection(int client_fd) {
    const int nodelay = 1;
    ::setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
    timeval timeout{2, 0};
    ::setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    ::setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    std::vector<MarketMessage> scratch(kMaxRecoveryBatch);

    // The client keeps one connection open and pipelines requests over it. Gaps
    // are bursty, so a quiet stretch between them is expected and must not cost
    // the connection.
    while (running_.load(std::memory_order_acquire)) {
        RecoveryRequest request{};
        const FrameResult result = read_frame(client_fd, &request, sizeof(request));
        if (result == FrameResult::IdleTimeout) {
            continue;
        }
        if (result != FrameResult::Ok) {
            return;
        }
        if (request.magic != kRecoveryMagic || request.to_sequence < request.from_sequence) {
            return;
        }

        const std::uint64_t span = request.to_sequence - request.from_sequence + 1;
        const auto capped = static_cast<std::uint32_t>(
            span > kMaxRecoveryBatch ? kMaxRecoveryBatch : span);
        const std::uint32_t found = history_.fetch(request.from_sequence, request.to_sequence,
                                                   scratch.data(), capped);

        RecoveryResponseHeader header{kRecoveryMagic, found};
        if (!write_exact(client_fd, &header, sizeof(header))) {
            return;
        }
        if (found > 0 &&
            !write_exact(client_fd, scratch.data(), found * sizeof(MarketMessage))) {
            return;
        }

        served_requests_.fetch_add(1, std::memory_order_relaxed);
        served_messages_.fetch_add(found, std::memory_order_relaxed);
    }
}

}  // namespace llte
