#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "llte/market_message.hpp"

namespace llte {

// Bounded history of published messages, kept so the exchange can answer
// retransmission requests. Guarded by a mutex on purpose: it is touched by the
// publisher thread and the recovery thread, and it is not on the measured
// consumer hot path.
class HistoryStore {
public:
    explicit HistoryStore(std::size_t capacity);

    void append(const MarketMessage& message);

    // Copies the retained subset of [from, to] into `out`. Returns how many were
    // written; a short result means the range has already aged out of the ring.
    std::uint32_t fetch(std::uint64_t from, std::uint64_t to, MarketMessage* out,
                        std::uint32_t max_messages) const;

private:
    mutable std::mutex mutex_;
    std::vector<MarketMessage> ring_;
    std::uint64_t oldest_sequence_ = 0;
    std::uint64_t newest_sequence_ = 0;
    bool empty_ = true;
};

// Serves retransmission requests over TCP, deliberately separate from the
// multicast fast path so a retransmit never stalls live delivery.
class RecoveryServer {
public:
    explicit RecoveryServer(HistoryStore& history) : history_(history) {}
    ~RecoveryServer();

    RecoveryServer(const RecoveryServer&) = delete;
    RecoveryServer& operator=(const RecoveryServer&) = delete;

    bool start(std::uint16_t port, std::string& error);
    void stop();

    std::uint64_t served_requests() const {
        return served_requests_.load(std::memory_order_relaxed);
    }
    std::uint64_t served_messages() const {
        return served_messages_.load(std::memory_order_relaxed);
    }

private:
    void run();
    void serve_connection(int client_fd);

    HistoryStore& history_;
    int listen_fd_ = -1;
    std::thread thread_;
    std::atomic<bool> running_{false};
    std::atomic<std::uint64_t> served_requests_{0};
    std::atomic<std::uint64_t> served_messages_{0};
};

}  // namespace llte
