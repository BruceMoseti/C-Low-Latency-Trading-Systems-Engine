#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "llte/market_message.hpp"

namespace llte {

// Requests specific sequence ranges from the exchange over TCP after the
// sequence checker spots a gap in the multicast feed.
class RecoveryClient {
public:
    RecoveryClient() = default;
    ~RecoveryClient();

    RecoveryClient(const RecoveryClient&) = delete;
    RecoveryClient& operator=(const RecoveryClient&) = delete;

    bool connect(const std::string& host, std::uint16_t port, int timeout_ms,
                 std::string& error);
    bool request(std::uint64_t from, std::uint64_t to, std::vector<MarketMessage>& out,
                 std::string& error);
    void close();

    bool connected() const { return fd_ >= 0; }
    std::uint64_t reconnects() const { return reconnects_; }

private:
    // Retries a failed request once on a fresh connection. Without this, a single
    // broken socket abandons every gap for the rest of the session, silently
    // turning recovery off in the process that exists to perform it.
    bool try_request(std::uint64_t from, std::uint64_t to, std::vector<MarketMessage>& out,
                     std::string& error);
    bool reconnect(std::string& error);

    int fd_ = -1;
    std::string host_;
    std::uint16_t port_ = 0;
    int timeout_ms_ = 0;
    std::uint64_t reconnects_ = 0;
};

}  // namespace llte
