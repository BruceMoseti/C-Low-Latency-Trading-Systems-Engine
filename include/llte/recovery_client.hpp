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

private:
    int fd_ = -1;
};

}  // namespace llte
