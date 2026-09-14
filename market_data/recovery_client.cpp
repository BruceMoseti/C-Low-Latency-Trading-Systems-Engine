#include "llte/recovery_client.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <ctime>

#include "llte/protocol.hpp"
#include "llte/socket_io.hpp"

namespace llte {
namespace {

std::string errno_message(const char* what) {
    return std::string(what) + ": " + std::strerror(errno);
}

void sleep_ms(int milliseconds) {
    timespec ts{milliseconds / 1000, static_cast<long>(milliseconds % 1000) * 1'000'000L};
    ::nanosleep(&ts, nullptr);
}

}  // namespace

RecoveryClient::~RecoveryClient() { close(); }

bool RecoveryClient::connect(const std::string& host, std::uint16_t port, int timeout_ms,
                             std::string& error) {
    host_ = host;
    port_ = port;
    timeout_ms_ = timeout_ms;

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &address.sin_addr) != 1) {
        error = "invalid recovery host: " + host;
        return false;
    }

    const int interval_ms = 20;
    for (int waited = 0; waited <= timeout_ms; waited += interval_ms) {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            error = errno_message("socket");
            return false;
        }
        if (::connect(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == 0) {
            const int nodelay = 1;
            ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
            timeval timeout{2, 0};
            ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
            ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
            fd_ = fd;
            return true;
        }
        ::close(fd);
        sleep_ms(interval_ms);
    }

    error = "could not reach the recovery server at " + host + ":" + std::to_string(port);
    return false;
}

bool RecoveryClient::reconnect(std::string& error) {
    close();
    if (host_.empty()) {
        error = "recovery client was never connected";
        return false;
    }
    reconnects_ += 1;
    return connect(host_, port_, timeout_ms_, error);
}

bool RecoveryClient::request(std::uint64_t from, std::uint64_t to,
                             std::vector<MarketMessage>& out, std::string& error) {
    if (try_request(from, to, out, error)) {
        return true;
    }
    // One retry on a fresh connection. The server closes an idle session, and a
    // gap can be the first traffic in a while, so the first failure is usually
    // just a stale socket rather than an unreachable exchange.
    std::string reconnect_error;
    if (!reconnect(reconnect_error)) {
        error += "; reconnect failed: " + reconnect_error;
        return false;
    }
    return try_request(from, to, out, error);
}

bool RecoveryClient::try_request(std::uint64_t from, std::uint64_t to,
                                 std::vector<MarketMessage>& out, std::string& error) {
    out.clear();
    if (fd_ < 0) {
        error = "recovery client is not connected";
        return false;
    }
    if (to < from) {
        error = "invalid recovery range";
        return false;
    }

    const RecoveryRequest request{kRecoveryMagic, 0, from, to};
    if (!write_exact(fd_, &request, sizeof(request))) {
        error = errno_message("send(recovery request)");
        close();
        return false;
    }

    RecoveryResponseHeader header{};
    if (!read_exact(fd_, &header, sizeof(header))) {
        error = errno_message("recv(recovery header)");
        close();
        return false;
    }
    if (header.magic != kRecoveryMagic || header.count > kMaxRecoveryBatch) {
        error = "malformed recovery response";
        close();
        return false;
    }
    if (header.count == 0) {
        return true;
    }

    out.resize(header.count);
    if (!read_exact(fd_, out.data(), header.count * sizeof(MarketMessage))) {
        error = errno_message("recv(recovery payload)");
        out.clear();
        close();
        return false;
    }
    return true;
}

void RecoveryClient::close() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

}  // namespace llte
