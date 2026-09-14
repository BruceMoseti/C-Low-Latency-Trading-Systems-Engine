#include "llte/multicast_publisher.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace llte {
namespace {

std::string errno_message(const char* what) {
    return std::string(what) + ": " + std::strerror(errno);
}

}  // namespace

static_assert(sizeof(sockaddr_in) <= 16, "destination_ storage must fit sockaddr_in");

MulticastPublisher::~MulticastPublisher() { close(); }

bool MulticastPublisher::open(const std::string& group, std::uint16_t port,
                              const std::string& interface_ip, std::string& error) {
    fd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd_ < 0) {
        error = errno_message("socket");
        return false;
    }

    in_addr interface_address{};
    if (::inet_pton(AF_INET, interface_ip.c_str(), &interface_address) != 1) {
        error = "invalid interface address: " + interface_ip;
        close();
        return false;
    }
    // Pin egress to a specific interface: the default route would otherwise decide
    // where the feed goes.
    if (::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_IF, &interface_address,
                     sizeof(interface_address)) < 0) {
        error = errno_message("setsockopt(IP_MULTICAST_IF)");
        close();
        return false;
    }

    const int loop = 1;
    if (::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop)) < 0) {
        error = errno_message("setsockopt(IP_MULTICAST_LOOP)");
        close();
        return false;
    }

    auto* destination = reinterpret_cast<sockaddr_in*>(destination_);
    std::memset(destination, 0, sizeof(sockaddr_in));
    destination->sin_family = AF_INET;
    destination->sin_port = htons(port);
    if (::inet_pton(AF_INET, group.c_str(), &destination->sin_addr) != 1) {
        error = "invalid multicast group: " + group;
        close();
        return false;
    }
    return true;
}

bool MulticastPublisher::send(const void* data, std::size_t length, std::string& error) {
    const auto* destination = reinterpret_cast<const sockaddr_in*>(destination_);
    const ssize_t sent = ::sendto(fd_, data, length, 0,
                                  reinterpret_cast<const sockaddr*>(destination),
                                  sizeof(sockaddr_in));
    if (sent < 0) {
        error = errno_message("sendto");
        return false;
    }
    return true;
}

void MulticastPublisher::close() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

}  // namespace llte
