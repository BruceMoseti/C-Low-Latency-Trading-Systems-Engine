#include "llte/multicast_receiver.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace llte {
namespace {

std::string errno_message(const char* what) {
    return std::string(what) + ": " + std::strerror(errno);
}

}  // namespace

static_assert(sizeof(ip_mreq) <= 8, "membership_ storage must fit ip_mreq");

MulticastReceiver::~MulticastReceiver() { close(); }

bool MulticastReceiver::open(const std::string& group, std::uint16_t port,
                             const std::string& interface_ip, std::string& error) {
    fd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd_ < 0) {
        error = errno_message("socket");
        return false;
    }

    const int reuse = 1;
    if (::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        error = errno_message("setsockopt(SO_REUSEADDR)");
        close();
        return false;
    }

    // A larger kernel buffer is the cheapest defence against drops when the
    // handler is briefly descheduled.
    const int receive_buffer = 8 * 1024 * 1024;
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &receive_buffer, sizeof(receive_buffer));

    sockaddr_in bind_address{};
    bind_address.sin_family = AF_INET;
    bind_address.sin_port = htons(port);
    bind_address.sin_addr.s_addr = htonl(INADDR_ANY);
    if (::bind(fd_, reinterpret_cast<const sockaddr*>(&bind_address), sizeof(bind_address)) < 0) {
        error = errno_message("bind");
        close();
        return false;
    }

    auto* membership = reinterpret_cast<ip_mreq*>(membership_);
    std::memset(membership, 0, sizeof(ip_mreq));
    if (::inet_pton(AF_INET, group.c_str(), &membership->imr_multiaddr) != 1) {
        error = "invalid multicast group: " + group;
        close();
        return false;
    }
    if (::inet_pton(AF_INET, interface_ip.c_str(), &membership->imr_interface) != 1) {
        error = "invalid interface address: " + interface_ip;
        close();
        return false;
    }
    if (::setsockopt(fd_, IPPROTO_IP, IP_ADD_MEMBERSHIP, membership, sizeof(ip_mreq)) < 0) {
        error = errno_message("setsockopt(IP_ADD_MEMBERSHIP)");
        close();
        return false;
    }
    joined_ = true;
    return true;
}

bool MulticastReceiver::set_receive_timeout(int milliseconds, std::string& error) {
    timeval timeout{};
    timeout.tv_sec = milliseconds / 1000;
    timeout.tv_usec = (milliseconds % 1000) * 1000;
    if (::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
        error = errno_message("setsockopt(SO_RCVTIMEO)");
        return false;
    }
    return true;
}

long MulticastReceiver::receive(void* buffer, std::size_t length) {
    const ssize_t received = ::recv(fd_, buffer, length, 0);
    if (received < 0) {
        return (errno == EAGAIN || errno == EWOULDBLOCK) ? 0 : -1;
    }
    return static_cast<long>(received);
}

void MulticastReceiver::close() {
    if (fd_ >= 0) {
        if (joined_) {
            ::setsockopt(fd_, IPPROTO_IP, IP_DROP_MEMBERSHIP, membership_, sizeof(ip_mreq));
            joined_ = false;
        }
        ::close(fd_);
        fd_ = -1;
    }
}

}  // namespace llte
