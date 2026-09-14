#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace llte {

// UDP multicast sender. One publisher fans the same feed out to every subscriber
// without the exchange tracking who is listening.
class MulticastPublisher {
public:
    MulticastPublisher() = default;
    ~MulticastPublisher();

    MulticastPublisher(const MulticastPublisher&) = delete;
    MulticastPublisher& operator=(const MulticastPublisher&) = delete;

    bool open(const std::string& group, std::uint16_t port, const std::string& interface_ip,
              std::string& error);
    bool send(const void* data, std::size_t length, std::string& error);
    void close();

private:
    int fd_ = -1;
    // sockaddr_in, kept opaque so the header stays free of <netinet/in.h>.
    alignas(8) unsigned char destination_[16]{};
};

}  // namespace llte
