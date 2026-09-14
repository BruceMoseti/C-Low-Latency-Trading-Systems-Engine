#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace llte {

// Joins a multicast group and reads datagrams. UDP gives cheap fan-out but no
// delivery or ordering guarantee, which is what makes sequence checking and the
// TCP recovery path necessary downstream.
class MulticastReceiver {
public:
    MulticastReceiver() = default;
    ~MulticastReceiver();

    MulticastReceiver(const MulticastReceiver&) = delete;
    MulticastReceiver& operator=(const MulticastReceiver&) = delete;

    bool open(const std::string& group, std::uint16_t port, const std::string& interface_ip,
              std::string& error);
    bool set_receive_timeout(int milliseconds, std::string& error);
    // Bytes read, 0 on timeout, -1 on error.
    long receive(void* buffer, std::size_t length);
    void close();

private:
    int fd_ = -1;
    bool joined_ = false;
    alignas(8) unsigned char membership_[8]{};
};

}  // namespace llte
