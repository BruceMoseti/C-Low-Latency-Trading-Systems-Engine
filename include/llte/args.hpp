#pragma once

#include <cstdlib>
#include <string>
#include <unordered_map>

namespace llte {

// Minimal `--key value` / `--flag` parser shared by the three binaries.
class Args {
public:
    Args(int argc, char** argv) {
        for (int i = 1; i < argc; ++i) {
            std::string token(argv[i]);
            if (token.rfind("--", 0) != 0) {
                continue;
            }
            token.erase(0, 2);
            const std::size_t equals = token.find('=');
            if (equals != std::string::npos) {
                values_[token.substr(0, equals)] = token.substr(equals + 1);
            } else if (i + 1 < argc && argv[i + 1][0] != '-') {
                values_[token] = argv[++i];
            } else {
                values_[token] = "1";
            }
        }
    }

    bool has(const std::string& key) const { return values_.count(key) > 0; }

    std::string str(const std::string& key, const std::string& fallback) const {
        const auto it = values_.find(key);
        return it == values_.end() ? fallback : it->second;
    }

    long long integer(const std::string& key, long long fallback) const {
        const auto it = values_.find(key);
        return it == values_.end() ? fallback : std::strtoll(it->second.c_str(), nullptr, 10);
    }

    double real(const std::string& key, double fallback) const {
        const auto it = values_.find(key);
        return it == values_.end() ? fallback : std::strtod(it->second.c_str(), nullptr);
    }

private:
    std::unordered_map<std::string, std::string> values_;
};

}  // namespace llte
