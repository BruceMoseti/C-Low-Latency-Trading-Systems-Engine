#pragma once

#include <cstdio>
#include <string>
#include <type_traits>

// Deliberately tiny: the sandbox has no package access, and the assertions this
// project needs are simple enough that a framework would be overhead.
namespace llte::test {

inline int g_checks = 0;
inline int g_failures = 0;

inline void record(bool passed, const char* expression, const char* file, int line,
                   const std::string& detail) {
    g_checks += 1;
    if (!passed) {
        g_failures += 1;
        std::printf("  FAIL %s:%d  %s%s\n", file, line, expression, detail.c_str());
    }
}

inline int report(const char* suite) {
    if (g_failures == 0) {
        std::printf("PASS %s (%d checks)\n", suite, g_checks);
        return 0;
    }
    std::printf("FAIL %s (%d/%d checks failed)\n", suite, g_failures, g_checks);
    return 1;
}

template <typename T>
std::string describe(const T& value) {
    return std::to_string(value);
}

}  // namespace llte::test

#define CHECK(expr) ::llte::test::record((expr), #expr, __FILE__, __LINE__, "")

#define CHECK_EQ(actual, expected)                                                          \
    do {                                                                                    \
        using ValueType = std::decay_t<decltype(actual)>;                                   \
        const ValueType actual_value = (actual);                                            \
        const ValueType expected_value = static_cast<ValueType>(expected);                  \
        ::llte::test::record(actual_value == expected_value, #actual " == " #expected,      \
                             __FILE__, __LINE__,                                            \
                             "  (got " + ::llte::test::describe(actual_value) + ", want " + \
                                 ::llte::test::describe(expected_value) + ")");             \
    } while (0)
