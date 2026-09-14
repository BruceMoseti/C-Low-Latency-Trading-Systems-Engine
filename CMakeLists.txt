cmake_minimum_required(VERSION 3.20)
project(llte LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_CXX_FLAGS_RELEASE "-O2 -g -DNDEBUG")

# Sanitizers change timing enough to make latency numbers meaningless, so they
# are opt-in and only used by the race and correctness runs.
set(LLTE_SANITIZER "off" CACHE STRING "Sanitizer to build with: off, thread, address")
set_property(CACHE LLTE_SANITIZER PROPERTY STRINGS off thread address)

add_library(llte_flags INTERFACE)
target_compile_options(llte_flags INTERFACE -Wall -Wextra -Wpedantic)

if(LLTE_SANITIZER STREQUAL "thread")
  target_compile_options(llte_flags INTERFACE -fsanitize=thread -fno-omit-frame-pointer)
  target_link_options(llte_flags INTERFACE -fsanitize=thread)
elseif(LLTE_SANITIZER STREQUAL "address")
  target_compile_options(llte_flags INTERFACE -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(llte_flags INTERFACE -fsanitize=address,undefined)
endif()

find_package(Threads REQUIRED)

add_library(llte_core
  orderbook/order_book.cpp
  ipc/shared_memory_queue.cpp
  exchange/multicast_publisher.cpp
  exchange/recovery_server.cpp
  market_data/multicast_receiver.cpp
  market_data/recovery_client.cpp
  market_data/sequence_manager.cpp
)
target_include_directories(llte_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(llte_core PUBLIC llte_flags Threads::Threads rt)

function(llte_add_binary name)
  add_executable(${name} ${ARGN})
  target_link_libraries(${name} PRIVATE llte_core)
endfunction()

llte_add_binary(exchange_simulator exchange/exchange_simulator.cpp)
llte_add_binary(market_data_handler market_data/market_data_handler.cpp)
llte_add_binary(order_book_engine orderbook/order_book_engine.cpp)

llte_add_binary(queue_benchmark benchmarks/queue_benchmark.cpp)
llte_add_binary(book_benchmark benchmarks/book_benchmark.cpp)
llte_add_binary(end_to_end benchmarks/end_to_end.cpp)
llte_add_binary(timer_overhead benchmarks/timer_overhead.cpp)

llte_add_binary(spsc_race_demo tools/spsc_race_demo.cpp)

enable_testing()

foreach(test_name test_order_book test_queue test_gap_recovery)
  llte_add_binary(${test_name} tests/${test_name}.cpp)
  add_test(NAME ${test_name} COMMAND ${test_name})
endforeach()
