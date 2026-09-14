# C-Low-Latency-Trading-Systems-Engine

A small exchange simulator and a low-latency C++ market-data pipeline for Linux.

The exchange publishes sequenced order-book updates over UDP multicast. A separate
market-data process receives and validates those messages, detects missing sequence
numbers, and requests the missing ones over a separate TCP recovery connection.
Valid messages pass through a shared-memory SPSC ring buffer into a third process
that maintains a preallocated central limit order book. Every stage is timestamped
so the pipeline can be measured at p50 through p99.9 rather than on averages alone.

This is a study of the infrastructure around electronic trading — networking,
sequencing, recovery, lock-free IPC, book maintenance and latency measurement. It is
not a production trading system and does not connect to a real venue.

## Architecture

```
                        exchange_simulator
                   ┌──────────────────────────┐
                   │ generates market events  │
                   │ seq 1001, 1002, 1003 ... │
                   │ keeps a history ring     │
                   └────┬────────────────┬────┘
                        │                │
                UDP multicast      TCP recovery
              (fast path, lossy)   (retransmission)
                        │                ▲
                        ▼                │
                   ┌─────────────────────┴────┐
                   │  market_data_handler     │
                   │  recv -> parse -> verify │
                   │  sequence gap detection  │
                   │  SequenceManager is the  │
                   │  single writer below     │
                   └────────────┬─────────────┘
                                │
                   shared-memory SPSC ring buffer
                       (POSIX shm, cache-aligned)
                                │
                                ▼
                   ┌──────────────────────────┐
                   │   order_book_engine      │
                   │   ADD / CANCEL /         │
                   │   MODIFY / TRADE         │
                   │   preallocated CLOB      │
                   │   per-stage latency      │
                   └──────────────────────────┘
```

Instrumentation points, carried with each message through the ring:

```
T0 udp recv → T1 parsed → T2 enqueue → T3 dequeue → T4 book updated
```

## Build and run

Requires Linux, CMake 3.20+ and a C++20 compiler.

```bash
./scripts/build.sh                 # configure + build into ./build
ctest --test-dir build             # unit and integration tests
./scripts/run_pipeline.sh --messages 400000 --rate 200000 --pin
```

`run_pipeline.sh` starts all three processes in dependency order and prints what
each one saw. Useful options:

| Option | Meaning |
| --- | --- |
| `--messages N` | messages the exchange publishes |
| `--rate N` | publish rate in messages/second, `0` for unthrottled |
| `--drop-rate F` | fraction of datagrams the exchange builds but never sends |
| `--batch N` | messages per datagram |
| `--pin` | pin each process to its own core |
| `--csv PATH` | write per-message stage timings for `scripts/analyze_latency.py` |

`scripts/build.sh` defaults `CXX` to `g++`. On this image clang selects the GCC 14
directory while only `libstdc++-13-dev` is installed, so it cannot find
`libstdc++.so`; clang works if pointed at the right toolchain:

```bash
CXX=clang++ CXXFLAGS=--gcc-install-dir=/usr/lib/gcc/x86_64-linux-gnu/13 ./scripts/build.sh
```

## Design decisions

**Integer prices.** Prices are `int64_t` ticks (cents), so `$187.53` is `18753`.
Financial values need exact representation, and integer comparison keeps the book's
hot path simple. There is no `double` anywhere in the message path.

**UDP multicast for the feed, TCP only for recovery.** Multicast gives one publisher
cheap fan-out to many subscribers. It offers no delivery or ordering guarantee, so
the receiver checks sequence numbers. Using TCP for everything would give reliable
ordered delivery, but a retransmission stalls the whole stream behind the missing
byte. Separating the two means a gap costs a targeted request instead of a
head-of-line block on the fast path.

**Single-producer ownership.** The ring buffer is SPSC, which is a correctness
constraint rather than a performance note. The multicast path and the recovery path
both feed the `SequenceManager`, and only the sequencer writes to the ring. The two
sources converge *before* the queue, never at it. See "The race" below.

**Cache-aligned atomics.** The producer writes `write_idx_` and the consumer writes
`read_idx_`. Adjacent in memory they would share a cache line, so the two cores would
invalidate each other's line on every operation despite never touching the same
variable. They are `alignas(64)` on separate lines, each next to that side's cached
copy of the opposite index.

**Acquire/release publication.** The producer fills the slot and then publishes the
index with a release store; the consumer acquires the index before reading the slot.
That ordering is what guarantees the consumer cannot observe a published index before
the payload writes that preceded it.

**Preallocated, not "never allocates".** Every container is sized in the `OrderBook`
constructor: a flat array of price levels indexed by `price - min_price`, a fixed
order pool with an intrusive free list, and an open-addressed `order_id` index that
uses backward-shift deletion so it needs neither tombstones nor a rehash pause. The
claim is that the steady-state path performs no heap allocation, and the benchmark
below counts allocations to check it.

**Real IPC.** The handler and the book run as separate processes and share the ring
through `shm_open` + `mmap`, so "inter-process" is accurate. Where components are
threads rather than processes, this README says inter-thread.

## Measured results

All numbers below are from this repository on an 8-core Linux container, `-O2`,
processes pinned. They are measurements from the runs in `scripts/`, not estimates.

### Pipeline, no injected loss

400,000 messages at 200,000 msg/s. Nothing lost, so nothing recovered.

| stage | p50 | p90 | p99 | p99.9 | max |
| --- | --- | --- | --- | --- | --- |
| udp recv → parsed | 0.025 µs | 0.036 µs | 0.044 µs | 0.055 µs | 2.58 µs |
| parsed → enqueue | 0.055 µs | 0.065 µs | 0.090 µs | 0.126 µs | 4.63 µs |
| ring transit | 0.314 µs | 0.340 µs | 0.453 µs | 3.70 µs | 142 µs |
| book update | 0.148 µs | 0.246 µs | 0.422 µs | 0.536 µs | 9.53 µs |
| **recv → book** | **0.549 µs** | **0.677 µs** | **0.871 µs** | **4.01 µs** | **143 µs** |
| exchange → book (wire) | 5.42 µs | 5.76 µs | 6.93 µs | 22.9 µs | 317 µs |

The engine's reconstructed book matched the exchange's authoritative book exactly:
`best_bid=18749 best_ask=18751 live_orders=177846` on both sides.

### Pipeline with packet loss and recovery

Same run with the exchange dropping 2% of datagrams:

```
exchange: packets sent=392009 dropped=7991 (messages dropped=7991)
handler:  packets=389053 gaps=7773 missing=10947 recovered=10947 unrecoverable=0
engine:   consumed 400000 events (10947 arrived via TCP recovery), applied=400000
engine:   best_bid=18749 best_ask=18751 live_orders=178442   (matches the exchange)
```

The accounting closes exactly: 7,991 datagrams were dropped deliberately and a
further 2,956 were lost by the kernel under load (392,009 sent − 389,053 received).
That is 10,947 missing messages, all 10,947 recovered over TCP, none abandoned. The
book still reconstructs exactly.

### The cost of synchronous recovery

Splitting end-to-end latency by how each message arrived shows a real tradeoff:

| loss rate | gaps | fast path p99 | fast path p99.9 | recovery path p50 | recovery path p90 |
| --- | --- | --- | --- | --- | --- |
| 0.1% | 411 | 7.96 µs | 2513 µs | 31.7 µs | 1124 µs |
| 2% | 7,773 | 2400 µs | 3849 µs | 26.8 µs | 1435 µs |

Recovery is a round trip, so recovered messages are naturally slower — that part is
expected. The interesting result is the *fast path* degrading at 2% loss. Recovery is
synchronous: when the handler detects a gap it blocks on TCP while multicast packets
pile up in the socket buffer, and those queued messages are then measured late. The
gap rate decides which percentile absorbs the stall — at 411 gaps in 400,000 messages
it lands on p99.9, at 7,773 gaps it reaches p99. Moving recovery off the receive
thread would fix this, at the cost of reintroducing the second producer that the
sequencer design exists to avoid.

### Queue variants

2,000,000 messages, producer and consumer pinned to separate cores. Throughput is
measured with the producer unthrottled; latency is measured separately at a paced
1 M msg/s, because at saturation the ring simply stays full and "latency" degenerates
into a measure of queue depth.

| variant | throughput | paced p50 | paced p99 |
| --- | --- | --- | --- |
| 1. mutex + deque (bounded to same depth) | 3.87 M msg/s | 2.398 µs | 5.71 µs |
| 2. lock-free, both indexes on one line | 7.34 M msg/s | 0.370 µs | 0.404 µs |
| 3. lock-free, cache-aligned indexes | 12.67 M msg/s | 0.327 µs | 2.26 µs |
| 4. + producer/consumer-cached indexes (shipped) | 22.13 M msg/s | 0.290 µs | 2.38 µs |

Throughput is where the design differences show: removing the mutex roughly doubles
it, separating the cache lines adds ~1.7x, and caching the opposite index adds
another ~1.7x by avoiding a cross-core read on every operation. At a paced 1 M msg/s
the ring is nearly empty and all three lock-free variants land within noise of each
other above p50 — the ordering at p99 there is not meaningful, and this table does not
claim otherwise.

Pinning mattered enough to be worth stating: unpinned, variant 4 measured anywhere
from 11.7 to 30.1 M msg/s depending on where the scheduler placed the two threads.

### Order book

2,000,000 mixed add/cancel/modify/trade operations. Allocations are counted by
overriding global `operator new`, measured after construction so only steady-state
work is counted.

| implementation | throughput | heap allocations | p50 | p99 | p99.9 | max |
| --- | --- | --- | --- | --- | --- | --- |
| node-based (`std::map` + `std::list`) | 4.20 M ops/s | 2,864,360 | 140 ns | 487 ns | 1658 ns | 34.97 ms |
| preallocated (shipped) | 7.29 M ops/s | **0** | 93 ns | 231 ns | 404 ns | 28.3 µs |

Zero allocations on the steady-state path, measured rather than asserted. The tail is
the real story: the node-based book's worst case is 35 ms against 28 µs, which is what
allocator and container growth pauses cost a latency-sensitive consumer.

### In-process pipeline

`benchmarks/end_to_end` runs encode → decode → sequence → ring → book in one process,
isolating the software cost from the kernel network stack. Paced at 1 M msg/s:
p50 0.454 µs, p99 0.893 µs, p99.9 2.144 µs end to end. Unthrottled it sustains
3.86 M msg/s.

## The race

`tools/spsc_race_demo` exists because the single-producer rule is easy to state and
easy to violate. The natural way to add recovery is to let the recovery path publish
downstream alongside the receive path — which quietly puts two producers on a queue
built for one.

```bash
BUILD_DIR=build-tsan SANITIZER=thread ./scripts/build.sh
./build-tsan/spsc_race_demo --mode broken --producers 2 --messages 20000
./build-tsan/spsc_race_demo --mode fixed  --producers 2 --messages 20000
```

`--mode broken` puts two producers on one `SpscQueue`. ThreadSanitizer reports data
races on the write index, on the producer-local cached read index, and on the slot
storage itself — a producer writing a slot while the consumer reads it:

```
SUMMARY: ThreadSanitizer: data race include/llte/spsc_queue.hpp:34 in try_push
SUMMARY: ThreadSanitizer: data race include/llte/spsc_queue.hpp:35 in try_push
SUMMARY: ThreadSanitizer: data race include/llte/spsc_queue.hpp:40 in try_push
SUMMARY: ThreadSanitizer: data race include/llte/spsc_queue.hpp:55 in try_pop
sent=40000 received=40000 corrupted=39 out_of_order=925 stalled_pushes=31111
result: STREAM DAMAGED
```

The two producers clobber each other's index update. The observable damage varies by
interleaving — torn payloads that fail their checksum, messages delivered out of
order, a ring wedged into a permanently-full state, and a consumer that re-reads
slots which were never published. Because the failure mode is nondeterministic, the
demo runs to a wall-clock budget (`--time-budget-ms`) rather than a fixed spin count;
across repeated runs it reports 8–12 distinct races and always ends STREAM DAMAGED.

`--mode fixed` gives each producer its own queue and makes one sequencer thread the
sole writer of the downstream queue — the same shape the real handler uses:
`sent=40000 received=40000 lost=0 corrupted=0 out_of_order=0`, no sanitizer reports,
and it finishes in ~70 ms instead of burning the budget. The shipped test suite also
runs clean under ThreadSanitizer.

## Measurement methodology

Two mistakes were caught while building these benchmarks, and both are worth naming
because they inflate results in opposite directions:

1. **Saturation is not latency.** With an unthrottled producer the ring sits full and
   the measured transit time is just queue depth divided by throughput — the first
   end-to-end run reported an 18 ms p50 that was entirely backlog. Latency is now
   measured under a paced load below saturation; throughput is measured separately.
2. **Thread startup leaks into the first samples.** The consumer allocates a ~64 MB
   book before its first pop while the paced producer is already publishing, which
   showed up as a 5.4 ms p99 that had nothing to do with steady state. Both benchmarks
   now hold the producer behind a start barrier until the consumer is in its loop; the
   same p99 then measures under 1 µs.

Percentiles come from full sorted sample sets, not reservoir sampling or bucketed
histograms. Sample buffers are reserved up front so recording never allocates.

## Repository layout

```
include/llte/     protocol, message, ring buffer, book, instrumentation headers
exchange/         simulator, multicast publisher, TCP recovery server
market_data/      multicast receiver, sequence manager, recovery client, handler
orderbook/        order book and the consumer process
ipc/              POSIX shared-memory channel
benchmarks/       queue variants, book implementations, in-process pipeline
tests/            order book, ring buffer, gap recovery
tools/            SPSC ownership race demo
scripts/          build, run the pipeline, analyze latency CSVs
```

## Limitations

- The simulator generates plausible order flow but does not match orders; it produces
  a market-data stream, not executions against incoming aggressive orders.
- Recovery is synchronous on the receive thread. See the measured cost above.
- Single symbol, single feed partition, no snapshot/refresh channel.
- The exchange's history ring uses a mutex. It is off the measured consumer path, but
  it is not lock-free and is not claimed to be.
- Latency figures come from a shared container, not tuned hardware: no isolated cores,
  no busy-poll NIC, no huge pages. Treat them as relative comparisons between designs
  rather than absolute numbers for this class of system.
