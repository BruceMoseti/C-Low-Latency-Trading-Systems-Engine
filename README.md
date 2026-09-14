# Low-Latency Market Data Engine

[![CI](https://github.com/BruceMoseti/C-Low-Latency-Trading-Systems-Engine/actions/workflows/ci.yml/badge.svg)](https://github.com/BruceMoseti/C-Low-Latency-Trading-Systems-Engine/actions/workflows/ci.yml)

A market-data pipeline in C++20 for Linux, built the way an exchange feed actually
works: an exchange simulator publishes sequenced order-book events over UDP
multicast, a handler process detects missing sequence numbers and repairs them over
a separate TCP connection, and an order-book process consumes the repaired stream
through a shared-memory ring buffer and maintains a preallocated central limit order
book. Every hop is timestamped, so the pipeline is characterised by its tail — p99
and p99.9 — rather than by an average.

![Architecture of the pipeline: an exchange simulator publishing over UDP multicast to a market-data handler, which repairs gaps over TCP and forwards an ordered stream through a shared-memory SPSC ring buffer into an order-book engine](docs/figures/architecture.png)

Three processes, two transports, one shared-memory queue:

| Process | Responsibility |
| --- | --- |
| `exchange_simulator` | generates order flow, maintains the authoritative book, assigns sequence numbers, publishes the feed, serves retransmissions |
| `market_data_handler` | joins the group, validates and decodes packets, detects gaps, requests the missing range over TCP, emits one ordered stream |
| `order_book_engine` | applies add/cancel/modify/trade to a preallocated book and reports the latency of every stage |

## Quick start

Needs Linux, CMake 3.20+ and a C++20 compiler. The engine, the tests and the
benchmarks have no third-party dependencies; only the optional figure script needs
matplotlib.

```bash
./scripts/build.sh                    # configure and build into ./build
ctest --test-dir build                # 3 suites, ~33k assertions
./scripts/check_pipeline.sh --messages 200000 --rate 200000 --drop-rate 0.02
```

That last command starts all three processes, drops 2% of the datagrams on purpose,
and then checks that the book the engine rebuilt is identical to the one the exchange
actually had:

```
pipeline check: messages=200000 rate=200000 drop-rate=0.02 batch=1

  ok    engine best bid matches exchange               18749
  ok    engine best ask matches exchange               18751
  ok    engine live order count matches exchange       89114
  ok    handler published every message                200000
  ok    engine consumed every message                  200000
  ok    engine applied every message                   200000
  ok    every gap was recovered                        3909
  ok    no message abandoned                           0
  ok    no malformed packet                            0
  ok    no unknown order referenced                    0
  ok    no out-of-band price                           0

PASS  book reconstructed exactly; 3909 of 3909 gaps recovered over TCP
```

The gap count moves between runs, because on top of the injected drops the host loses
a variable number of datagrams of its own. What does not move is the last line.

## How it works

### The protocol

One fixed-layout 48-byte struct carries every event. It is memcpy'd onto the wire
with no serialization step, since both sides are built from the same source on the
same host, and `static_assert` pins the layout so a field reorder cannot silently
change the format.

```cpp
struct MarketMessage {
    std::uint64_t sequence_number;   // gap detection hangs off this
    std::uint64_t timestamp_ns;      // publish time, travels with retransmissions
    std::uint64_t order_id;
    Price         price;             // int64 ticks, never floating point
    std::uint32_t quantity;
    std::uint32_t symbol_id;
    MessageType   type;              // Add / Cancel / Modify / Trade
    Side          side;
    std::uint16_t reserved;
};
```

`$187.53` is stored as `18753`. Binary floating point cannot represent most decimal
prices exactly, so money in a book belongs in integers; it also makes price
comparison and the price-to-level index trivial.

A datagram carries an 8-byte header and up to 32 messages. Batching amortises the
`sendto` syscall; the receiver accepts any count within bounds and rejects anything
whose declared length disagrees with the bytes received.

### Two transports, on purpose

Market data goes out over UDP multicast because one publisher can serve many
subscribers without tracking any of them. The cost is that UDP guarantees nothing:
the receiver will see `1001, 1002, 1004` and has to notice.

Running the feed over TCP instead would make that impossible, but at a price that
matters more: a single lost segment stalls everything behind it while the kernel
retransmits, so one drop delays every later message. Splitting the paths means a gap
costs one targeted request for one range, on a connection of its own, while the live
feed keeps flowing.

The handler detects a gap whenever an arriving sequence number is ahead of the one it
expects, asks the exchange for exactly the missing range, and slots the replies into
a reorder buffer. Anything recovery cannot supply — already aged out of the
exchange's history ring — is marked skipped rather than left to stall the book
forever behind a hole that will never be filled.

![Message accounting under 2% injected packet loss: 7,991 datagrams dropped deliberately plus 1,942 dropped by the kernel gives 9,933 missing messages, all 9,933 recovered over TCP with none abandoned](docs/figures/recovery_accounting.png)

### One writer per queue

The handler and the engine are separate processes sharing a ring buffer through
`shm_open` and `mmap`, so this is genuinely inter-process, not two threads described
with borrowed vocabulary.

The ring is single-producer, single-consumer. That is a correctness constraint, not a
performance note, and it shapes the design: the natural way to add recovery is to let
the recovery path publish downstream alongside the receive path, which quietly puts
two writers on a queue built for one. Instead both sources feed the `SequenceManager`,
and the sequencer is the only thing that ever writes to the ring. The two paths
converge *before* the queue, never at it.

Two details carry the performance:

**The indexes live on separate cache lines.** The producer writes `write_idx_`, the
consumer writes `read_idx_`. Adjacent in memory they share a 64-byte line, and the
two cores then invalidate each other's copy on every single operation despite never
touching the same variable. `alignas(64)` separates them, and each sits beside that
side's private cached copy of the opposite index — so the common case does not read
the other core's line at all.

**Publication is release/acquire.** The producer fills the slot and then publishes the
index with a release store; the consumer acquires the index before reading the slot.
That pairing is what guarantees the consumer cannot observe a published index while
the payload writes that preceded it are still invisible.

### The order book

Price-time priority, with every container sized in the constructor:

- price levels in a flat array indexed by `price - min_price`, so finding a level is
  an offset rather than a tree descent
- orders from a fixed pool threaded on an intrusive free list, and an intrusive
  doubly-linked list per level, which makes cancel an O(1) unlink
- `order_id` to pool slot through an open-addressed table using backward-shift
  deletion, so it needs neither tombstones nor the rehash pause they eventually force
- best bid and ask tracked incrementally, scanning outward only when a touch level
  empties

Modify follows real venue semantics: shrinking in place keeps time priority, while a
price change or a size increase sends the order to the back of its new queue.

"Preallocated" is a claim about the steady state, and the benchmark below measures it
by replacing global `operator new` and counting, rather than asserting it in prose.

## Results

All numbers below come from the logs committed under
[`docs/measurements/`](docs/measurements), on an 8-core Linux container with `-O2` and
processes pinned. The figures are generated from those same logs by
`scripts/make_figures.py`, so a chart cannot drift from the run behind it.

### The book reconstructs exactly

400,000 messages at 200,000 msg/s. With the exchange dropping 2% of datagrams, the
accounting closes with nothing left over:

| | injected drops | kernel drops | missing | recovered | abandoned | applied |
| --- | --- | --- | --- | --- | --- | --- |
| 400,000 messages, 2% loss | 7,991 | 1,942 | 9,933 | 9,933 | 0 | 400,000 |

The kernel drops are real, not simulated: the exchange put 392,009 datagrams on the
wire and the handler received 390,067, so 1,942 were lost by the host under load.
Added to the 7,991 deliberate drops that is exactly the 9,933 gaps the handler
reported. Every one was retransmitted, and both processes finished holding the same
book — `best_bid=18749 best_ask=18751 live_orders=178442`.

### Latency through the pipeline

![Per-stage latency with no packet loss, showing receive to decode at 0.025 microseconds p50, ring transit at 0.366, book update at 0.165, and a total receive-to-book of 0.605 microseconds p50](docs/figures/stage_breakdown.png)

With no loss, the handler-to-book path costs 0.605 µs at the median and 0.826 µs at
p95. Across the wire, including the kernel's UDP send and receive, the median is
5.491 µs.

| stage | p50 | p90 | p95 | p99 | p99.9 |
| --- | --- | --- | --- | --- | --- |
| receive → decode | 0.025 µs | 0.025 µs | 0.025 µs | 0.031 µs | 0.034 µs |
| decode → sequence → enqueue | 0.054 µs | 0.056 µs | 0.057 µs | 0.071 µs | 0.108 µs |
| shared-memory ring transit | 0.366 µs | 0.394 µs | 0.404 µs | 2.942 µs | 27.50 µs |
| order book update | 0.165 µs | 0.305 µs | 0.374 µs | 0.487 µs | 0.626 µs |
| **total: receive → book** | **0.605 µs** | **0.748 µs** | **0.826 µs** | **3.247 µs** | **27.86 µs** |
| exchange → book, across the wire | 5.491 µs | 5.780 µs | 5.912 µs | 10.70 µs | 141.7 µs |

The ring transit tail is the honest weak point, and it is scheduler jitter rather
than queueing: the consumer spins on an empty ring, and when the host deschedules it
the next message waits. On a machine with isolated cores it would mostly disappear.
Nothing here is tuned — no core isolation, no busy-poll networking, no huge pages.

### What synchronous recovery costs

Splitting end-to-end latency by how each message arrived shows a trade-off worth
naming.

![Latency distributions on log-log axes. With no loss the handler path and wire path both stay flat to p99. Under 2% loss, the TCP recovery path is slower by design, but the multicast fast path also degrades](docs/figures/latency_distribution.png)

| loss rate | gaps | fast path p99 | fast path p99.9 | recovery p50 | recovery p90 |
| --- | --- | --- | --- | --- | --- |
| none | 0 | 10.70 µs | 141.7 µs | — | — |
| 0.1% | 410 | 9.271 µs | 1700 µs | 27.70 µs | 448.6 µs |
| 2% | 7,799 | 1404 µs | 3801 µs | 26.62 µs | 1277 µs |

Retransmitted messages being slower is expected — they cost a round trip. The
interesting result is the *fast path* degrading as loss rises. Recovery is
synchronous on the receive thread, so while the handler blocks on TCP, multicast
packets queue in the socket buffer and are then measured late. The gap rate decides
which percentile absorbs the stall: at 410 gaps in 400,000 messages it sits out at
p99.9, at 7,799 gaps it has reached p99.

Moving recovery onto its own thread would fix it, and would immediately reintroduce
the second writer the sequencer exists to prevent — the fix is a queue-per-source
feeding the sequencer, which is exactly the shape `tools/spsc_race_demo --mode fixed`
demonstrates. It is not implemented here, so the limitation stands as measured.

### The queue design, measured rather than assumed

Four ways to move a message between two threads, same payload, same capacity,
producer and consumer pinned to separate cores.

![Queue variant comparison. Sustained throughput rises from 3.11 to 7.66 to 12.99 million messages per second across mutex, false-shared and cache-aligned variants; the cached-index variant has a median of 18.46 but a range from 9.27 to 31.90](docs/figures/queue_variants.png)

| variant | throughput, median of 7 | observed range | paced p50 | paced p99 |
| --- | --- | --- | --- | --- |
| mutex + deque, same capacity | 3.11 M msg/s | 3.10 – 3.16 | 2.067 µs | 5.452 µs |
| lock-free, indexes share a cache line | 7.66 M msg/s | 7.63 – 7.67 | 0.364 µs | 0.386 µs |
| lock-free, indexes on separate lines | 12.99 M msg/s | 12.83 – 13.63 | 0.315 µs | 0.331 µs |
| + cached opposite index (shipped) | 18.46 M msg/s | 9.27 – 31.90 | 0.298 µs | 0.317 µs |

Dropping the mutex is worth 2.5x and splitting the cache line another 1.7x, both
tightly repeatable. Caching the opposite index is the interesting one: it improves
latency slightly and consistently, but its throughput is bimodal across a 3.4x range,
because the producer only reloads the consumer's index when it believes the ring is
full — so the benefit depends on whether the consumer is keeping up. Reporting its
median alone would overstate a result that is really "sometimes much faster, sometimes
level with the previous variant". Unpinned, the spread is wider still.

Note also what the latency column does *not* show: at a paced 1 M msg/s the ring stays
near-empty and all three lock-free variants land within a few tens of nanoseconds of
each other. The design differences show up in sustained throughput, not in handoff
latency at a modest offered load.

### Preallocation is a tail-latency decision

![Order book comparison. The preallocated book reaches 7.54 million operations per second with zero heap allocations and a 45 microsecond worst case, against 4.25 million, 2.86 million allocations and a 35 millisecond worst case for a node-based book](docs/figures/book_comparison.png)

2,000,000 mixed add/cancel/modify/trade operations against identical order flow:

| implementation | throughput | heap allocations | p50 | p99 | p99.9 | worst |
| --- | --- | --- | --- | --- | --- | --- |
| node-based (`std::map` + `std::list`) | 4.25 M ops/s | 2,864,360 | 138 ns | 463 ns | 1631 ns | 34.96 ms |
| preallocated (shipped) | 7.54 M ops/s | **0** | 89 ns | 201 ns | 274 ns | 45.4 µs |

Throughput improves by 1.8x, which is the least interesting column. The worst case
goes from 35 ms to 45 µs — a factor of 770 — and that is the whole argument for
preallocation. A book that is usually fast and occasionally stalls for tens of
milliseconds is a book that misses the move it was built to see.

### The software path on its own

`benchmarks/end_to_end` runs encode → decode → sequence → ring → book in one process,
with the kernel's network stack removed, isolating the cost the code itself is
responsible for. Paced at 1 M msg/s: **0.490 µs p50, 0.913 µs p99, 3.513 µs p99.9**
end to end. Unthrottled it sustains 3.39 M msg/s.

## The single-producer contract

`tools/spsc_race_demo` exists because the ring's single-writer rule is easy to state
and easy to violate, and because a rule with no test is a comment.

```bash
BUILD_DIR=build-tsan SANITIZER=thread ./scripts/build.sh
./build-tsan/spsc_race_demo --mode broken --producers 2 --messages 20000
./build-tsan/spsc_race_demo --mode fixed  --producers 2 --messages 20000
```

`--mode broken` puts two producers on one ring. ThreadSanitizer reports races on the
write index, on the producer-local cached read index, and on the slot storage itself —
a producer writing a slot while the consumer reads it:

```
SUMMARY: ThreadSanitizer: data race include/llte/spsc_queue.hpp:34 in try_push
SUMMARY: ThreadSanitizer: data race include/llte/spsc_queue.hpp:35 in try_push
SUMMARY: ThreadSanitizer: data race include/llte/spsc_queue.hpp:40 in try_push
SUMMARY: ThreadSanitizer: data race include/llte/spsc_queue.hpp:55 in try_pop
sent=40000 received=40000 corrupted=39 out_of_order=925 stalled_pushes=31111
result: STREAM DAMAGED
```

The visible damage varies with interleaving — payloads that fail their checksum,
messages delivered out of order, a ring wedged into a permanently-full state, or a
consumer re-reading slots that were never published. Because the failure is
nondeterministic, the demo works to a wall-clock budget rather than a spin count; a
spin budget was tried first and was not enough, since an occasional successful push
resets it and one run took over two minutes.

`--mode fixed` gives each producer its own ring and makes a single sequencer the only
writer downstream, which is the shape the real handler uses: `sent=40000
received=40000 lost=0 corrupted=0 out_of_order=0`, no sanitizer reports, finishing in
about 70 ms. CI asserts both halves — that the misuse is caught, and that the shipped
design is not.

## How the numbers were measured

Two mistakes in the first version of these benchmarks are worth naming, because both
produced confident numbers that meant something other than what they appeared to.

**Saturation is not latency.** With the producer unthrottled the ring simply stays
full, so the measured transit time is queue depth divided by throughput. The first
in-process run reported an 18 ms p50 that was entirely backlog — 65,536 slots at
3.57 M msg/s is 18.4 ms, which matched to three digits. Throughput is now measured
with the producer flat out, and latency separately under a paced load below
saturation, where the ring is near-empty.

**Thread startup leaks into the first samples.** The consumer allocates a ~64 MB book
before its first pop while the paced producer is already publishing, which showed up
as a 5.4 ms p99 with nothing to do with steady state — and got *worse* with fewer
messages, which is what gave it away. Both benchmarks now hold the producer behind a
start barrier until the consumer is in its loop.

Beyond that: percentiles come from full sorted sample sets rather than bucketed
histograms, sample buffers are reserved up front so recording never allocates, the
queue benchmark repeats every variant and reports a median with its range, and
`CLOCK_MONOTONIC` is system-wide on Linux so stamps taken in three different
processes are directly comparable.

## Verifying it yourself

```bash
# correctness: reconstruct the book through loss, batching and recovery
./scripts/check_pipeline.sh --messages 200000 --rate 200000 --drop-rate 0.02
./scripts/check_pipeline.sh --messages 200000 --rate 200000 --drop-rate 0.01 --batch 8

# clean under both sanitizers, including the full three-process run
BUILD_DIR=build-tsan SANITIZER=thread   ./scripts/build.sh && ctest --test-dir build-tsan
BUILD_DIR=build-asan SANITIZER=address  ./scripts/build.sh && ctest --test-dir build-asan
BUILD_DIR=build-asan ./scripts/check_pipeline.sh --messages 100000 --rate 100000 --drop-rate 0.02

# reproduce the measurements and regenerate every figure
./build/queue_benchmark --messages 2000000 --repeat 7 --producer-cpu 2 --consumer-cpu 4
./build/book_benchmark  --operations 2000000
./scripts/run_pipeline.sh --messages 400000 --rate 200000 --drop-rate 0.02 --pin --csv /tmp/l.csv
python3 scripts/analyze_latency.py /tmp/l.csv       # splits fast path from recovery path
python3 scripts/make_figures.py --recovery-csv /tmp/l.csv
```

CI runs the builds, the test suites under both sanitizers, the three-process pipeline
in three configurations, and both halves of the ownership demo on every push.

`scripts/build.sh` defaults `CXX` to `g++`. Clang works when pointed at a toolchain
whose development symlink is present:

```bash
CXX=clang++ CXXFLAGS=--gcc-install-dir=/usr/lib/gcc/x86_64-linux-gnu/13 ./scripts/build.sh
```

## Repository layout

```
include/llte/     protocol, message, SPSC ring, order book, instrumentation
exchange/         simulator, multicast publisher, TCP recovery server
market_data/      multicast receiver, sequence manager, recovery client, handler
orderbook/        order book and the consumer process
ipc/              POSIX shared-memory channel
benchmarks/       queue variants, book implementations, in-process pipeline
tests/            order book, ring buffer, gap recovery
tools/            SPSC single-producer ownership demo
scripts/          build, run, assert correctness, analyse latency, draw figures
docs/             committed measurement logs and the figures derived from them
```

## What this is not

A simulation of the infrastructure patterns behind electronic market data, built to
study networking, sequencing, recovery, lock-free IPC, book maintenance and latency
measurement under controlled conditions. It is not a production trading system and
does not connect to any venue.

Specifically:

- The wire format is the host's in-memory layout, with no byte-order conversion and
  no version field. That is deliberate for a single-host pipeline and it is why the
  offsets are pinned by `static_assert`, but it means the format is not portable
  across architectures and has no story for rolling upgrades. A real feed would
  specify endianness and carry a version.
- The simulator produces a market-data stream, not executions; it generates plausible
  order flow and never matches an aggressive order against the book.
- Recovery is synchronous on the receive thread, at the measured cost above.
- A gap triggers a request immediately, with no short grace period for a reordered
  packet to arrive on its own, so genuine reordering pays for a round trip it did not
  need.
- One symbol, one feed partition, no snapshot or refresh channel, so a handler that
  starts mid-stream begins from the first sequence number it sees rather than
  recovering the book state before it.
- The exchange's history ring is mutex-guarded. It is off the measured consumer path,
  but it is not lock-free and is not claimed to be.
- Latency figures come from a shared container, not tuned hardware: no isolated cores,
  no busy-poll NIC, no huge pages, and a spinning consumer competing with whatever
  else the host is running. Treat them as comparisons between designs measured under
  identical conditions rather than as absolute numbers for this class of system.
