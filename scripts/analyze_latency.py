#!/usr/bin/env python3
"""Summarize the per-message latency CSV written by order_book_engine --csv.

Reports each stage's distribution, and separately the messages that arrived over
the TCP recovery path, which are expected to be far slower than the fast path.
"""

import argparse
import csv
import sys

STAGES = [
    ("parse_ns", "udp recv -> parsed"),
    ("enqueue_ns", "parsed -> enqueue"),
    ("queue_ns", "ring transit"),
    ("book_ns", "book update"),
    ("handler_ns", "recv -> book (handler)"),
    ("wire_ns", "exchange -> book (wire)"),
]


def percentile(values, fraction):
    if not values:
        return 0
    index = min(int(round(fraction * (len(values) - 1))), len(values) - 1)
    return values[index]


def summarize(values):
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "p50": percentile(ordered, 0.50),
        "p90": percentile(ordered, 0.90),
        "p95": percentile(ordered, 0.95),
        "p99": percentile(ordered, 0.99),
        "p999": percentile(ordered, 0.999),
        "max": ordered[-1] if ordered else 0,
        "mean": sum(ordered) / len(ordered) if ordered else 0.0,
    }


def print_table(title, rows, markdown=False):
    print(f"\n{title}")
    headers = ["stage", "count", "p50", "p90", "p95", "p99", "p99.9", "max"]

    if markdown:
        print("| " + " | ".join(headers) + " |")
        print("|" + "|".join("---" for _ in headers) + "|")
        for label, stats in rows:
            print(
                f"| {label} | {stats['count']} | {stats['p50'] / 1000:.3f} | "
                f"{stats['p90'] / 1000:.3f} | {stats['p95'] / 1000:.3f} | "
                f"{stats['p99'] / 1000:.3f} | {stats['p999'] / 1000:.3f} | "
                f"{stats['max'] / 1000:.3f} |"
            )
        return

    print(
        f"{'stage':<26}{'count':>10}{'p50':>10}{'p90':>10}"
        f"{'p95':>10}{'p99':>10}{'p99.9':>10}{'max':>12}"
    )
    print("-" * 98)
    for label, stats in rows:
        print(
            f"{label:<26}{stats['count']:>10}"
            f"{stats['p50'] / 1000:>9.3f}u{stats['p90'] / 1000:>9.3f}u"
            f"{stats['p95'] / 1000:>9.3f}u{stats['p99'] / 1000:>9.3f}u"
            f"{stats['p999'] / 1000:>9.3f}u{stats['max'] / 1000:>11.3f}u"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", help="CSV produced by order_book_engine --csv")
    parser.add_argument("--markdown", action="store_true", help="emit markdown tables")
    args = parser.parse_args()

    columns = {name: [] for name, _ in STAGES}
    recovered_wire = []
    fast_path_wire = []

    try:
        with open(args.csv_path, newline="") as handle:
            for row in csv.DictReader(handle):
                for name, _ in STAGES:
                    columns[name].append(int(row[name]))
                wire = int(row["wire_ns"])
                if row["recovered"] == "1":
                    recovered_wire.append(wire)
                else:
                    fast_path_wire.append(wire)
    except FileNotFoundError:
        print(f"no such file: {args.csv_path}", file=sys.stderr)
        return 1

    total = len(fast_path_wire) + len(recovered_wire)
    if total == 0:
        print("no samples in file", file=sys.stderr)
        return 1

    print(f"samples: {total}  (fast path {len(fast_path_wire)}, recovered {len(recovered_wire)})")

    print_table(
        "Per-stage latency (microseconds)",
        [(label, summarize(columns[name])) for name, label in STAGES],
        args.markdown,
    )

    if recovered_wire:
        # Recovery is a deliberate tail: a round trip to the exchange costs far
        # more than the multicast fast path, which is exactly why it is separate.
        print_table(
            "Exchange -> book, split by path (microseconds)",
            [
                ("multicast fast path", summarize(fast_path_wire)),
                ("tcp recovery path", summarize(recovered_wire)),
            ],
            args.markdown,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
