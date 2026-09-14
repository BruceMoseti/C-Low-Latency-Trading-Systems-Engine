#!/usr/bin/env python3
"""Regenerate the figures in docs/figures from the measurement logs.

Every number plotted is parsed out of the raw program output committed under
docs/measurements, so the figures cannot drift from the runs they describe. The
latency curves need the per-message CSV that order_book_engine --csv writes; that
file is large and is not committed, so its quantiles are cached alongside the logs
as latency_percentiles.json and the curves are drawn from the cache when the CSV
is absent.

Usage:
    scripts/make_figures.py                      # use cached quantiles
    scripts/make_figures.py --clean-csv a.csv --recovery-csv b.csv

Requires matplotlib. The C++ project itself has no dependencies.
"""

import argparse
import csv
import json
import os
import re
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MEASUREMENTS = os.path.join(REPO, "docs", "measurements")
FIGURES = os.path.join(REPO, "docs", "figures")

# Explicit light background everywhere: a transparent figure with dark text is
# unreadable against GitHub's dark theme.
BACKGROUND = "#ffffff"
INK = "#1b1f24"
MUTED = "#6a737d"
GRID = "#e2e6ea"

ACCENT = "#1f6feb"
ACCENT_DARK = "#0a3069"
WARN = "#bc4c00"
GOOD = "#1a7f37"
NEUTRAL = "#9aa5b1"

STAGE_ROW = re.compile(
    r"^(?P<label>.*?)\s{2,}(?P<count>\d+)"
    r"\s+(?P<p50>[\d.]+)u\s+(?P<p90>[\d.]+)u\s+(?P<p95>[\d.]+)u"
    r"\s+(?P<p99>[\d.]+)u\s+(?P<p999>[\d.]+)u\s+(?P<max>[\d.]+)u\s*$"
)
THROUGHPUT_ROW = re.compile(
    r"^(?P<label>\d\.\s+.*?)\s{2,}(?P<median>[\d.]+)"
    r"\s+(?P<low>[\d.]+)\s*-\s*(?P<high>[\d.]+)\s*$"
)
PACED_ROW = re.compile(
    r"^(?P<label>\d\.\s+.*?)\s{2,}(?P<p50>[\d.]+)\s+(?P<p99>[\d.]+)\s+(?P<p999>[\d.]+)\s*$"
)
BOOK_ROW = re.compile(
    r"^(?P<label>[a-z].*?)\s{2,}(?P<ops>[\d.]+)\s+(?P<allocs>\d+)"
    r"\s+(?P<p50>\d+)\s+(?P<p99>\d+)\s+(?P<p999>\d+)\s+(?P<max>\d+)\s*$"
)


def read_lines(name):
    path = os.path.join(MEASUREMENTS, name)
    with open(path) as handle:
        return handle.read().splitlines()


def parse_stages(name):
    """Per-stage latency table, in microseconds, keyed by stage label."""
    stages = {}
    for line in read_lines(name):
        found = STAGE_ROW.match(line)
        if found:
            stages[found.group("label").strip()] = {
                key: float(found.group(key))
                for key in ("p50", "p90", "p95", "p99", "p999", "max")
            }
    return stages


def parse_queue(name):
    lines = read_lines(name)
    throughput, paced, section = [], [], None
    for line in lines:
        if "saturation" in line:
            section = "throughput"
            continue
        if "paced at" in line:
            section = "paced"
            continue
        if section == "throughput":
            found = THROUGHPUT_ROW.match(line)
            if found:
                throughput.append(
                    (
                        found.group("label").strip(),
                        float(found.group("median")),
                        float(found.group("low")),
                        float(found.group("high")),
                    )
                )
        elif section == "paced":
            found = PACED_ROW.match(line)
            if found:
                paced.append(
                    (
                        found.group("label").strip(),
                        float(found.group("p50")),
                        float(found.group("p99")),
                        float(found.group("p999")),
                    )
                )
    repeats = 0
    found = re.search(r"median of (\d+)", "\n".join(lines))
    if found:
        repeats = int(found.group(1))
    return throughput, paced, repeats


def parse_book(name):
    rows = []
    for line in read_lines(name):
        found = BOOK_ROW.match(line)
        if found:
            rows.append(
                {
                    "label": found.group("label").strip(),
                    "ops": float(found.group("ops")),
                    "allocs": int(found.group("allocs")),
                    "p50": int(found.group("p50")),
                    "p99": int(found.group("p99")),
                    "p999": int(found.group("p999")),
                    "max": int(found.group("max")),
                }
            )
    return rows


def parse_counters(name):
    """Message accounting from a pipeline log."""
    text = "\n".join(read_lines(name))
    counters = {}
    for key, pattern in [
        ("sent", r"packets sent=(\d+)"),
        ("dropped", r"dropped=(\d+) \(messages"),
        ("received", r"handler: packets=(\d+)"),
        ("gaps", r"gaps=(\d+)"),
        ("missing", r"missing=(\d+)"),
        ("recovered", r"recovered=(\d+)"),
        ("unrecoverable", r"unrecoverable=(\d+)"),
        ("consumed", r"consumed (\d+) events"),
        ("applied", r"applied=(\d+)"),
    ]:
        found = re.search(pattern, text)
        if found:
            counters[key] = int(found.group(1))
    return counters


QUANTILES = (
    [0.0, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.92, 0.94, 0.95]
    + [0.96, 0.97, 0.975, 0.98, 0.985, 0.99, 0.992, 0.994, 0.995, 0.996, 0.997, 0.998]
    + [0.9985, 0.999, 0.9995, 0.9999, 1.0]
)


def quantiles_of(values):
    ordered = sorted(values)
    if not ordered:
        return []
    out = []
    for q in QUANTILES:
        index = min(int(round(q * (len(ordered) - 1))), len(ordered) - 1)
        out.append([q, ordered[index] / 1000.0])  # nanoseconds -> microseconds
    return out


def curves_from_csv(clean_csv, recovery_csv):
    """Quantile curves for the series the figures need."""
    curves = {}

    if clean_csv:
        with open(clean_csv, newline="") as handle:
            rows = list(csv.DictReader(handle))
        curves["clean_handler"] = quantiles_of([int(r["handler_ns"]) for r in rows])
        curves["clean_wire"] = quantiles_of([int(r["wire_ns"]) for r in rows])

    if recovery_csv:
        with open(recovery_csv, newline="") as handle:
            rows = list(csv.DictReader(handle))
        fast = [int(r["wire_ns"]) for r in rows if r["recovered"] == "0"]
        slow = [int(r["wire_ns"]) for r in rows if r["recovered"] == "1"]
        curves["recovery_fast_path"] = quantiles_of(fast)
        curves["recovery_tcp_path"] = quantiles_of(slow)
        curves["recovery_counts"] = {"fast": len(fast), "recovered": len(slow)}

    return curves


def style(ax, title=None, xlabel=None, ylabel=None):
    ax.set_facecolor(BACKGROUND)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=9, length=3)
    if title:
        ax.set_title(title, color=INK, fontsize=12, fontweight="bold", pad=12, loc="left")
    if xlabel:
        ax.set_xlabel(xlabel, color=MUTED, fontsize=9.5)
    if ylabel:
        ax.set_ylabel(ylabel, color=MUTED, fontsize=9.5)


def save(fig, name):
    # PNG only, and deliberately: these figures carry hand-placed labels, and a
    # renderer substituting a different font would shift them into each other.
    os.makedirs(FIGURES, exist_ok=True)
    path = os.path.join(FIGURES, f"{name}.png")
    fig.savefig(path, facecolor=BACKGROUND, bbox_inches="tight", dpi=160)
    plt.close(fig)
    print(f"docs/figures/{name}.png")


def figure_architecture():
    fig, ax = plt.subplots(figsize=(13.0, 7.0))
    ax.set_xlim(0, 118)
    ax.set_ylim(-5, 104)
    ax.axis("off")
    fig.patch.set_facecolor(BACKGROUND)

    def box(x, y, w, h, title, lines):
        ax.add_patch(
            FancyBboxPatch(
                (x, y), w, h,
                boxstyle="round,pad=0.5,rounding_size=1.4",
                linewidth=1.6, edgecolor=ACCENT, facecolor="#f2f7ff",
            )
        )
        ax.text(x + w / 2, y + h - 4.6, title, ha="center", va="center",
                fontsize=11, fontweight="bold", color=ACCENT_DARK, family="monospace")
        for i, line in enumerate(lines):
            ax.text(x + w / 2, y + h - 10.6 - i * 4.8, line, ha="center", va="center",
                    fontsize=8.7, color=INK)

    # Three processes in one column, with the arrow lanes kept clear of the boxes.
    left, width = 6, 46
    box(left, 78, width, 22, "exchange_simulator",
        ["maintains the authoritative order book",
         "stamps every event: seq 1001, 1002, ...",
         "retains history for retransmission"])
    box(left, 42, width, 22, "market_data_handler",
        ["recvfrom \u2192 validate \u2192 decode",
         "detects missing sequence numbers",
         "SequenceManager is the ring's only writer"])
    box(left, 4, width, 22, "order_book_engine",
        ["ADD / CANCEL / MODIFY / TRADE",
         "preallocated central limit order book",
         "no heap allocation on the hot path"])

    def down_arrow(x, top, bottom, color, label, sublabel):
        ax.add_patch(FancyArrowPatch(
            (x, top), (x, bottom), arrowstyle="-|>", mutation_scale=18,
            linewidth=2.0, color=color,
        ))
        mid = (top + bottom) / 2
        ax.text(x + 2.6, mid + 1.6, label, ha="left", va="center",
                fontsize=9.2, color=color, fontweight="bold")
        ax.text(x + 2.6, mid - 2.8, sublabel, ha="left", va="center",
                fontsize=8.2, color=MUTED)

    down_arrow(15, 77.2, 65.0, ACCENT, "UDP multicast",
               "cheap fan-out; lossy and unordered")
    down_arrow(15, 41.2, 27.0, GOOD, "shared-memory SPSC ring",
               "cache-aligned indexes, release/acquire")

    # Recovery runs back up the right-hand side as an explicit elbow, so it reads
    # as a separate path rather than a branch of the feed.
    elbow_x = 58
    ax.plot([left + width, elbow_x], [53, 53], color=WARN, linewidth=2.0,
            linestyle=(0, (6, 3)))
    ax.plot([elbow_x, elbow_x], [53, 89], color=WARN, linewidth=2.0,
            linestyle=(0, (6, 3)))
    ax.add_patch(FancyArrowPatch(
        (elbow_x, 89), (left + width + 0.5, 89), arrowstyle="-|>", mutation_scale=18,
        linewidth=2.0, color=WARN, linestyle=(0, (6, 3)),
    ))
    ax.text(elbow_x + 2.0, 74.5, "TCP recovery", ha="left", va="center",
            fontsize=9.2, color=WARN, fontweight="bold")
    ax.text(elbow_x + 2.0, 70.2, "request the exact", ha="left", va="center",
            fontsize=8.2, color=MUTED)
    ax.text(elbow_x + 2.0, 66.6, "missing range", ha="left", va="center",
            fontsize=8.2, color=MUTED)

    panel = 78
    ax.text(panel, 99.5, "Why the feed and the recovery path are separate",
            fontsize=10.2, fontweight="bold", color=INK)
    for i, line in enumerate([
        "Multicast lets one publisher serve many",
        "subscribers, but guarantees no delivery and",
        "no ordering. Reliable ordered delivery would",
        "stall every later message behind a lost one,",
        "so a gap instead costs a single targeted",
        "request on a connection of its own.",
    ]):
        ax.text(panel, 94.0 - i * 4.3, line, fontsize=8.6, color=MUTED)

    ax.text(panel, 62.0, "Measured at every hop", fontsize=10.2,
            fontweight="bold", color=INK)
    for i, line in enumerate([
        "T0   recvfrom returns",
        "T1   header checked, message decoded",
        "T2   handed to the ring",
        "T3   taken off the ring",
        "T4   book updated",
    ]):
        ax.text(panel, 56.4 - i * 4.4, line, fontsize=8.6, color=INK, family="monospace")
    for i, line in enumerate([
        "The stamps travel with the payload through",
        "shared memory, so a retransmitted message",
        "is timed from when it actually arrived.",
    ]):
        ax.text(panel, 30.0 - i * 4.0, line, fontsize=8.4, color=MUTED)

    ax.text(panel, 14.0, "One writer per queue", fontsize=10.2,
            fontweight="bold", color=INK)
    for i, line in enumerate([
        "Both sources meet at the sequencer, never at",
        "the ring. That keeps the single-producer",
        "contract the ring's correctness depends on.",
    ]):
        ax.text(panel, 8.4 - i * 4.0, line, fontsize=8.4, color=MUTED)

    save(fig, "architecture")


def figure_latency_distribution(curves):
    fig, axes = plt.subplots(1, 2, figsize=(11.4, 4.3))
    fig.patch.set_facecolor(BACKGROUND)

    def plot(ax, series, title, xlabel):
        for label, key, color, width in series:
            curve = curves.get(key)
            if not curve:
                continue
            qs = [1.0 - q if q < 1.0 else 1e-5 for q, _ in curve]
            xs = [v for _, v in curve]
            ax.plot(xs, qs, label=label, color=color, linewidth=width)
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.invert_yaxis()
        ax.grid(True, which="major", color=GRID, linewidth=0.8)
        ax.grid(True, which="minor", color=GRID, linewidth=0.4, alpha=0.6)
        style(ax, title, xlabel, None)
        ax.set_yticks([1, 0.5, 0.1, 0.01, 1e-3, 1e-4])
        ax.set_yticklabels(["min", "p50", "p90", "p99", "p99.9", "p99.99"])
        ax.legend(frameon=False, fontsize=8.6, labelcolor=INK, loc="upper left")

    plot(
        axes[0],
        [("recv \u2192 book, in the handler's process", "clean_handler", ACCENT, 2.0),
         ("exchange \u2192 book, across the wire", "clean_wire", ACCENT_DARK, 2.0)],
        "No packet loss",
        "latency (\u00b5s, log scale)",
    )
    plot(
        axes[1],
        [("arrived on multicast (fast path)", "recovery_fast_path", ACCENT, 2.0),
         ("arrived via TCP recovery", "recovery_tcp_path", WARN, 2.0)],
        "2% of datagrams dropped",
        "exchange \u2192 book latency (\u00b5s, log scale)",
    )

    counts = curves.get("recovery_counts")
    if counts:
        axes[1].text(
            0.98, 0.04,
            f"{counts['fast']:,} on multicast, {counts['recovered']:,} recovered",
            transform=axes[1].transAxes, ha="right", va="bottom",
            fontsize=8.2, color=MUTED,
        )

    fig.suptitle(
        "Where the tail comes from",
        x=0.007, y=1.06, ha="left", fontsize=13, fontweight="bold", color=INK,
    )
    fig.text(
        0.007, 0.995,
        "400,000 messages at 200,000 msg/s. Recovery is a round trip, so retransmitted "
        "messages are slower by design; the tail it adds to the fast path is not.",
        ha="left", fontsize=9, color=MUTED,
    )
    save(fig, "latency_distribution")


def figure_stage_breakdown(stages):
    order = [
        ("udp recv -> parsed", "receive \u2192 decode"),
        ("parsed -> enqueue", "decode \u2192 sequence \u2192 enqueue"),
        ("ring transit", "shared-memory ring transit"),
        ("book update", "order book update"),
        ("recv -> book (handler)", "total: receive \u2192 book"),
    ]
    rows = [(pretty, stages[key]) for key, pretty in order if key in stages]

    fig, ax = plt.subplots(figsize=(9.6, 4.0))
    fig.patch.set_facecolor(BACKGROUND)
    positions = range(len(rows))
    height = 0.26

    for offset, metric, color, label in [
        (height, "p50", ACCENT, "p50"),
        (0.0, "p99", ACCENT_DARK, "p99"),
        (-height, "p999", WARN, "p99.9"),
    ]:
        values = [row[1][metric] for row in rows]
        bars = ax.barh([p + offset for p in positions], values, height=height,
                       color=color, label=label)
        for bar, value in zip(bars, values):
            ax.text(bar.get_width() * 1.06, bar.get_y() + bar.get_height() / 2,
                    f"{value:.3f}", va="center", fontsize=7.8, color=INK)

    ax.set_yticks(list(positions))
    ax.set_yticklabels([row[0] for row in rows], fontsize=9, color=INK)
    ax.invert_yaxis()
    ax.set_xscale("log")
    ax.grid(True, axis="x", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    style(ax, "Per-stage latency, no packet loss",
          "microseconds (log scale), 400,000 messages")
    ax.legend(frameon=False, fontsize=8.6, labelcolor=INK, loc="lower right", ncols=3)
    save(fig, "stage_breakdown")


SHORT_NAMES = {
    "mutex + deque": "mutex\n+ deque",
    "lock-free, shared line": "lock-free,\nindexes share\na cache line",
    "lock-free, cache-aligned": "lock-free,\nindexes on\nseparate lines",
    "+ cached indices (shipped)": "+ cached\nopposite index\n(shipped)",
}

# The left panel already spells the variants out; the right panel only needs
# enough to identify the row without running into it.
TERSE_NAMES = {
    "mutex + deque": "mutex",
    "lock-free, shared line": "shared line",
    "lock-free, cache-aligned": "separate lines",
    "+ cached indices (shipped)": "+ cached index",
}


def figure_queue_variants(throughput, paced, repeats):
    fig, axes = plt.subplots(1, 2, figsize=(12.4, 4.5), width_ratios=[1.15, 1])
    fig.subplots_adjust(wspace=0.32)
    fig.patch.set_facecolor(BACKGROUND)

    names = [label.split(". ", 1)[1] for label, *_ in throughput]
    short = [SHORT_NAMES.get(name, name) for name in names]
    medians = [row[1] for row in throughput]
    lows = [row[2] for row in throughput]
    highs = [row[3] for row in throughput]
    colors = [NEUTRAL, "#9ec5fe", "#6ea8fe", ACCENT]

    bars = axes[0].bar(range(len(medians)), medians, color=colors[: len(medians)], width=0.58)
    # Error bars are the whole point here: one of these variants is far less
    # predictable than its median suggests.
    axes[0].errorbar(
        range(len(medians)), medians,
        yerr=[[m - lo for m, lo in zip(medians, lows)],
              [hi - m for m, hi in zip(medians, highs)]],
        fmt="none", ecolor=INK, elinewidth=1.4, capsize=6, capthick=1.4,
    )
    for index, (bar, value) in enumerate(zip(bars, medians)):
        axes[0].text(bar.get_x() + bar.get_width() / 2, highs[index] + max(highs) * 0.035,
                     f"{value:.2f}", ha="center", fontsize=9.4, color=INK, fontweight="bold")
        if index > 0:
            axes[0].text(bar.get_x() + bar.get_width() / 2, value * 0.42,
                         f"\u00d7{value / medians[index - 1]:.1f}", ha="center",
                         fontsize=10.5, color="#ffffff", fontweight="bold")
    axes[0].set_xticks(range(len(short)))
    axes[0].set_xticklabels(short, fontsize=8.2, color=INK, linespacing=1.4)
    axes[0].set_ylim(0, max(highs) * 1.18)
    axes[0].grid(True, axis="y", color=GRID, linewidth=0.8)
    axes[0].set_axisbelow(True)
    style(axes[0], f"Sustained throughput (median and range of {repeats})",
          None, "million messages / second")

    positions = list(range(len(paced)))
    p50 = [row[1] for row in paced]
    p99 = [row[2] for row in paced]
    axes[1].barh([p - 0.17 for p in positions], p50, height=0.32, color=ACCENT, label="p50")
    axes[1].barh([p + 0.17 for p in positions], p99, height=0.32, color=ACCENT_DARK, label="p99")
    for index, (a, b) in enumerate(zip(p50, p99)):
        axes[1].text(a + max(p99) * 0.015, index - 0.17, f"{a:.3f}", va="center",
                     fontsize=8.2, color=INK)
        axes[1].text(b + max(p99) * 0.015, index + 0.17, f"{b:.3f}", va="center",
                     fontsize=8.2, color=INK)
    axes[1].set_yticks(positions)
    axes[1].set_yticklabels([TERSE_NAMES.get(n, n) for n in names], fontsize=8.6, color=INK)
    axes[1].invert_yaxis()
    axes[1].set_xlim(0, max(p99) * 1.22)
    axes[1].grid(True, axis="x", color=GRID, linewidth=0.8)
    axes[1].set_axisbelow(True)
    style(axes[1], f"Handoff latency at a paced 1 M msg/s (median of {repeats})",
          "microseconds")
    axes[1].legend(frameon=False, fontsize=8.6, labelcolor=INK, loc="lower right")

    fig.suptitle(
        "Four ways to hand a message between two threads",
        x=0.007, y=1.10, ha="left", fontsize=13.5, fontweight="bold", color=INK,
    )
    fig.text(
        0.007, 1.005,
        "2,000,000 messages per variant, producer and consumer pinned to separate cores. "
        "Dropping the mutex and splitting the cache line are large and repeatable wins. "
        "Caching the opposite index improves latency slightly but makes throughput "
        "bimodal, so its median is reported with the full range.",
        ha="left", fontsize=9, color=MUTED,
    )
    save(fig, "queue_variants")


BOOK_LABELS = {
    "node-based, index unreserved": "node-based,\nindex\nunreserved",
    "node-based, index reserved": "node-based,\nindex\nreserved",
    "preallocated (shipped)": "preallocated\n(shipped)",
}


def figure_book_comparison(rows):
    fig, axes = plt.subplots(1, 3, figsize=(12.0, 4.0))
    fig.subplots_adjust(wspace=0.34)
    fig.patch.set_facecolor(BACKGROUND)
    labels = [BOOK_LABELS.get(r["label"], r["label"]) for r in rows]
    colors = ["#c7ccd1", NEUTRAL, ACCENT][: len(rows)]

    def bars(ax, values, title, ylabel, fmt, log=False):
        drawn = ax.bar(range(len(values)), values, color=colors, width=0.56)
        if log:
            ax.set_yscale("log")
        for bar, value in zip(drawn, values):
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() if bar.get_height() > 0 else 1,
                    fmt(value), ha="center", va="bottom", fontsize=9.2,
                    color=INK, fontweight="bold")
        ax.set_xticks(range(len(values)))
        ax.set_xticklabels(labels, fontsize=7.8, color=INK, linespacing=1.4)
        ax.grid(True, axis="y", color=GRID, linewidth=0.8)
        ax.set_axisbelow(True)
        style(ax, title, None, ylabel)

    bars(axes[0], [r["ops"] for r in rows], "Throughput", "million ops / second",
         lambda v: f"{v:.2f}")
    bars(axes[1], [max(r["allocs"], 0.6) for r in rows], "Heap allocations",
         "allocations (log scale)",
         lambda v: "0" if v < 1 else f"{int(v):,}", log=True)
    bars(axes[2], [r["p99"] for r in rows], "p99 per operation", "nanoseconds",
         lambda v: f"{int(v)} ns")

    fig.suptitle(
        "What preallocation actually buys, against a fairly configured baseline",
        x=0.007, y=1.10, ha="left", fontsize=13, fontweight="bold", color=INK,
    )
    fig.text(
        0.007, 1.005,
        "2,000,000 mixed add/cancel/modify/trade operations against identical order flow, "
        "median of 5. The middle baseline differs from the left one by a single "
        "index_.reserve() call, which removes a 36 ms rehash pause \u2014 so the shipped book "
        "should be judged against the reserved column, where it wins on throughput, p99 and "
        "allocation count but not at the median.",
        ha="left", fontsize=8.6, color=MUTED,
    )
    save(fig, "book_comparison")


def figure_recovery_accounting(counters):
    fig, ax = plt.subplots(figsize=(10.4, 3.4))
    fig.patch.set_facecolor(BACKGROUND)
    ax.axis("off")

    injected = counters["dropped"]
    kernel = counters["sent"] - counters["received"]
    missing = counters["missing"]
    recovered = counters["recovered"]
    gaps = counters.get("gaps", 0)
    total = injected + kernel

    # Counts go inside the segments and the wording goes in a legend, so a narrow
    # segment cannot clip its own label.
    segments = [
        (injected, WARN, "dropped by the exchange on purpose"),
        (kernel, "#c9930a", "dropped by the kernel under load"),
    ]
    left = 0.0
    for value, color, _ in segments:
        width = value / total * 100
        ax.add_patch(FancyBboxPatch(
            (left, 62), width, 15,
            boxstyle="round,pad=0.2,rounding_size=0.9",
            linewidth=0, facecolor=color,
        ))
        ax.text(left + width / 2, 69.5, f"{value:,}", ha="center", va="center",
                fontsize=11, color="#ffffff", fontweight="bold")
        left += width

    legend_x = 0.0
    for value, color, text in segments:
        ax.add_patch(FancyBboxPatch(
            (legend_x, 53.5), 2.0, 2.6,
            boxstyle="round,pad=0.1,rounding_size=0.4",
            linewidth=0, facecolor=color,
        ))
        ax.text(legend_x + 3.4, 54.8, text, ha="left", va="center", fontsize=8.8, color=INK)
        legend_x += 46.0

    ax.add_patch(FancyArrowPatch((50, 50), (50, 34), arrowstyle="-|>",
                                 mutation_scale=15, linewidth=1.6, color=MUTED))
    ax.text(50, 42, f"{gaps:,} gaps spanning {missing:,} messages",
            ha="center", va="center", fontsize=9, color=INK, fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.34", facecolor=BACKGROUND, edgecolor=GRID))

    ax.add_patch(FancyBboxPatch(
        (0, 17), 100, 15,
        boxstyle="round,pad=0.2,rounding_size=0.9",
        linewidth=0, facecolor=GOOD,
    ))
    ax.text(50, 24.5, f"recovered over TCP: {recovered:,}      unrecoverable: "
                      f"{counters['unrecoverable']}",
            ha="center", va="center", fontsize=11, color="#ffffff", fontweight="bold")

    ax.text(0, 6.5, f"{injected:,} deliberate + {kernel:,} genuine = {missing:,} missing, "
                    f"all {recovered:,} retransmitted, {counters['applied']:,} of "
                    f"{counters['consumed']:,} applied to the book.",
            fontsize=9.2, color=MUTED)
    ax.text(0, 0.5, "The engine's reconstructed book matched the exchange's authoritative "
                    "book exactly.", fontsize=9.2, color=MUTED)

    ax.set_xlim(-1, 101)
    ax.set_ylim(-3, 86)
    ax.set_title("Every missing message is accounted for", color=INK, fontsize=12.5,
                 fontweight="bold", loc="left", pad=10)
    save(fig, "recovery_accounting")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clean-csv", help="CSV from the no-loss pipeline run")
    parser.add_argument("--recovery-csv", help="CSV from the packet-loss pipeline run")
    args = parser.parse_args()

    cache_path = os.path.join(MEASUREMENTS, "latency_percentiles.json")
    if args.clean_csv or args.recovery_csv:
        curves = curves_from_csv(args.clean_csv, args.recovery_csv)
        with open(cache_path, "w") as handle:
            json.dump(curves, handle, indent=1)
        print(f"docs/measurements/latency_percentiles.json ({len(curves)} series)")
    elif os.path.exists(cache_path):
        with open(cache_path) as handle:
            curves = json.load(handle)
    else:
        print("no CSV given and no cached quantiles found", file=sys.stderr)
        return 1

    figure_architecture()
    figure_latency_distribution(curves)
    figure_stage_breakdown(parse_stages("pipeline_clean.log"))
    figure_queue_variants(*parse_queue("bench_queue.log"))
    figure_book_comparison(parse_book("bench_book.log"))
    figure_recovery_accounting(parse_counters("pipeline_recovery.log"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
