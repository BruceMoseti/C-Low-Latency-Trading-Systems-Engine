#!/usr/bin/env bash
# Brings up the three processes in dependency order and reports what each one saw.
#
#   exchange_simulator   publishes the feed, serves TCP recovery
#   market_data_handler  receives, sequences, recovers, writes the ring
#   order_book_engine    reads the ring, maintains the book, reports latency
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_dir="${BUILD_DIR:-${repo_root}/build}"

messages=200000
drop_rate=0
rate=0
batch=1
out_dir=""
csv=""
pin=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --messages) messages="$2"; shift 2 ;;
    --drop-rate) drop_rate="$2"; shift 2 ;;
    --rate) rate="$2"; shift 2 ;;
    --batch) batch="$2"; shift 2 ;;
    --out) out_dir="$2"; shift 2 ;;
    --csv) csv="$2"; shift 2 ;;
    --pin) pin=1; shift ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "${out_dir}" ]]; then
  out_dir="$(mktemp -d "${TMPDIR:-/tmp}/llte-run-XXXXXX")"
fi
mkdir -p "${out_dir}"

for binary in exchange_simulator market_data_handler order_book_engine; do
  if [[ ! -x "${build_dir}/${binary}" ]]; then
    echo "missing ${build_dir}/${binary} -- run scripts/build.sh first" >&2
    exit 1
  fi
done

# A previous crashed run can leave the segment behind with stale indices.
rm -f /dev/shm/llte_feed

engine_cpu=()
handler_cpu=()
exchange_cpu=()
if [[ "${pin}" == "1" ]]; then
  exchange_cpu=(--cpu 1)
  handler_cpu=(--cpu 2)
  engine_cpu=(--cpu 3)
fi

engine_csv=()
if [[ -n "${csv}" ]]; then
  engine_csv=(--csv "${csv}")
fi

echo "run directory: ${out_dir}"
echo "messages=${messages} drop-rate=${drop_rate} rate=${rate} batch=${batch} pin=${pin}"
echo

# The handler connects to the exchange's recovery server at startup, so the
# exchange goes first; its start delay holds the feed until subscribers are up.
"${build_dir}/exchange_simulator" \
  --messages "${messages}" --drop-rate "${drop_rate}" --rate "${rate}" --batch "${batch}" \
  --start-delay-ms 1500 --linger-ms 3000 "${exchange_cpu[@]}" \
  > "${out_dir}/exchange.log" 2>&1 &
exchange_pid=$!

sleep 0.4

"${build_dir}/market_data_handler" \
  --expect "${messages}" --idle-ms 2000 "${handler_cpu[@]}" \
  > "${out_dir}/handler.log" 2>&1 &
handler_pid=$!

"${build_dir}/order_book_engine" \
  --attach-timeout-ms 15000 "${engine_csv[@]}" "${engine_cpu[@]}" \
  > "${out_dir}/engine.log" 2>&1 &
engine_pid=$!

status=0
wait "${handler_pid}" || status=$?
wait "${engine_pid}" || status=$?
wait "${exchange_pid}" || status=$?

for stage in exchange handler engine; do
  echo "───────────────────────────── ${stage} ─────────────────────────────"
  cat "${out_dir}/${stage}.log"
  echo
done

exit "${status}"
