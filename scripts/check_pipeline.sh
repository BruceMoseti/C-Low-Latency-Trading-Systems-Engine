#!/usr/bin/env bash
# Runs the three-process pipeline and asserts that the book was reconstructed
# exactly, that every gap was filled, and that the message accounting closes.
#
# This is the end-to-end regression check: a pipeline that starts, runs and exits
# zero can still have silently lost or misapplied messages, so exit codes alone
# prove nothing. Used by CI and safe to run by hand.
#
#   scripts/check_pipeline.sh --messages 200000 --rate 200000 --drop-rate 0.02
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_dir="${BUILD_DIR:-${repo_root}/build}"

messages=200000
rate=200000
drop_rate=0
batch=1
seed=42
pin_flag=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --messages) messages="$2"; shift 2 ;;
    --rate) rate="$2"; shift 2 ;;
    --drop-rate) drop_rate="$2"; shift 2 ;;
    --batch) batch="$2"; shift 2 ;;
    --seed) seed="$2"; shift 2 ;;
    --pin) pin_flag=(--pin); shift ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

out_dir="$(mktemp -d "${TMPDIR:-/tmp}/llte-check-XXXXXX")"

BUILD_DIR="${build_dir}" "${repo_root}/scripts/run_pipeline.sh" \
  --messages "${messages}" --rate "${rate}" --drop-rate "${drop_rate}" \
  --batch "${batch}" --seed "${seed}" "${pin_flag[@]}" --out "${out_dir}" > "${out_dir}/combined.log" 2>&1 || {
    echo "pipeline exited non-zero" >&2
    cat "${out_dir}/combined.log" >&2
    exit 1
  }

exchange_log="${out_dir}/exchange.log"
handler_log="${out_dir}/handler.log"
engine_log="${out_dir}/engine.log"

field() { # field <file> <regex-with-one-group>
  sed -nE "s/.*$2.*/\1/p" "$1" | head -1
}

exchange_bid=$(field "${exchange_log}" 'book best_bid=(-?[0-9]+)')
exchange_ask=$(field "${exchange_log}" 'best_ask=(-?[0-9]+)')
exchange_live=$(field "${exchange_log}" 'live_orders=([0-9]+)')
exchange_digest=$(field "${exchange_log}" 'book_digest=([0-9a-f]+)')

engine_bid=$(field "${engine_log}" 'best_bid=(-?[0-9]+)')
engine_ask=$(field "${engine_log}" 'best_ask=(-?[0-9]+)')
engine_live=$(field "${engine_log}" 'live_orders=([0-9]+)')
engine_digest=$(field "${engine_log}" 'book_digest=([0-9a-f]+)')

handler_published=$(field "${handler_log}" 'published=([0-9]+)')
missing=$(field "${handler_log}" 'missing=([0-9]+)')
recovered=$(field "${handler_log}" 'recovered=([0-9]+)')
unrecoverable=$(field "${handler_log}" 'unrecoverable=([0-9]+)')
malformed=$(field "${handler_log}" 'malformed=([0-9]+)')
dropped_on_shutdown=$(field "${handler_log}" 'dropped_on_shutdown=([0-9]+)')

consumed=$(field "${engine_log}" 'consumed ([0-9]+) events')
applied=$(field "${engine_log}" 'applied=([0-9]+)')
unknown=$(field "${engine_log}" 'unknown_order=([0-9]+)')
out_of_band=$(field "${engine_log}" 'out_of_band=([0-9]+)')
over_filled=$(field "${engine_log}" 'over_filled=([0-9]+)')

failures=0
check() { # check <description> <actual> <expected>
  if [[ "$2" == "$3" ]]; then
    printf '  ok    %-46s %s\n' "$1" "$2"
  else
    printf '  FAIL  %-46s got %s, want %s\n' "$1" "$2" "$3"
    failures=$((failures + 1))
  fi
}

echo "pipeline check: messages=${messages} rate=${rate} drop-rate=${drop_rate} batch=${batch} seed=${seed}"
echo

# The digest covers every occupied level on both sides plus the id, quantity and
# queue position of every resting order. The touch and the live count alone are far
# too weak: with this simulator's price distribution the touch is pinned, so those
# two assertions can pass while thousands of messages are missing.
check "engine book digest matches exchange" "${engine_digest}" "${exchange_digest}"
check "engine best bid matches exchange" "${engine_bid}" "${exchange_bid}"
check "engine best ask matches exchange" "${engine_ask}" "${exchange_ask}"
check "engine live order count matches exchange" "${engine_live}" "${exchange_live}"

check "handler published every message" "${handler_published}" "${messages}"
check "engine consumed every message" "${consumed}" "${messages}"
check "engine applied every message" "${applied}" "${consumed}"

check "every gap was recovered" "${recovered}" "${missing}"
check "no message abandoned" "${unrecoverable}" "0"
check "nothing dropped at shutdown" "${dropped_on_shutdown}" "0"
check "no malformed packet" "${malformed}" "0"
check "no unknown order referenced" "${unknown}" "0"
check "no out-of-band price" "${out_of_band}" "0"
# An over-fill can only happen if an Add or Modify never arrived, so it is a
# desync signal even when every other counter looks clean.
check "no over-filled order" "${over_filled}" "0"

echo
if [[ "${failures}" -eq 0 ]]; then
  echo "PASS  book digest ${engine_digest} matches the exchange;" \
       "${recovered} of ${missing} missing messages recovered over TCP"
  rm -rf "${out_dir}"
  exit 0
fi

echo "FAIL  ${failures} check(s) failed; logs kept in ${out_dir}"
cat "${out_dir}/combined.log"
exit 1
