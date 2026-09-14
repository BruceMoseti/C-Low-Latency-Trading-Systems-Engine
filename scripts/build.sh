#!/usr/bin/env bash
# Configure and build. Safe to re-run: CMake reuses the existing build tree.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_dir="${BUILD_DIR:-${repo_root}/build}"
build_type="${BUILD_TYPE:-Release}"
sanitizer="${SANITIZER:-off}"

# This image ships libstdc++-13-dev but a GCC 14 runtime, so clang selects the
# GCC 14 directory and fails to find libstdc++.so. GCC works as-is; clang needs
# to be pointed at the toolchain that actually has the development symlink.
if [[ -z "${CXX:-}" ]]; then
  export CXX=g++
fi

cmake -S "${repo_root}" -B "${build_dir}" \
  -DCMAKE_BUILD_TYPE="${build_type}" \
  -DLLTE_SANITIZER="${sanitizer}"

cmake --build "${build_dir}" -j "$(nproc)"

echo "build complete: ${build_dir} (type=${build_type}, sanitizer=${sanitizer}, CXX=${CXX})"
