#!/usr/bin/env bash
set -euo pipefail

RUA_BIN="${RUA_BIN:-../target/release/rua}"
LUA_BIN="${LUA_BIN:-lua}"

if ! command -v hyperfine >/dev/null 2>&1; then
  echo "hyperfine is required. Install it first."
  exit 1
fi

if [[ ! -x "$RUA_BIN" ]]; then
  echo "Rua binary not found/executable at: $RUA_BIN"
  echo "Run: cargo build --release"
  exit 1
fi

if [[ ! -x "bin/fib" || ! -x "bin/sum_loop" ]]; then
  echo "Missing Haskell binaries. Run ./build_haskell.sh first."
  exit 1
fi

echo "== fib benchmark =="
hyperfine --warmup 3 \
  "$RUA_BIN rua/fib.rua" \
  "$LUA_BIN lua/fib.lua" \
  "./bin/fib"

echo
echo "== sum_loop benchmark =="
hyperfine --warmup 3 \
  "$RUA_BIN rua/sum_loop.rua" \
  "$LUA_BIN lua/sum_loop.lua" \
  "./bin/sum_loop"
