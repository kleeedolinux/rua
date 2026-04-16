#!/usr/bin/env bash
set -euo pipefail

mkdir -p bin
ghc -O2 -o bin/fib haskell/Fib.hs
ghc -O2 -o bin/sum_loop haskell/SumLoop.hs
echo "Built Haskell binaries in benchmarks/bin"
