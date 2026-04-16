# Benchmarks

This folder contains comparable micro-benchmarks for:
- `rua` (this project)
- `lua` (Lua interpreter)
- `haskell` (GHC compiled binary)

## Workloads
- `fib` (naive recursion)
- `sum_loop` (integer accumulation style)

## Layout
- `rua/` Rua source files
- `lua/` Lua source files
- `haskell/` Haskell source files
- `run_hyperfine.sh` benchmark runner (requires `hyperfine`)
- `build_haskell.sh` builds Haskell binaries with `ghc -O2`

## Usage

```bash
cd benchmarks
./build_haskell.sh
./run_hyperfine.sh
```

## Notes
- Ensure `rua`, `lua`, and `ghc` are in `PATH`.
- Rua command defaults to `../target/release/rua`.
