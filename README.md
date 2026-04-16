# Rua

Rua is a Lua-like, expression-oriented, pure-functional language implemented in Rust.

Goals:
- Lua-like syntax and embeddability
- immutable/pure-functional core semantics
- lightweight runtime and small distribution footprint
- actor-style concurrency with message passing

## Workspace Layout

- `src/` (`rua`): frontend (lexer, parser, AST, IR, compiler)
- `crates/rua_vm`: bytecode VM, runtime, scheduler, GC, process model
- `crates/rua_cli`: CLI runner for `.rua` files
- `crates/rua_capi`: embeddable C API (`rua.h` + FFI-safe interface)
- `examples/`: runnable Rua programs

## Language Core (v0.1)

Implemented front-end forms include:
- immutable bindings: `local x = expr`
- functions: `fn(args) ... end`
- expression `if` blocks
- lists and records: `{1,2,3}`, `{ name = "Lia" }`
- immutable record update: `{ rec with field = value }`
- field access: `obj.field`
- function calls
- `receive/case/after` concurrency expression
- `unsafe` annotation blocks for privileged host operations

## Concurrency Runtime

Rua uses isolated lightweight processes with mailbox messaging.

Builtins:
- `spawn`, `spawn_link`, `spawn_monitor`
- `send`, `self`, `exit`
- `link`, `unlink`, `monitor`, `demonitor`
- `register`, `unregister`, `whereis`
- `supervisor(child_fn, policy, max_restarts, window_ticks)`

### Scheduling

- deterministic round-robin run queue
- configurable preemptive timeslice (`VmConfig::timeslice_instructions`)
- per-step VM tick accounting
- blocked/runnable process tracking

### `receive after` semantics

`receive ... after timeout_expr -> body end` uses real timeout waiting:
- timeout expression is evaluated first
- process blocks until either
  - matching message arrives, or
  - timeout deadline is reached
- if timeout is reached first, after-body executes

## VM + Runtime

`rua_vm` provides:
- bytecode interpreter
- process scheduler + mailboxes
- metatable-style record fallback (`with_meta`, `get_meta`)
- host FFI call gate: `unsafe ffi("name", ...)`
- native module registry + `require("name")`
- generational incremental GC (young/old + incremental slices)

Lua-style runtime records currently exposed:
- `math` (`abs`, `max`, `min`, `sqrt`)
- `string` (`len`, `lower`, `upper`)
- `table` (`len`)

## CLI

Run scripts:

```bash
cargo run -p rua_cli -- examples/01_arith_if.rua
cargo run -p rua_cli -- examples/06_receive_after.rua
```

Release build:

```bash
cargo build --release
./target/release/rua examples/01_arith_if.rua
```

## C API (`rua_capi`)

Header: `crates/rua_capi/include/rua.h`

Highlights:
- opaque `RuaVmHandle`
- create/free VM from source/file
- run/step/step_n incremental execution
- status enum (`RuaStatus`) and typed error code enum (`RuaErrorCode`)
- result/error/state string retrieval
- host function registration for `unsafe ffi(...)`
- native module registration from source (scalar-return in v0.1)
- GC controls and telemetry

## Build and Test

```bash
cargo test --workspace
cargo build --release
```

## Current Scope and Notes

- Core is expression-oriented and immutable-first.
- Actor/process primitives are implemented with deterministic scheduling.
- Supervision supports restart policies: `temporary`, `transient`, `permanent`.
- C API focuses on stable embedding primitives and incremental control loops.

## Roadmap

Planned areas:
- richer standard library compatibility surface
- stronger typed FFI value marshaling
- module system evolution
- deeper optimization and profiling for runtime footprint
