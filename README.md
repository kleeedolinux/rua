# Rua Language

Rua is a Lua-like language with a pure-functional core, expression-oriented semantics, and lightweight embeddability.

## Design Goals

- Lua-like readable syntax
- Immutable data and bindings by default
- Functions as the main abstraction
- Actor-style concurrency with isolated processes
- Small runtime footprint
- Explicit unsafe boundary for host/system effects

## Core Syntax

### Bindings

```rua
local x = 10
local name = "Lia"
```

Bindings are immutable and lexically scoped.

### Functions

```rua
local add = fn(a, b)
  a + b
end

local inc = fn(x) x + 1 end
```

Functions are first-class values. Function bodies evaluate to a value.

### If Expressions

```rua
local abs = fn(n)
  if n < 0 then
    -n
  else
    n
  end
end
```

`if` is an expression and always returns a value.

### Lists and Records

```rua
local xs = {1, 2, 3}
local person = { nome = "Lia", idade = 20 }
```

- Lists are ordered immutable collections
- Records are immutable key-value structures

### Immutable Record Update

```rua
local adult = { person with idade = 21 }
```

Creates a new record, never mutates original data.

### Field Access and Calls

```rua
person.nome
add(1, 2)
```

## Language Semantics

- Expression-oriented evaluation model
- No mutable variables in core language
- Structural equality for values
- Recursion is the primary looping model

Example:

```rua
local fact = fn(n)
  if n == 0 then
    1
  else
    n * fact(n - 1)
  end
end

fact(5)
```

## Concurrency Model

Rua provides lightweight isolated processes with mailbox message passing.

### Process Primitives

- `spawn(fn() ... end)`
- `send(pid, msg)`
- `self()`
- `exit(reason)`

### Receive Expression

```rua
local msg =
  receive
    case { type = "ping" } -> "pong"
    case _ -> nil
  end
```

### Timeout Form

```rua
receive
  case { type = "ok", value = x } -> x
  after 1000 -> "timeout"
end
```

### Lifecycle and Coordination

Rua supports:
- links/unlinks
- monitors (`DOWN` notifications)
- supervision with restart policies
- process registry (`register`, `whereis`)

## Pattern Matching (Receive)

Supported patterns:
- wildcard: `_`
- binding: `msg`
- literal: `42`, `"ok"`, `true`, `nil`
- record patterns (including nested)

Example:

```rua
receive
  case { type = "result", ref = id, value = v } -> v
end
```

## Unsafe Boundary and FFI

Unsafe operations are explicit:

```rua
unsafe ffi_register("libc.strlen", "libc.so.6", "strlen", "u64", "cstring")
unsafe ffi("libc.strlen", "Rua")
```

- `unsafe` marks privileged interop operations
- capability-based FFI is the default safety model

## Modules

Rua uses `require("name")` for module loading.

Supported module forms:
- source modules (`.rua`)
- bytecode modules (`.ruac`)

Typical style:

```rua
local cfg = require("config")
cfg.env
```

## Standard Library (Current Subset)

### `math`
- `abs`, `max`, `min`, `sqrt`

### `string`
- `len`, `lower`, `upper`

### `table`
- `len`

## Bytecode

Rua defines a stable `RUAC` bytecode container with versioning and validation.

This enables:
- source-independent execution
- compatibility policy evolution
- malformed bytecode rejection

## Examples

See `examples/` for complete scripts:
- functional patterns
- records/metatable style
- receive/timeout
- supervision/monitoring
- module loading
- FFI usage

## Benchmarking

See `benchmarks/` for Rua vs Lua vs Haskell benchmark workloads and runner scripts.

## Tooling

- CLI: `rua <script.rua|script.ruac> ...`
- Embedding: C API via `crates/rua_capi/include/rua.h`
- CI/CD includes multi-platform builds, smoke tests, size budget, and auto-tagging from `version.rua`.
