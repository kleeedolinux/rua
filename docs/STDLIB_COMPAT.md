# Rua Stdlib Compatibility Contract (v0.1)

Rua currently ships a minimal Lua-compatible subset focused on embeddable purity:

## `math`
- `math.abs(x)`
- `math.max(a, b)`
- `math.min(a, b)`
- `math.sqrt(x)`

## `string`
- `string.len(s)`
- `string.lower(s)`
- `string.upper(s)`

## `table`
- `table.len(x)` where `x` is list or record

## Conformance
- Conformance tests are located in `crates/rua_vm/src/lib.rs` under:
  - `stdlib_math_string_table_subset`

## Notes
- Contract is intentionally small and stable for embedders.
- Additional Lua surface should be added with tests and versioned compatibility notes.
