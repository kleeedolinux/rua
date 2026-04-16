# Rua Security and Fuzzing Notes

This repository includes baseline hardening primitives:

- Bytecode parser rejects malformed streams (`decode_module`).
- Structural bytecode validator rejects invalid indices/jumps/handlers.
- VM enforces deterministic hard limits (process/mailbox/stack/heap/step).
- FFI is capability-gated by default.

## Fuzzing Targets (recommended)

1. Lexer/parser:
- Generate random UTF-8 input and ensure no panics.

2. Compiler/IR:
- Compile random-but-valid AST fragments and ensure deterministic errors.

3. Bytecode:
- Feed random bytes into `decode_module` and `validate_module`.

4. VM:
- Execute random small modules under strict limits and assert no UB/panic.

## CI Recommendation

- Run fuzz corpus regression on every PR.
- Nightly scheduled fuzz jobs should run longer campaigns.
