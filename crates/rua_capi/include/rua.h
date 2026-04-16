#ifndef RUA_H
#define RUA_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct RuaVmHandle RuaVmHandle;

typedef enum RuaStatus {
    RUA_STATUS_OK = 0,
    RUA_STATUS_HALTED = 1,
    RUA_STATUS_BLOCKED = 2,
    RUA_STATUS_ERROR = 3
} RuaStatus;

typedef enum RuaErrorCode {
    RUA_ERROR_NONE = 0,
    RUA_ERROR_NULL_POINTER = 1,
    RUA_ERROR_INVALID_UTF8 = 2,
    RUA_ERROR_COMPILE_ERROR = 3,
    RUA_ERROR_RUNTIME_ERROR = 4,
    RUA_ERROR_TYPE_ERROR = 5,
    RUA_ERROR_UNKNOWN_GLOBAL = 6,
    RUA_ERROR_UNKNOWN_FIELD = 7,
    RUA_ERROR_ARITY_MISMATCH = 8,
    RUA_ERROR_INVALID_CALL_TARGET = 9,
    RUA_ERROR_INVALID_INSTRUCTION = 10,
    RUA_ERROR_RECEIVE_BLOCKED = 11,
    RUA_ERROR_HALTED = 12,
    RUA_ERROR_PROCESS_NOT_FOUND = 13,
    RUA_ERROR_INVALID_RESTART_STRATEGY = 14,
    RUA_ERROR_LIMIT_EXCEEDED = 15,
    RUA_ERROR_SECURITY_VIOLATION = 16,
    RUA_ERROR_INVALID_BYTECODE = 17,
    RUA_ERROR_MODULE_VERIFICATION_FAILED = 18
} RuaErrorCode;

/*
 * Host callback used by unsafe ffi("name", ...).
 * - argc: number of UTF-8 string arguments
 * - argv: array of UTF-8 C strings
 * Return:
 * - UTF-8 C string pointer owned by host callback side
 * - NULL means nil
 * Convention:
 * - returning "error:<message>" signals an FFI error
 */
typedef const char* (*RuaHostCallback)(void* user_data, size_t argc, const char* const* argv);

/* VM lifecycle */
RuaVmHandle* rua_vm_new_from_source(const char* source);
RuaVmHandle* rua_vm_new_from_file(const char* path);
RuaVmHandle* rua_vm_new_from_bytecode_file(const char* path);
void rua_vm_free(RuaVmHandle* vm);

/* VM execution */
int rua_vm_run(RuaVmHandle* vm);
int rua_vm_step(RuaVmHandle* vm);
RuaStatus rua_vm_run_status(RuaVmHandle* vm);
RuaStatus rua_vm_step_status(RuaVmHandle* vm);
RuaStatus rua_vm_step_n(RuaVmHandle* vm, size_t max_steps);
char* rua_vm_state_string(RuaVmHandle* vm);

/* VM diagnostics/results (returned strings must be freed with rua_string_free) */
char* rua_vm_result_string(RuaVmHandle* vm);
char* rua_vm_last_error(RuaVmHandle* vm);
RuaErrorCode rua_vm_last_error_code(RuaVmHandle* vm);

/* GC controls and telemetry */
int rua_vm_gc_set_threshold(RuaVmHandle* vm, size_t threshold);
int rua_vm_gc_set_full_every_minor(RuaVmHandle* vm, size_t count);
int rua_vm_gc_collect_now(RuaVmHandle* vm);
char* rua_vm_gc_stats(RuaVmHandle* vm);
int rua_vm_apply_embedded_profile(RuaVmHandle* vm);
int rua_vm_set_limits(
    RuaVmHandle* vm,
    size_t max_steps,
    size_t max_processes,
    size_t max_mailbox_messages,
    size_t max_stack_values,
    size_t max_heap_objects
);
int rua_vm_add_module_search_path(RuaVmHandle* vm, const char* path);
int rua_vm_set_require_signed_modules(RuaVmHandle* vm, int required);
int rua_vm_set_allow_unrestricted_system_ffi(RuaVmHandle* vm, int allowed);

/* Register named host function for unsafe ffi("name", ...) */
int rua_vm_register_host_fn(
    RuaVmHandle* vm,
    const char* name,
    RuaHostCallback callback,
    void* user_data
);
int rua_vm_register_native_module_source(
    RuaVmHandle* vm,
    const char* name,
    const char* source
);
int rua_vm_register_system_ffi_capability(
    RuaVmHandle* vm,
    const char* cap_name,
    const char* lib_name,
    const char* symbol_name,
    const char* ret_type,
    const char* param_types_csv
);

/* Convenience one-shot APIs */
char* rua_eval_source(const char* source);
char* rua_eval_file(const char* path);

/* Free strings allocated by Rua C API */
void rua_string_free(char* ptr);

#ifdef __cplusplus
}
#endif

#endif /* RUA_H */
