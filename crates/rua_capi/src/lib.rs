use std::ffi::{CStr, CString};
use std::fs;
use std::os::raw::{c_char, c_int, c_void};
use std::path::PathBuf;

use rua::bytecode::decode_module;
use rua::frontend::compile_source;
use rua_vm::{FfiSignature, FfiType, Value, Vm, VmError, VmProfile};

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuaStatus {
    Ok = 0,
    Halted = 1,
    Blocked = 2,
    Error = 3,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuaErrorCode {
    None = 0,
    NullPointer = 1,
    InvalidUtf8 = 2,
    CompileError = 3,
    RuntimeError = 4,
    TypeError = 5,
    UnknownGlobal = 6,
    UnknownField = 7,
    ArityMismatch = 8,
    InvalidCallTarget = 9,
    InvalidInstruction = 10,
    ReceiveBlocked = 11,
    Halted = 12,
    ProcessNotFound = 13,
    InvalidRestartStrategy = 14,
    LimitExceeded = 15,
    SecurityViolation = 16,
    InvalidBytecode = 17,
    ModuleVerificationFailed = 18,
}

pub type RuaHostCallback = unsafe extern "C" fn(
    user_data: *mut c_void,
    argc: usize,
    argv: *const *const c_char,
) -> *const c_char;

#[repr(C)]
pub struct RuaVmHandle {
    vm: Vm,
    last_error: Option<String>,
    last_error_code: RuaErrorCode,
    last_result: Option<Value>,
}

fn cstr_to_str<'a>(ptr: *const c_char) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err("null pointer".into());
    }

    let c_str = unsafe { CStr::from_ptr(ptr) };
    c_str.to_str().map_err(|_| "invalid UTF-8 string".into())
}

fn to_c_string_ptr(s: String) -> *mut c_char {
    CString::new(s)
        .unwrap_or_else(|_| CString::new("string contains interior NUL byte").expect("literal"))
        .into_raw()
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Integer(v) => v.to_string(),
        Value::Float(v) => v.to_string(),
        Value::String(v) => v.clone(),
        Value::Bool(v) => v.to_string(),
        Value::Nil => "nil".into(),
        Value::Pid(v) => format!("pid({v})"),
        Value::List(_) => "<list>".into(),
        Value::Record(_) => "<record>".into(),
        Value::Closure(_) => "<closure>".into(),
        Value::Builtin(_) => "<builtin>".into(),
    }
}

fn string_to_value(s: &str) -> Value {
    if s == "nil" {
        Value::Nil
    } else if s == "true" {
        Value::Bool(true)
    } else if s == "false" {
        Value::Bool(false)
    } else if let Ok(v) = s.parse::<i64>() {
        Value::Integer(v)
    } else if let Ok(v) = s.parse::<f64>() {
        Value::Float(v)
    } else {
        Value::String(s.to_string())
    }
}

fn eval_source_to_string(source: &str) -> Result<String, String> {
    let module = compile_source(source).map_err(|e| format!("compile error: {e}"))?;
    let mut vm = Vm::new(module);
    let value = vm.run().map_err(|e| format!("runtime error: {e}"))?;
    Ok(vm.format_value(&value))
}

fn vm_error_code(err: &VmError) -> RuaErrorCode {
    match err {
        VmError::TypeError(_) => RuaErrorCode::TypeError,
        VmError::UnknownGlobal(_) => RuaErrorCode::UnknownGlobal,
        VmError::UnknownField(_) => RuaErrorCode::UnknownField,
        VmError::InvalidCallTarget => RuaErrorCode::InvalidCallTarget,
        VmError::ArityMismatch { .. } => RuaErrorCode::ArityMismatch,
        VmError::InvalidInstructionPointer | VmError::InvalidJumpTarget(_) | VmError::FunctionOutOfBounds => {
            RuaErrorCode::InvalidInstruction
        }
        VmError::ReceiveBlocked => RuaErrorCode::ReceiveBlocked,
        VmError::Halted => RuaErrorCode::Halted,
        VmError::ProcessNotFound(_) => RuaErrorCode::ProcessNotFound,
        VmError::InvalidRestartStrategy(_) => RuaErrorCode::InvalidRestartStrategy,
        VmError::LimitExceeded { .. } => RuaErrorCode::LimitExceeded,
        VmError::SecurityViolation(_) => RuaErrorCode::SecurityViolation,
        VmError::InvalidBytecode(_) => RuaErrorCode::InvalidBytecode,
        VmError::ModuleVerificationFailed(_) => RuaErrorCode::ModuleVerificationFailed,
        _ => RuaErrorCode::RuntimeError,
    }
}

fn set_error(handle: &mut RuaVmHandle, code: RuaErrorCode, err: impl Into<String>) {
    handle.last_error = Some(err.into());
    handle.last_error_code = code;
}

fn clear_error(handle: &mut RuaVmHandle) {
    handle.last_error = None;
    handle.last_error_code = RuaErrorCode::None;
}

fn handle_mut<'a>(ptr: *mut RuaVmHandle) -> Result<&'a mut RuaVmHandle, String> {
    if ptr.is_null() {
        return Err("null vm handle".into());
    }
    Ok(unsafe { &mut *ptr })
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_new_from_source(source: *const c_char) -> *mut RuaVmHandle {
    let source = match cstr_to_str(source) {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };

    let module = match compile_source(source) {
        Ok(m) => m,
        Err(_) => return std::ptr::null_mut(),
    };

    Box::into_raw(Box::new(RuaVmHandle {
        vm: Vm::new(module),
        last_error: None,
        last_error_code: RuaErrorCode::None,
        last_result: None,
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_new_from_file(path: *const c_char) -> *mut RuaVmHandle {
    let path = match cstr_to_str(path) {
        Ok(p) => p,
        Err(_) => return std::ptr::null_mut(),
    };

    let source = match fs::read_to_string(path) {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };

    let module = match compile_source(&source) {
        Ok(m) => m,
        Err(_) => return std::ptr::null_mut(),
    };

    Box::into_raw(Box::new(RuaVmHandle {
        vm: Vm::new(module),
        last_error: None,
        last_error_code: RuaErrorCode::None,
        last_result: None,
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_new_from_bytecode_file(path: *const c_char) -> *mut RuaVmHandle {
    let path = match cstr_to_str(path) {
        Ok(p) => p,
        Err(_) => return std::ptr::null_mut(),
    };
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(_) => return std::ptr::null_mut(),
    };
    let module = match decode_module(&bytes) {
        Ok(m) => m,
        Err(_) => return std::ptr::null_mut(),
    };
    Box::into_raw(Box::new(RuaVmHandle {
        vm: Vm::new(module),
        last_error: None,
        last_error_code: RuaErrorCode::None,
        last_result: None,
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_free(vm: *mut RuaVmHandle) {
    if vm.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(vm));
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_run(vm: *mut RuaVmHandle) -> c_int {
    rua_vm_run_status(vm) as c_int
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_run_status(vm: *mut RuaVmHandle) -> RuaStatus {
    let handle = match handle_mut(vm) {
        Ok(h) => h,
        Err(_) => return RuaStatus::Error,
    };

    match handle.vm.run() {
        Ok(v) => {
            handle.last_result = Some(v);
            clear_error(handle);
            RuaStatus::Ok
        }
        Err(e) => {
            match e {
                VmError::Halted => RuaStatus::Halted,
                VmError::ReceiveBlocked => RuaStatus::Blocked,
                other => {
                    let code = vm_error_code(&other);
                    set_error(handle, code, format!("runtime error: {other}"));
                    RuaStatus::Error
                }
            }
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_step(vm: *mut RuaVmHandle) -> c_int {
    rua_vm_step_status(vm) as c_int
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_step_status(vm: *mut RuaVmHandle) -> RuaStatus {
    let handle = match handle_mut(vm) {
        Ok(h) => h,
        Err(_) => return RuaStatus::Error,
    };

    match handle.vm.step() {
        Ok(()) => {
            clear_error(handle);
            RuaStatus::Ok
        }
        Err(e) => {
            match e {
                VmError::Halted => RuaStatus::Halted,
                VmError::ReceiveBlocked => RuaStatus::Blocked,
                other => {
                    let code = vm_error_code(&other);
                    set_error(handle, code, format!("step error: {other}"));
                    RuaStatus::Error
                }
            }
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_step_n(vm: *mut RuaVmHandle, max_steps: usize) -> RuaStatus {
    let handle = match handle_mut(vm) {
        Ok(h) => h,
        Err(_) => return RuaStatus::Error,
    };

    if max_steps == 0 {
        clear_error(handle);
        return RuaStatus::Ok;
    }

    for _ in 0..max_steps {
        match handle.vm.step() {
            Ok(()) => {
                clear_error(handle);
                if handle.vm.state().halted {
                    handle.last_result = handle.vm.state().result.clone();
                    return RuaStatus::Halted;
                }
            }
            Err(VmError::Halted) => return RuaStatus::Halted,
            Err(VmError::ReceiveBlocked) => return RuaStatus::Blocked,
            Err(e) => {
                let code = vm_error_code(&e);
                set_error(handle, code, format!("step error: {e}"));
                return RuaStatus::Error;
            }
        }
    }
    RuaStatus::Ok
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_state_string(vm: *mut RuaVmHandle) -> *mut c_char {
    let handle = match handle_mut(vm) {
        Ok(h) => h,
        Err(err) => return to_c_string_ptr(format!("error:{err}")),
    };
    let s = handle.vm.state();
    to_c_string_ptr(format!(
        "halted={};running={};blocked={};process_count={};ticks={};ready_queue={}",
        s.halted, s.running, s.blocked, s.process_count, s.ticks, s.ready_queue_len
    ))
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_last_error_code(vm: *mut RuaVmHandle) -> RuaErrorCode {
    let handle = match handle_mut(vm) {
        Ok(h) => h,
        Err(_) => return RuaErrorCode::NullPointer,
    };
    handle.last_error_code
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_result_string(vm: *mut RuaVmHandle) -> *mut c_char {
    let handle = match handle_mut(vm) {
        Ok(h) => h,
        Err(err) => return to_c_string_ptr(format!("error:{err}")),
    };

    match &handle.last_result {
        Some(v) => to_c_string_ptr(handle.vm.format_value(v)),
        None => to_c_string_ptr("nil".into()),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_last_error(vm: *mut RuaVmHandle) -> *mut c_char {
    let handle = match handle_mut(vm) {
        Ok(h) => h,
        Err(err) => return to_c_string_ptr(err),
    };

    to_c_string_ptr(handle.last_error.clone().unwrap_or_default())
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_gc_set_threshold(vm: *mut RuaVmHandle, threshold: usize) -> c_int {
    let handle = match handle_mut(vm) {
        Ok(h) => h,
        Err(_) => return 1,
    };
    handle.vm.gc_set_threshold(threshold);
    clear_error(handle);
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_gc_set_full_every_minor(vm: *mut RuaVmHandle, count: usize) -> c_int {
    let handle = match handle_mut(vm) {
        Ok(h) => h,
        Err(_) => return 1,
    };
    handle.vm.gc_set_full_every_minor(count);
    clear_error(handle);
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_gc_collect_now(vm: *mut RuaVmHandle) -> c_int {
    let handle = match handle_mut(vm) {
        Ok(h) => h,
        Err(_) => return 1,
    };
    handle.vm.gc_collect_now();
    clear_error(handle);
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_apply_embedded_profile(vm: *mut RuaVmHandle) -> c_int {
    let handle = match handle_mut(vm) {
        Ok(h) => h,
        Err(_) => return 1,
    };
    handle.vm.set_profile(VmProfile::EmbeddedSmall);
    clear_error(handle);
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_set_limits(
    vm: *mut RuaVmHandle,
    max_steps: usize,
    max_processes: usize,
    max_mailbox_messages: usize,
    max_stack_values: usize,
    max_heap_objects: usize,
) -> c_int {
    let handle = match handle_mut(vm) {
        Ok(h) => h,
        Err(_) => return 1,
    };
    handle.vm.set_limits(
        max_steps,
        max_processes,
        max_mailbox_messages,
        max_stack_values,
        max_heap_objects,
    );
    clear_error(handle);
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_add_module_search_path(
    vm: *mut RuaVmHandle,
    path: *const c_char,
) -> c_int {
    let handle = match handle_mut(vm) {
        Ok(h) => h,
        Err(_) => return 1,
    };
    let p = match cstr_to_str(path) {
        Ok(v) => v,
        Err(e) => {
            set_error(handle, RuaErrorCode::InvalidUtf8, e);
            return 1;
        }
    };
    handle.vm.add_module_search_path(PathBuf::from(p));
    clear_error(handle);
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_set_require_signed_modules(vm: *mut RuaVmHandle, required: c_int) -> c_int {
    let handle = match handle_mut(vm) {
        Ok(h) => h,
        Err(_) => return 1,
    };
    handle.vm.set_require_signed_modules(required != 0);
    clear_error(handle);
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_set_allow_unrestricted_system_ffi(
    vm: *mut RuaVmHandle,
    allowed: c_int,
) -> c_int {
    let handle = match handle_mut(vm) {
        Ok(h) => h,
        Err(_) => return 1,
    };
    handle.vm.set_allow_unrestricted_system_ffi(allowed != 0);
    clear_error(handle);
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_gc_stats(vm: *mut RuaVmHandle) -> *mut c_char {
    let handle = match handle_mut(vm) {
        Ok(h) => h,
        Err(err) => return to_c_string_ptr(format!("error:{err}")),
    };
    let s = handle.vm.gc_telemetry();
    to_c_string_ptr(format!(
        "minor_collections={};full_collections={};total_collected={};total_promoted={};live_objects={}",
        s.minor_collections, s.full_collections, s.total_collected, s.total_promoted, s.live_objects
    ))
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_register_host_fn(
    vm: *mut RuaVmHandle,
    name: *const c_char,
    callback: RuaHostCallback,
    user_data: *mut c_void,
) -> c_int {
    let handle = match handle_mut(vm) {
        Ok(h) => h,
        Err(_) => return 1,
    };

    let name = match cstr_to_str(name) {
        Ok(s) => s.to_string(),
        Err(e) => {
            set_error(handle, RuaErrorCode::InvalidUtf8, e);
            return 1;
        }
    };

    let user_data_addr = user_data as usize;
    handle.vm.register_host_function(name, move |args: &[Value]| {
        let arg_strings = args.iter().map(value_to_string).collect::<Vec<_>>();
        let cstrings = arg_strings
            .iter()
            .map(|s| CString::new(s.as_str()).map_err(|_| VmError::TypeError("invalid ffi arg".into())))
            .collect::<Result<Vec<_>, _>>()?;
        let ptrs = cstrings.iter().map(|s| s.as_ptr()).collect::<Vec<_>>();

        let ret_ptr = unsafe { callback(user_data_addr as *mut c_void, ptrs.len(), ptrs.as_ptr()) };
        if ret_ptr.is_null() {
            return Ok(Value::Nil);
        }

        let out = unsafe { CStr::from_ptr(ret_ptr) }
            .to_str()
            .map_err(|_| VmError::TypeError("host function returned invalid UTF-8".into()))?
            .to_string();

        if let Some(err) = out.strip_prefix("error:") {
            Err(VmError::TypeError(err.to_string()))
        } else {
            Ok(string_to_value(&out))
        }
    });

    clear_error(handle);
    0
}

fn parse_ffi_type(name: &str) -> Option<FfiType> {
    match name {
        "i64" => Some(FfiType::Int64),
        "u64" => Some(FfiType::UInt64),
        "bool" => Some(FfiType::Bool),
        "cstring" => Some(FfiType::CString),
        "ptr" => Some(FfiType::Ptr),
        "void" => Some(FfiType::Void),
        _ => None,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_register_system_ffi_capability(
    vm: *mut RuaVmHandle,
    cap_name: *const c_char,
    lib_name: *const c_char,
    symbol_name: *const c_char,
    ret_type: *const c_char,
    param_types_csv: *const c_char,
) -> c_int {
    let handle = match handle_mut(vm) {
        Ok(h) => h,
        Err(_) => return 1,
    };
    let cap_name = match cstr_to_str(cap_name) {
        Ok(v) => v,
        Err(e) => {
            set_error(handle, RuaErrorCode::InvalidUtf8, e);
            return 1;
        }
    };
    let lib_name = match cstr_to_str(lib_name) {
        Ok(v) => v,
        Err(e) => {
            set_error(handle, RuaErrorCode::InvalidUtf8, e);
            return 1;
        }
    };
    let symbol_name = match cstr_to_str(symbol_name) {
        Ok(v) => v,
        Err(e) => {
            set_error(handle, RuaErrorCode::InvalidUtf8, e);
            return 1;
        }
    };
    let ret = match cstr_to_str(ret_type).ok().and_then(parse_ffi_type) {
        Some(v) => v,
        None => {
            set_error(handle, RuaErrorCode::TypeError, "invalid ffi return type");
            return 1;
        }
    };
    let params_csv = match cstr_to_str(param_types_csv) {
        Ok(v) => v,
        Err(e) => {
            set_error(handle, RuaErrorCode::InvalidUtf8, e);
            return 1;
        }
    };
    let mut params = Vec::new();
    for part in params_csv.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        let Some(ty) = parse_ffi_type(part) else {
            set_error(handle, RuaErrorCode::TypeError, format!("invalid ffi param type: {part}"));
            return 1;
        };
        params.push(ty);
    }
    handle.vm.register_system_ffi_capability(
        cap_name.to_string(),
        lib_name.to_string(),
        symbol_name.to_string(),
        FfiSignature { params, ret },
    );
    clear_error(handle);
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_vm_register_native_module_source(
    vm: *mut RuaVmHandle,
    name: *const c_char,
    source: *const c_char,
) -> c_int {
    let handle = match handle_mut(vm) {
        Ok(h) => h,
        Err(_) => return 1,
    };
    let name = match cstr_to_str(name) {
        Ok(v) => v.to_string(),
        Err(e) => {
            set_error(handle, RuaErrorCode::InvalidUtf8, e);
            return 1;
        }
    };
    let source = match cstr_to_str(source) {
        Ok(v) => v,
        Err(e) => {
            set_error(handle, RuaErrorCode::InvalidUtf8, e);
            return 1;
        }
    };

    let module = match compile_source(source) {
        Ok(m) => m,
        Err(e) => {
            set_error(handle, RuaErrorCode::CompileError, format!("compile error: {e}"));
            return 1;
        }
    };
    let mut module_vm = Vm::new(module);
    let value = match module_vm.run() {
        Ok(v) => v,
        Err(e) => {
            set_error(
                handle,
                vm_error_code(&e),
                format!("module runtime error: {e}"),
            );
            return 1;
        }
    };
    if !matches!(
        value,
        Value::Integer(_) | Value::Float(_) | Value::String(_) | Value::Bool(_) | Value::Nil
    ) {
        set_error(
            handle,
            RuaErrorCode::RuntimeError,
            "module source must return scalar value in C API v0.1",
        );
        return 1;
    }
    handle.vm.register_native_module(name, value);
    clear_error(handle);
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_eval_source(source: *const c_char) -> *mut c_char {
    match cstr_to_str(source).and_then(eval_source_to_string) {
        Ok(result) => to_c_string_ptr(result),
        Err(err) => to_c_string_ptr(format!("error:{err}")),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_eval_file(path: *const c_char) -> *mut c_char {
    let result = cstr_to_str(path)
        .and_then(|p| fs::read_to_string(p).map_err(|e| format!("read error: {e}")))
        .and_then(|source| eval_source_to_string(&source));

    match result {
        Ok(output) => to_c_string_ptr(output),
        Err(err) => to_c_string_ptr(format!("error:{err}")),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn rua_string_free(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        drop(CString::from_raw(ptr));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eval_source_works() {
        let out = eval_source_to_string("40 + 2").unwrap();
        assert_eq!(out, "42");
    }

    #[test]
    fn step_n_reports_halted() {
        let src = CString::new("40 + 2").unwrap();
        let vm = rua_vm_new_from_source(src.as_ptr());
        assert!(!vm.is_null());
        let status = rua_vm_step_n(vm, 10_000);
        assert!(matches!(status, RuaStatus::Halted | RuaStatus::Ok));
        rua_vm_free(vm);
    }
}
