use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ffi::{c_char, c_void, CString};
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use rua::ast::{BinaryOp, UnaryOp};
use rua::bytecode::{decode_module, validate_module};
use rua::frontend::compile_source;
use rua::ir::{
    CaptureRef, ConstId, Constant, Function, FunctionId, Instr, Module, Pattern, ReceiveAfter,
    ReceiveCase,
};
#[cfg(feature = "builtin_sha256_verify")]
use sha2::{Digest, Sha256};

use crate::error::VmError;
use crate::gc::{GcConfig, GcProfile, GcTelemetry, Heap, HeapObject};
use crate::value::{Builtin, Value};

pub type ProcessId = u64;
type MonitorRef = u64;
type HostFunction = dyn Fn(&[Value]) -> Result<Value, VmError>;
type ModuleLoader = dyn Fn() -> Result<Value, VmError>;
type ModuleVerifier = dyn Fn(&str, &[u8], &[u8]) -> bool;

#[cfg(unix)]
const RTLD_NOW: i32 = 2;

#[cfg(unix)]
unsafe extern "C" {
    fn dlopen(filename: *const c_char, flags: i32) -> *mut c_void;
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    fn dlclose(handle: *mut c_void) -> i32;
}

struct NativeLibrary {
    handle: *mut c_void,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VmProfile {
    EmbeddedSmall,
    Balanced,
    Throughput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FfiType {
    Int64,
    UInt64,
    Bool,
    CString,
    Ptr,
    Void,
}

#[derive(Debug, Clone)]
pub struct FfiSignature {
    pub params: Vec<FfiType>,
    pub ret: FfiType,
}

#[derive(Debug, Clone)]
struct SystemFfiCapability {
    lib: String,
    symbol: String,
    signature: FfiSignature,
}

#[derive(Debug, Clone)]
pub struct VmFrameTrace {
    pub function: Option<String>,
    pub function_id: usize,
    pub ip: usize,
}

#[derive(Debug, Clone)]
pub struct VmDiagnostic {
    pub code: &'static str,
    pub message: String,
    pub process_id: ProcessId,
    pub stack_trace: Vec<VmFrameTrace>,
}

#[derive(Debug, Clone)]
pub struct VmConfig {
    pub max_steps: usize,
    pub gc_slice_budget: usize,
    pub timeslice_instructions: usize,
    pub max_processes: usize,
    pub max_mailbox_messages: usize,
    pub max_stack_values: usize,
    pub max_call_depth: usize,
    pub max_heap_objects: usize,
    pub max_module_bytes: usize,
    pub max_module_cache_entries: usize,
    pub allow_unrestricted_system_ffi: bool,
    pub require_signed_modules: bool,
    pub module_search_paths: Vec<PathBuf>,
}

impl Default for VmConfig {
    fn default() -> Self {
        Self {
            max_steps: 1_000_000,
            gc_slice_budget: 128,
            timeslice_instructions: 64,
            max_processes: 4096,
            max_mailbox_messages: 50_000,
            max_stack_values: 100_000,
            max_call_depth: 2048,
            max_heap_objects: 1_000_000,
            max_module_bytes: 4 * 1024 * 1024,
            max_module_cache_entries: 1024,
            allow_unrestricted_system_ffi: false,
            require_signed_modules: false,
            module_search_paths: vec![PathBuf::from(".")],
        }
    }
}

#[derive(Debug, Clone)]
pub struct VmState {
    pub halted: bool,
    pub result: Option<Value>,
    pub running: usize,
    pub blocked: usize,
    pub process_count: usize,
    pub ticks: u64,
    pub ready_queue_len: usize,
}

#[derive(Debug, Clone)]
struct CallFrame {
    function: FunctionId,
    ip: usize,
    locals: Vec<Value>,
    upvalues: Vec<Value>,
    self_closure: Option<Value>,
}

#[derive(Debug, Clone)]
struct Process {
    stack: Vec<Value>,
    call_stack: Vec<CallFrame>,
    mailbox: VecDeque<Value>,
    unsafe_depth: usize,
    blocked: bool,
    halted: bool,
    result: Option<Value>,
    waiting_receive: Option<ReceiveWaitState>,
}

#[derive(Debug, Clone)]
enum ReceiveWaitStage {
    EvaluatingTimeout,
    WaitingUntil(u64),
}

#[derive(Debug, Clone)]
struct ReceiveWaitState {
    frame_idx: usize,
    instr_ip: usize,
    body_handler: FunctionId,
    upvalues: Vec<Value>,
    stage: ReceiveWaitStage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestartPolicy {
    Temporary,
    Transient,
    Permanent,
}

#[derive(Debug, Clone)]
struct SupervisorChild {
    supervisor: ProcessId,
    closure: Value,
    policy: RestartPolicy,
    max_restarts: usize,
    window_ticks: u64,
    restart_ticks: VecDeque<u64>,
}

#[derive(Debug, Clone)]
struct Monitor {
    watcher: ProcessId,
    target: ProcessId,
    ref_id: MonitorRef,
}

pub struct Vm {
    module: Rc<Module>,
    config: VmConfig,
    heap: Heap,
    globals: HashMap<String, Value>,
    host_functions: HashMap<String, Box<HostFunction>>,
    native_modules: HashMap<String, Value>,
    module_loaders: HashMap<String, Box<ModuleLoader>>,
    module_verifier: Option<Box<ModuleVerifier>>,
    ffi_libs: HashMap<String, NativeLibrary>,
    ffi_caps: HashMap<String, SystemFfiCapability>,
    processes: HashMap<ProcessId, Process>,
    run_queue: VecDeque<ProcessId>,
    queued: HashSet<ProcessId>,
    links: HashMap<ProcessId, HashSet<ProcessId>>,
    monitors_by_target: HashMap<ProcessId, Vec<Monitor>>,
    monitors_by_watcher: HashMap<ProcessId, Vec<Monitor>>,
    next_monitor_ref: MonitorRef,
    registry: HashMap<String, ProcessId>,
    supervision: HashMap<ProcessId, SupervisorChild>,
    main_pid: ProcessId,
    next_pid: ProcessId,
    ticks: u64,
    halted: bool,
    result: Option<Value>,
    last_diagnostic: Option<VmDiagnostic>,
}

impl Vm {
    pub fn new(module: Module) -> Self {
        Self::with_config(module, VmConfig::default())
    }

    pub fn with_config(module: Module, config: VmConfig) -> Self {
        let module = Rc::new(module);
        let main_pid = 0;

        let main_process = Process {
            stack: Vec::new(),
            call_stack: vec![CallFrame {
                function: module.entry,
                ip: 0,
                locals: Vec::new(),
                upvalues: Vec::new(),
                self_closure: None,
            }],
            mailbox: VecDeque::new(),
            unsafe_depth: 0,
            blocked: false,
            halted: false,
            result: None,
            waiting_receive: None,
        };

        let mut vm = Self {
            module,
            config,
            heap: Heap::new(GcConfig::default()),
            globals: HashMap::new(),
            host_functions: HashMap::new(),
            native_modules: HashMap::new(),
            module_loaders: HashMap::new(),
            module_verifier: None,
            ffi_libs: HashMap::new(),
            ffi_caps: HashMap::new(),
            processes: HashMap::from([(main_pid, main_process)]),
            run_queue: VecDeque::from([main_pid]),
            queued: HashSet::from([main_pid]),
            links: HashMap::new(),
            monitors_by_target: HashMap::new(),
            monitors_by_watcher: HashMap::new(),
            next_monitor_ref: 1,
            registry: HashMap::new(),
            supervision: HashMap::new(),
            main_pid,
            next_pid: 1,
            ticks: 0,
            halted: false,
            result: None,
            last_diagnostic: None,
        };
        vm.install_builtins();
        vm.gc_set_threshold(1024);
        vm
    }

    pub fn register_host_function<F>(&mut self, name: impl Into<String>, callback: F)
    where
        F: Fn(&[Value]) -> Result<Value, VmError> + 'static,
    {
        self.host_functions.insert(name.into(), Box::new(callback));
    }

    pub fn register_native_module(&mut self, name: impl Into<String>, module: Value) {
        self.native_modules.insert(name.into(), module);
    }

    pub fn register_module_loader<F>(&mut self, name: impl Into<String>, loader: F)
    where
        F: Fn() -> Result<Value, VmError> + 'static,
    {
        self.module_loaders.insert(name.into(), Box::new(loader));
    }

    pub fn register_module_verifier<F>(&mut self, verifier: F)
    where
        F: Fn(&str, &[u8], &[u8]) -> bool + 'static,
    {
        self.module_verifier = Some(Box::new(verifier));
    }

    pub fn register_system_ffi_capability(
        &mut self,
        name: impl Into<String>,
        lib: impl Into<String>,
        symbol: impl Into<String>,
        signature: FfiSignature,
    ) {
        self.ffi_caps.insert(
            name.into(),
            SystemFfiCapability {
                lib: lib.into(),
                symbol: symbol.into(),
                signature,
            },
        );
    }

    pub fn set_profile(&mut self, profile: VmProfile) {
        match profile {
            VmProfile::EmbeddedSmall => {
                self.config.gc_slice_budget = 64;
                self.heap.set_profile(GcProfile::LowLatency);
            }
            VmProfile::Balanced => {
                self.config.gc_slice_budget = 128;
                self.heap.set_profile(GcProfile::Balanced);
            }
            VmProfile::Throughput => {
                self.config.gc_slice_budget = 512;
                self.heap.set_profile(GcProfile::Throughput);
            }
        }
    }

    pub fn set_limits(
        &mut self,
        max_steps: usize,
        max_processes: usize,
        max_mailbox_messages: usize,
        max_stack_values: usize,
        max_heap_objects: usize,
    ) {
        self.config.max_steps = max_steps.max(1);
        self.config.max_processes = max_processes.max(1);
        self.config.max_mailbox_messages = max_mailbox_messages.max(1);
        self.config.max_stack_values = max_stack_values.max(1);
        self.config.max_heap_objects = max_heap_objects.max(1);
    }

    pub fn add_module_search_path(&mut self, path: impl Into<PathBuf>) {
        self.config.module_search_paths.push(path.into());
    }

    pub fn set_require_signed_modules(&mut self, required: bool) {
        self.config.require_signed_modules = required;
    }

    pub fn set_allow_unrestricted_system_ffi(&mut self, allowed: bool) {
        self.config.allow_unrestricted_system_ffi = allowed;
    }

    pub fn gc_set_threshold(&mut self, threshold: usize) {
        self.heap.set_threshold(threshold);
    }

    pub fn gc_set_full_every_minor(&mut self, count: usize) {
        self.heap.set_full_every_minor(count);
    }

    pub fn gc_set_profile(&mut self, profile: GcProfile) {
        self.heap.set_profile(profile);
    }

    pub fn gc_collect_now(&mut self) {
        let roots = self.collect_roots();
        let _ = self.heap.collect_full(roots.iter());
    }

    pub fn gc_telemetry(&self) -> &GcTelemetry {
        self.heap.telemetry()
    }

    pub fn last_diagnostic(&self) -> Option<&VmDiagnostic> {
        self.last_diagnostic.as_ref()
    }

    pub fn state(&self) -> VmState {
        let running = self
            .processes
            .values()
            .filter(|p| !p.halted && !p.blocked)
            .count();
        let blocked = self
            .processes
            .values()
            .filter(|p| !p.halted && p.blocked)
            .count();
        VmState {
            halted: self.halted,
            result: self.result.clone(),
            running,
            blocked,
            process_count: self.processes.len(),
            ticks: self.ticks,
            ready_queue_len: self.run_queue.len(),
        }
    }

    pub fn run(&mut self) -> Result<Value, VmError> {
        for _ in 0..self.config.max_steps {
            if self.halted {
                return self.result.clone().ok_or(VmError::InvalidInstructionPointer);
            }
            self.step()?;
        }
        self.fail(VmError::LimitExceeded {
            limit: "max_steps",
            max: self.config.max_steps,
        })
    }

    pub fn step(&mut self) -> Result<(), VmError> {
        if self.halted {
            return Err(VmError::Halted);
        }

        self.wake_timed_out_processes();

        let Some(pid) = self.run_queue.pop_front() else {
            if let Some(deadline) = self.next_timeout_deadline()
                && deadline >= self.ticks
            {
                self.ticks = deadline;
                self.wake_timed_out_processes();
                return Ok(());
            }
            if self.processes.values().all(|p| p.halted || p.blocked) {
                return Err(VmError::ReceiveBlocked);
            }
            return Ok(());
        };
        self.queued.remove(&pid);

        if self.processes.get(&pid).map(|p| p.halted).unwrap_or(true) {
            return Ok(());
        }

        let mut step_result = Ok(());
        let quantum = self.config.timeslice_instructions.max(1);
        for _ in 0..quantum {
            self.ticks = self.ticks.saturating_add(1);
            step_result = self.exec_one(pid);
            if step_result.is_err() {
                break;
            }
            self.collect_garbage_if_needed();
            if let Err(e) = self.enforce_limits() {
                step_result = Err(e);
                break;
            }
            let keep_running = self
                .processes
                .get(&pid)
                .map(|p| !p.halted && !p.blocked)
                .unwrap_or(false);
            if !keep_running {
                break;
            }
        }

        if self
            .processes
            .get(&pid)
            .map(|p| !p.halted && !p.blocked)
            .unwrap_or(false)
        {
            self.enqueue_ready(pid);
        }

        match step_result {
            Ok(()) => Ok(()),
            Err(e) => self.fail(e),
        }
    }

    pub fn format_value(&self, value: &Value) -> String {
        self.format_value_inner(value, 0)
    }

    fn format_value_inner(&self, value: &Value, depth: usize) -> String {
        if depth > 10 {
            return "<depth-limit>".into();
        }

        match value {
            Value::Integer(v) => v.to_string(),
            Value::Float(v) => v.to_string(),
            Value::String(v) => v.clone(),
            Value::Bool(v) => v.to_string(),
            Value::Nil => "nil".into(),
            Value::Pid(v) => format!("pid({v})"),
            Value::List(id) => match self.heap.get(*id) {
                Some(HeapObject::List(items)) => {
                    let inner = items
                        .iter()
                        .map(|v| self.format_value_inner(v, depth + 1))
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("{{{inner}}}")
                }
                _ => "<dangling-list>".into(),
            },
            Value::Record(id) => match self.heap.get(*id) {
                Some(HeapObject::Record { fields, .. }) => {
                    let inner = fields
                        .iter()
                        .map(|(k, v)| format!("{k} = {}", self.format_value_inner(v, depth + 1)))
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("{{ {inner} }}")
                }
                _ => "<dangling-record>".into(),
            },
            Value::Closure(_) => "<closure>".into(),
            Value::Builtin(_) => "<builtin>".into(),
        }
    }

    fn exec_one(&mut self, pid: ProcessId) -> Result<(), VmError> {
        let frame_idx = {
            let proc = self.process(pid)?;
            proc.call_stack
                .len()
                .checked_sub(1)
                .ok_or(VmError::InvalidInstructionPointer)?
        };

        let instr = {
            let proc = self.process(pid)?;
            let frame = &proc.call_stack[frame_idx];
            let function = self.function(frame.function)?;
            function
                .code
                .get(frame.ip)
                .cloned()
                .ok_or(VmError::InvalidInstructionPointer)?
        };

        self.process_mut(pid)?.call_stack[frame_idx].ip += 1;

        match instr {
            Instr::LoadConst(id) => {
                let value = self.load_constant(self.current_function(pid)?, id)?;
                self.process_mut(pid)?.stack.push(value);
            }
            Instr::PushBool(v) => self.process_mut(pid)?.stack.push(Value::Bool(v)),
            Instr::PushNil => self.process_mut(pid)?.stack.push(Value::Nil),
            Instr::LoadLocal(id) => {
                let value = self.process(pid)?.call_stack[frame_idx]
                    .locals
                    .get(id.0)
                    .cloned()
                    .unwrap_or(Value::Nil);
                self.process_mut(pid)?.stack.push(value);
            }
            Instr::LoadUpvalue(id) => {
                let value = self.process(pid)?.call_stack[frame_idx]
                    .upvalues
                    .get(id.0)
                    .cloned()
                    .unwrap_or(Value::Nil);
                self.process_mut(pid)?.stack.push(value);
            }
            Instr::LoadSelf => {
                let value = self.process(pid)?.call_stack[frame_idx]
                    .self_closure
                    .clone()
                    .unwrap_or(Value::Nil);
                self.process_mut(pid)?.stack.push(value);
            }
            Instr::LoadGlobal(id) => {
                let name = self.const_symbol(self.current_function(pid)?, id)?;
                let value = self
                    .globals
                    .get(&name)
                    .cloned()
                    .ok_or(VmError::UnknownGlobal(name))?;
                self.process_mut(pid)?.stack.push(value);
            }
            Instr::BindLocal(id) => {
                let value = self.pop(pid)?;
                self.set_local(pid, frame_idx, id.0, value)?;
            }
            Instr::Unary(op) => self.exec_unary(pid, op)?,
            Instr::Binary(op) => self.exec_binary(pid, op)?,
            Instr::MakeList(len) => {
                let mut items = Vec::with_capacity(len);
                for _ in 0..len {
                    items.push(self.pop(pid)?);
                }
                items.reverse();
                let value = self.heap.alloc_list(items);
                self.process_mut(pid)?.stack.push(value);
            }
            Instr::MakeRecord(fields) => {
                let function_id = self.current_function(pid)?;
                let mut map = BTreeMap::new();
                for field in fields.iter().rev() {
                    let value = self.pop(pid)?;
                    let key = self.const_symbol(function_id, *field)?;
                    map.insert(key, value);
                }
                let value = self.heap.alloc_record(map, None);
                self.process_mut(pid)?.stack.push(value);
            }
            Instr::RecordUpdate(fields) => {
                let function_id = self.current_function(pid)?;
                let mut updates = Vec::with_capacity(fields.len());
                for field in fields.iter().rev() {
                    let value = self.pop(pid)?;
                    let key = self.const_symbol(function_id, *field)?;
                    updates.push((key, value));
                }
                let base = self.pop(pid)?;
                let (base_fields, base_meta) = self.record_parts(&base)?;
                let mut new_fields = base_fields;
                for (k, v) in updates {
                    new_fields.insert(k, v);
                }
                let value = self.heap.alloc_record(new_fields, base_meta);
                self.process_mut(pid)?.stack.push(value);
            }
            Instr::GetField(field) => {
                let key = self.const_symbol(self.current_function(pid)?, field)?;
                let value = self.pop(pid)?;
                let field_value = self.get_field(value, &key)?;
                self.process_mut(pid)?.stack.push(field_value);
            }
            Instr::Call(argc) => self.exec_call(pid, argc)?,
            Instr::MakeClosure { function, captures } => {
                let captured = {
                    let proc = self.process(pid)?;
                    captures
                        .into_iter()
                        .map(|capture| match capture {
                            CaptureRef::Local(id) => proc.call_stack[frame_idx]
                                .locals
                                .get(id.0)
                                .cloned()
                                .unwrap_or(Value::Nil),
                            CaptureRef::Upvalue(id) => proc.call_stack[frame_idx]
                                .upvalues
                                .get(id.0)
                                .cloned()
                                .unwrap_or(Value::Nil),
                            CaptureRef::SelfClosure => proc.call_stack[frame_idx]
                                .self_closure
                                .clone()
                                .unwrap_or(Value::Nil),
                        })
                        .collect::<Vec<_>>()
                };
                let value = self.heap.alloc_closure(function, captured);
                self.process_mut(pid)?.stack.push(value);
            }
            Instr::UnsafeBegin => self.process_mut(pid)?.unsafe_depth += 1,
            Instr::UnsafeEnd => {
                let proc = self.process_mut(pid)?;
                proc.unsafe_depth = proc.unsafe_depth.saturating_sub(1);
            }
            Instr::Receive { cases, after } => self.exec_receive(pid, frame_idx, cases, after)?,
            Instr::JumpIfFalse(target) => {
                let function_id = self.current_function(pid)?;
                if target >= self.function(function_id)?.code.len() {
                    return Err(VmError::InvalidJumpTarget(target));
                }
                let cond = self.peek(pid)?.truthy().map_err(VmError::TypeError)?;
                if !cond {
                    self.process_mut(pid)?.call_stack[frame_idx].ip = target;
                }
            }
            Instr::Jump(target) => {
                let function_id = self.current_function(pid)?;
                if target >= self.function(function_id)?.code.len() {
                    return Err(VmError::InvalidJumpTarget(target));
                }
                self.process_mut(pid)?.call_stack[frame_idx].ip = target;
            }
            Instr::Pop => {
                let _ = self.pop(pid)?;
            }
            Instr::Return => {
                let value = self.pop(pid).unwrap_or(Value::Nil);
                let is_last_frame = {
                    let proc = self.process_mut(pid)?;
                    proc.call_stack.pop();
                    proc.call_stack.is_empty()
                };
                if is_last_frame {
                    self.terminate_process(pid, value)?;
                } else {
                    self.process_mut(pid)?.stack.push(value);
                }
            }
        }

        Ok(())
    }

    fn exec_unary(&mut self, pid: ProcessId, op: UnaryOp) -> Result<(), VmError> {
        let value = self.pop(pid)?;
        let out = match op {
            UnaryOp::Neg => match value {
                Value::Integer(v) => Value::Integer(-v),
                Value::Float(v) => Value::Float(-v),
                _ => return Err(VmError::TypeError("negation expects numeric value".into())),
            },
            UnaryOp::Not => Value::Bool(!value.truthy().map_err(VmError::TypeError)?),
        };
        self.process_mut(pid)?.stack.push(out);
        Ok(())
    }

    fn exec_binary(&mut self, pid: ProcessId, op: BinaryOp) -> Result<(), VmError> {
        let right = self.pop(pid)?;
        let left = self.pop(pid)?;

        let out = match op {
            BinaryOp::Add => self.num_bin(left, right, |a, b| a + b, |a, b| a + b)?,
            BinaryOp::Sub => self.num_bin(left, right, |a, b| a - b, |a, b| a - b)?,
            BinaryOp::Mul => self.num_bin(left, right, |a, b| a * b, |a, b| a * b)?,
            BinaryOp::Div => self.num_bin(left, right, |a, b| a / b, |a, b| a / b)?,
            BinaryOp::Mod => match (left, right) {
                (Value::Integer(a), Value::Integer(b)) => Value::Integer(a % b),
                _ => return Err(VmError::TypeError("mod expects integers".into())),
            },
            BinaryOp::Concat => {
                Value::String(format!("{}{}", left.to_concat_string(), right.to_concat_string()))
            }
            BinaryOp::Eq => Value::Bool(left == right),
            BinaryOp::Ne => Value::Bool(left != right),
            BinaryOp::Lt => self.cmp_bin(left, right, |a, b| a < b)?,
            BinaryOp::Le => self.cmp_bin(left, right, |a, b| a <= b)?,
            BinaryOp::Gt => self.cmp_bin(left, right, |a, b| a > b)?,
            BinaryOp::Ge => self.cmp_bin(left, right, |a, b| a >= b)?,
            BinaryOp::And | BinaryOp::Or => {
                return Err(VmError::TypeError("and/or are lowered into jumps".into()));
            }
        };

        self.process_mut(pid)?.stack.push(out);
        Ok(())
    }

    fn exec_call(&mut self, pid: ProcessId, argc: usize) -> Result<(), VmError> {
        let mut args = Vec::with_capacity(argc);
        for _ in 0..argc {
            args.push(self.pop(pid)?);
        }
        args.reverse();
        let callee = self.pop(pid)?;

        match callee {
            Value::Closure(id) => self.call_closure(pid, id, args),
            Value::Builtin(builtin) => {
                let result = self.call_builtin(pid, builtin, args)?;
                self.process_mut(pid)?.stack.push(result);
                Ok(())
            }
            _ => Err(VmError::InvalidCallTarget),
        }
    }

    fn call_closure(&mut self, pid: ProcessId, closure_id: crate::value::ObjRef, args: Vec<Value>) -> Result<(), VmError> {
        let (function_id, captures) = match self.heap.get(closure_id) {
            Some(HeapObject::Closure { function, captures }) => (*function, captures.clone()),
            _ => return Err(VmError::TypeError("invalid closure object".into())),
        };

        let function = self.function(function_id)?;
        if args.len() != function.arity {
            return Err(VmError::ArityMismatch {
                expected: function.arity,
                got: args.len(),
            });
        }

        let mut locals = vec![Value::Nil; function.arity.max(8)];
        for (i, arg) in args.into_iter().enumerate() {
            locals[i] = arg;
        }

        if self.process(pid)?.call_stack.len() >= self.config.max_call_depth {
            return Err(VmError::LimitExceeded {
                limit: "max_call_depth",
                max: self.config.max_call_depth,
            });
        }

        self.process_mut(pid)?.call_stack.push(CallFrame {
            function: function_id,
            ip: 0,
            locals,
            upvalues: captures,
            self_closure: Some(Value::Closure(closure_id)),
        });
        Ok(())
    }

    fn call_function_with_upvalues(
        &mut self,
        pid: ProcessId,
        function_id: FunctionId,
        args: Vec<Value>,
        upvalues: Vec<Value>,
    ) -> Result<(), VmError> {
        let function = self.function(function_id)?;
        if args.len() != function.arity {
            return Err(VmError::ArityMismatch {
                expected: function.arity,
                got: args.len(),
            });
        }

        let mut locals = vec![Value::Nil; function.arity.max(8)];
        for (i, arg) in args.into_iter().enumerate() {
            locals[i] = arg;
        }

        let max_call_depth = self.config.max_call_depth;
        let proc = self.process_mut(pid)?;
        if proc.call_stack.len() >= max_call_depth {
            return Err(VmError::LimitExceeded {
                limit: "max_call_depth",
                max: max_call_depth,
            });
        }
        proc.call_stack.push(CallFrame {
            function: function_id,
            ip: 0,
            locals,
            upvalues,
            self_closure: None,
        });
        proc.blocked = false;
        Ok(())
    }

    fn exec_receive(
        &mut self,
        pid: ProcessId,
        frame_idx: usize,
        cases: Vec<ReceiveCase>,
        after: Option<ReceiveAfter>,
    ) -> Result<(), VmError> {
        let current_function = self.current_function(pid)?;
        let receive_ip = self.process(pid)?.call_stack[frame_idx].ip.saturating_sub(1);
        let mailbox_snapshot = self.process(pid)?.mailbox.iter().cloned().collect::<Vec<_>>();

        for (msg_idx, message) in mailbox_snapshot.iter().enumerate() {
            for case in &cases {
                let mut bindings = Vec::new();
                if self.match_pattern(current_function, &case.pattern, message, &mut bindings)? {
                    let proc = self.process_mut(pid)?;
                    proc.mailbox.remove(msg_idx);
                    proc.waiting_receive = None;
                    proc.blocked = false;
                    let upvalues = self.capture_values(pid, frame_idx, &case.captures)?;
                    self.call_function_with_upvalues(pid, case.handler, bindings, upvalues)?;
                    return Ok(());
                }
            }
        }

        if let Some(after_case) = after {
            let wait_state = self.process(pid)?.waiting_receive.clone();
            if let Some(state) = wait_state
                && state.frame_idx == frame_idx
                && state.instr_ip == receive_ip
            {
                match state.stage {
                    ReceiveWaitStage::EvaluatingTimeout => {
                        let timeout_value = self.pop(pid)?;
                        let timeout = self.timeout_ticks_from_value(timeout_value)?;
                        if timeout == 0 {
                            self.process_mut(pid)?.waiting_receive = None;
                            self.call_function_with_upvalues(
                                pid,
                                state.body_handler,
                                Vec::new(),
                                state.upvalues,
                            )?;
                            return Ok(());
                        }
                        let deadline = self.ticks.saturating_add(timeout);
                        let proc = self.process_mut(pid)?;
                        proc.waiting_receive = Some(ReceiveWaitState {
                            frame_idx,
                            instr_ip: receive_ip,
                            body_handler: state.body_handler,
                            upvalues: state.upvalues,
                            stage: ReceiveWaitStage::WaitingUntil(deadline),
                        });
                        proc.blocked = true;
                        proc.call_stack[frame_idx].ip = proc.call_stack[frame_idx].ip.saturating_sub(1);
                        return Ok(());
                    }
                    ReceiveWaitStage::WaitingUntil(deadline) => {
                        if self.ticks >= deadline {
                            self.process_mut(pid)?.waiting_receive = None;
                            self.call_function_with_upvalues(
                                pid,
                                state.body_handler,
                                Vec::new(),
                                state.upvalues,
                            )?;
                            return Ok(());
                        }
                        let proc = self.process_mut(pid)?;
                        proc.blocked = true;
                        proc.call_stack[frame_idx].ip = proc.call_stack[frame_idx].ip.saturating_sub(1);
                        return Ok(());
                    }
                }
            }

            self.process_mut(pid)?.waiting_receive = Some(ReceiveWaitState {
                frame_idx,
                instr_ip: receive_ip,
                body_handler: after_case.body_handler,
                upvalues: self.capture_values(pid, frame_idx, &after_case.body_captures)?,
                stage: ReceiveWaitStage::EvaluatingTimeout,
            });
            {
                let proc = self.process_mut(pid)?;
                proc.call_stack[frame_idx].ip = proc.call_stack[frame_idx].ip.saturating_sub(1);
            }
            let timeout_upvalues = self.capture_values(pid, frame_idx, &after_case.timeout_captures)?;
            self.call_function_with_upvalues(pid, after_case.timeout_handler, Vec::new(), timeout_upvalues)?;
            return Ok(());
        }

        let proc = self.process_mut(pid)?;
        proc.waiting_receive = None;
        proc.blocked = true;
        proc.call_stack[frame_idx].ip = proc.call_stack[frame_idx].ip.saturating_sub(1);
        Ok(())
    }

    fn match_pattern(
        &self,
        current_function: FunctionId,
        pattern: &Pattern,
        value: &Value,
        bindings: &mut Vec<Value>,
    ) -> Result<bool, VmError> {
        match pattern {
            Pattern::Wildcard => Ok(true),
            Pattern::Binding => {
                bindings.push(value.clone());
                Ok(true)
            }
            Pattern::Literal(id) => {
                let literal = self.load_constant(current_function, *id)?;
                Ok(literal == *value)
            }
            Pattern::Bool(v) => Ok(*value == Value::Bool(*v)),
            Pattern::Nil => Ok(matches!(value, Value::Nil)),
            Pattern::Record(fields) => {
                let record_fields = match value {
                    Value::Record(id) => match self.heap.get(*id) {
                        Some(HeapObject::Record { fields, .. }) => fields,
                        _ => return Ok(false),
                    },
                    _ => return Ok(false),
                };

                for (key_id, subpattern) in fields {
                    let key = self.const_symbol(current_function, *key_id)?;
                    let Some(field_val) = record_fields.get(&key) else {
                        return Ok(false);
                    };
                    if !self.match_pattern(current_function, subpattern, field_val, bindings)? {
                        return Ok(false);
                    }
                }

                Ok(true)
            }
        }
    }

    fn call_builtin(
        &mut self,
        pid: ProcessId,
        builtin: Builtin,
        args: Vec<Value>,
    ) -> Result<Value, VmError> {
        match builtin {
            Builtin::Print => Ok(Value::Nil),
            Builtin::SelfPid => {
                if !args.is_empty() {
                    return Err(VmError::ArityMismatch {
                        expected: 0,
                        got: args.len(),
                    });
                }
                Ok(Value::Pid(pid))
            }
            Builtin::Send => {
                if args.len() != 2 {
                    return Err(VmError::ArityMismatch {
                        expected: 2,
                        got: args.len(),
                    });
                }
                let target = match &args[0] {
                    Value::Pid(pid) => *pid,
                    _ => return Err(VmError::TypeError("send expects pid as first arg".into())),
                };
                let msg = args[1].clone();
                let target_proc = self
                    .processes
                    .get_mut(&target)
                    .ok_or(VmError::ProcessNotFound(target))?;
                if target_proc.mailbox.len() >= self.config.max_mailbox_messages {
                    return Err(VmError::LimitExceeded {
                        limit: "max_mailbox_messages",
                        max: self.config.max_mailbox_messages,
                    });
                }
                target_proc.mailbox.push_back(msg);
                if target_proc.blocked && !target_proc.halted {
                    target_proc.blocked = false;
                    self.enqueue_ready(target);
                }
                Ok(Value::Nil)
            }
            Builtin::Spawn => {
                if args.len() != 1 {
                    return Err(VmError::ArityMismatch {
                        expected: 1,
                        got: args.len(),
                    });
                }
                let child_pid = self.spawn_child_from_closure(args[0].clone())?;
                Ok(Value::Pid(child_pid))
            }
            Builtin::SpawnLink => {
                if args.len() != 1 {
                    return Err(VmError::ArityMismatch {
                        expected: 1,
                        got: args.len(),
                    });
                }
                let child_pid = self.spawn_child_from_closure(args[0].clone())?;
                self.link_pair(pid, child_pid)?;
                Ok(Value::Pid(child_pid))
            }
            Builtin::SpawnMonitor => {
                if args.len() != 1 {
                    return Err(VmError::ArityMismatch {
                        expected: 1,
                        got: args.len(),
                    });
                }
                let child_pid = self.spawn_child_from_closure(args[0].clone())?;
                let ref_id = self.add_monitor(pid, child_pid)?;
                let out = self.heap.alloc_record(
                    BTreeMap::from([
                        ("pid".into(), Value::Pid(child_pid)),
                        ("ref".into(), Value::Integer(ref_id as i64)),
                    ]),
                    None,
                );
                Ok(out)
            }
            Builtin::OsSpawn => {
                if !self.is_unsafe_context(pid)? {
                    return Err(VmError::TypeError(
                        "os.spawn requires unsafe context".into(),
                    ));
                }
                self.call_builtin(pid, Builtin::Spawn, args)
            }
            Builtin::Link => {
                if args.len() != 1 {
                    return Err(VmError::ArityMismatch {
                        expected: 1,
                        got: args.len(),
                    });
                }
                let target = match args[0] {
                    Value::Pid(v) => v,
                    _ => return Err(VmError::TypeError("link expects pid".into())),
                };
                self.link_pair(pid, target)?;
                Ok(Value::Nil)
            }
            Builtin::Unlink => {
                if args.len() != 1 {
                    return Err(VmError::ArityMismatch {
                        expected: 1,
                        got: args.len(),
                    });
                }
                let target = match args[0] {
                    Value::Pid(v) => v,
                    _ => return Err(VmError::TypeError("unlink expects pid".into())),
                };
                self.unlink_pair(pid, target);
                Ok(Value::Nil)
            }
            Builtin::Monitor => {
                if args.len() != 1 {
                    return Err(VmError::ArityMismatch {
                        expected: 1,
                        got: args.len(),
                    });
                }
                let target = match args[0] {
                    Value::Pid(v) => v,
                    _ => return Err(VmError::TypeError("monitor expects pid".into())),
                };
                let ref_id = self.add_monitor(pid, target)?;
                Ok(Value::Integer(ref_id as i64))
            }
            Builtin::Demonitor => {
                if args.len() != 1 {
                    return Err(VmError::ArityMismatch {
                        expected: 1,
                        got: args.len(),
                    });
                }
                let ref_id = match args[0] {
                    Value::Integer(v) if v >= 0 => v as u64,
                    _ => return Err(VmError::TypeError("demonitor expects non-negative ref".into())),
                };
                self.remove_monitor(pid, ref_id);
                Ok(Value::Nil)
            }
            Builtin::Register => {
                if args.len() != 2 {
                    return Err(VmError::ArityMismatch {
                        expected: 2,
                        got: args.len(),
                    });
                }
                let name = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => return Err(VmError::TypeError("register expects string name".into())),
                };
                let target = match args[1] {
                    Value::Pid(v) => v,
                    _ => return Err(VmError::TypeError("register expects pid".into())),
                };
                if !self.processes.contains_key(&target) {
                    return Err(VmError::ProcessNotFound(target));
                }
                self.registry.insert(name, target);
                Ok(Value::Nil)
            }
            Builtin::Unregister => {
                if args.len() != 1 {
                    return Err(VmError::ArityMismatch {
                        expected: 1,
                        got: args.len(),
                    });
                }
                let name = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => return Err(VmError::TypeError("unregister expects string name".into())),
                };
                self.registry.remove(&name);
                Ok(Value::Nil)
            }
            Builtin::WhereIs => {
                if args.len() != 1 {
                    return Err(VmError::ArityMismatch {
                        expected: 1,
                        got: args.len(),
                    });
                }
                let name = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => return Err(VmError::TypeError("whereis expects string name".into())),
                };
                Ok(self
                    .registry
                    .get(&name)
                    .copied()
                    .map(Value::Pid)
                    .unwrap_or(Value::Nil))
            }
            Builtin::Supervisor => {
                if args.len() != 4 {
                    return Err(VmError::ArityMismatch {
                        expected: 4,
                        got: args.len(),
                    });
                }
                let closure = args[0].clone();
                let strategy = match &args[1] {
                    Value::String(s) => s.as_str(),
                    _ => return Err(VmError::TypeError("supervisor strategy must be string".into())),
                };
                let policy = match strategy {
                    "temporary" => RestartPolicy::Temporary,
                    "transient" => RestartPolicy::Transient,
                    "permanent" => RestartPolicy::Permanent,
                    _ => return Err(VmError::InvalidRestartStrategy(strategy.into())),
                };
                let max_restarts = match args[2] {
                    Value::Integer(v) if v >= 0 => v as usize,
                    _ => return Err(VmError::TypeError("supervisor max_restarts expects non-negative integer".into())),
                };
                let window_ticks = match args[3] {
                    Value::Integer(v) if v >= 0 => v as u64,
                    _ => return Err(VmError::TypeError("supervisor window expects non-negative integer".into())),
                };
                let child_pid = self.spawn_child_from_closure(closure.clone())?;
                self.link_pair(pid, child_pid)?;
                self.supervision.insert(
                    child_pid,
                    SupervisorChild {
                        supervisor: pid,
                        closure,
                        policy,
                        max_restarts,
                        window_ticks,
                        restart_ticks: VecDeque::new(),
                    },
                );
                Ok(Value::Pid(child_pid))
            }
            Builtin::Exit => {
                if args.len() > 1 {
                    return Err(VmError::ArityMismatch {
                        expected: 1,
                        got: args.len(),
                    });
                }
                let reason = args.first().cloned().unwrap_or(Value::Nil);
                self.terminate_process(pid, reason.clone())?;
                Ok(reason)
            }
            Builtin::OsExit => {
                if !self.is_unsafe_context(pid)? {
                    return Err(VmError::TypeError("os.exit requires unsafe context".into()));
                }
                self.call_builtin(pid, Builtin::Exit, args)
            }
            Builtin::WithMeta => {
                if args.len() != 2 {
                    return Err(VmError::ArityMismatch {
                        expected: 2,
                        got: args.len(),
                    });
                }
                let (base_fields, _) = self.record_parts(&args[0])?;
                Ok(self.heap.alloc_record(base_fields, Some(args[1].clone())))
            }
            Builtin::GetMeta => {
                if args.len() != 1 {
                    return Err(VmError::ArityMismatch {
                        expected: 1,
                        got: args.len(),
                    });
                }
                let (_, meta) = self.record_parts(&args[0])?;
                Ok(meta.unwrap_or(Value::Nil))
            }
            Builtin::Require => {
                if args.len() != 1 {
                    return Err(VmError::ArityMismatch {
                        expected: 1,
                        got: args.len(),
                    });
                }
                let name = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => return Err(VmError::TypeError("require expects module name string".into())),
                };
                if let Some(v) = self.native_modules.get(&name) {
                    return Ok(v.clone());
                }
                if self.module_loaders.contains_key(&name) {
                    let value = {
                        let loader = self
                            .module_loaders
                            .get(&name)
                            .expect("checked module loader presence");
                        loader()?
                    };
                    self.native_modules.insert(name, value.clone());
                    return Ok(value);
                }
                let loaded = self.load_module_from_search_path(&name)?;
                self.native_modules.insert(name.clone(), loaded.clone());
                if self.native_modules.len() > self.config.max_module_cache_entries {
                    return Err(VmError::LimitExceeded {
                        limit: "max_module_cache_entries",
                        max: self.config.max_module_cache_entries,
                    });
                }
                Ok(loaded)
            }
            Builtin::MathAbs => {
                if args.len() != 1 {
                    return Err(VmError::ArityMismatch {
                        expected: 1,
                        got: args.len(),
                    });
                }
                match args[0] {
                    Value::Integer(v) => Ok(Value::Integer(v.abs())),
                    Value::Float(v) => Ok(Value::Float(v.abs())),
                    _ => Err(VmError::TypeError("math.abs expects number".into())),
                }
            }
            Builtin::MathMax => {
                if args.len() != 2 {
                    return Err(VmError::ArityMismatch {
                        expected: 2,
                        got: args.len(),
                    });
                }
                match (args[0].clone(), args[1].clone()) {
                    (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a.max(b))),
                    (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a.max(b))),
                    (Value::Integer(a), Value::Float(b)) => Ok(Value::Float((a as f64).max(b))),
                    (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a.max(b as f64))),
                    _ => Err(VmError::TypeError("math.max expects numbers".into())),
                }
            }
            Builtin::MathMin => {
                if args.len() != 2 {
                    return Err(VmError::ArityMismatch {
                        expected: 2,
                        got: args.len(),
                    });
                }
                match (args[0].clone(), args[1].clone()) {
                    (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a.min(b))),
                    (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a.min(b))),
                    (Value::Integer(a), Value::Float(b)) => Ok(Value::Float((a as f64).min(b))),
                    (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a.min(b as f64))),
                    _ => Err(VmError::TypeError("math.min expects numbers".into())),
                }
            }
            Builtin::MathSqrt => {
                if args.len() != 1 {
                    return Err(VmError::ArityMismatch {
                        expected: 1,
                        got: args.len(),
                    });
                }
                match args[0] {
                    Value::Integer(v) => Ok(Value::Float((v as f64).sqrt())),
                    Value::Float(v) => Ok(Value::Float(v.sqrt())),
                    _ => Err(VmError::TypeError("math.sqrt expects number".into())),
                }
            }
            Builtin::StringLen => {
                if args.len() != 1 {
                    return Err(VmError::ArityMismatch {
                        expected: 1,
                        got: args.len(),
                    });
                }
                match &args[0] {
                    Value::String(v) => Ok(Value::Integer(v.chars().count() as i64)),
                    _ => Err(VmError::TypeError("string.len expects string".into())),
                }
            }
            Builtin::StringLower => {
                if args.len() != 1 {
                    return Err(VmError::ArityMismatch {
                        expected: 1,
                        got: args.len(),
                    });
                }
                match &args[0] {
                    Value::String(v) => Ok(Value::String(v.to_lowercase())),
                    _ => Err(VmError::TypeError("string.lower expects string".into())),
                }
            }
            Builtin::StringUpper => {
                if args.len() != 1 {
                    return Err(VmError::ArityMismatch {
                        expected: 1,
                        got: args.len(),
                    });
                }
                match &args[0] {
                    Value::String(v) => Ok(Value::String(v.to_uppercase())),
                    _ => Err(VmError::TypeError("string.upper expects string".into())),
                }
            }
            Builtin::TableLen => {
                if args.len() != 1 {
                    return Err(VmError::ArityMismatch {
                        expected: 1,
                        got: args.len(),
                    });
                }
                match args[0] {
                    Value::List(id) => match self.heap.get(id) {
                        Some(HeapObject::List(items)) => Ok(Value::Integer(items.len() as i64)),
                        _ => Err(VmError::TypeError("invalid list object".into())),
                    },
                    Value::Record(id) => match self.heap.get(id) {
                        Some(HeapObject::Record { fields, .. }) => {
                            Ok(Value::Integer(fields.len() as i64))
                        }
                        _ => Err(VmError::TypeError("invalid record object".into())),
                    },
                    _ => Err(VmError::TypeError("table.len expects list or record".into())),
                }
            }
            Builtin::Ffi => {
                if !self.is_unsafe_context(pid)? {
                    return Err(VmError::TypeError("ffi requires unsafe context".into()));
                }
                if args.is_empty() {
                    return Err(VmError::ArityMismatch {
                        expected: 1,
                        got: 0,
                    });
                }
                let name = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => {
                        return Err(VmError::TypeError(
                            "ffi expects function name string as first arg".into(),
                        ))
                    }
                };
                if self.host_functions.contains_key(&name) {
                    let host = self
                        .host_functions
                        .get(&name)
                        .ok_or_else(|| VmError::UnknownGlobal(format!("ffi:{name}")))?;
                    return host(&args[1..]);
                }
                self.call_system_ffi_capability_or_unrestricted(&args)
            }
            Builtin::FfiRegister => {
                if !self.is_unsafe_context(pid)? {
                    return Err(VmError::TypeError("ffi_register requires unsafe context".into()));
                }
                if args.len() != 5 {
                    return Err(VmError::ArityMismatch {
                        expected: 5,
                        got: args.len(),
                    });
                }
                let cap = match &args[0] {
                    Value::String(v) => v.clone(),
                    _ => return Err(VmError::TypeError("ffi_register cap name must be string".into())),
                };
                let lib = match &args[1] {
                    Value::String(v) => v.clone(),
                    _ => return Err(VmError::TypeError("ffi_register lib must be string".into())),
                };
                let symbol = match &args[2] {
                    Value::String(v) => v.clone(),
                    _ => {
                        return Err(VmError::TypeError(
                            "ffi_register symbol must be string".into(),
                        ))
                    }
                };
                let ret = match &args[3] {
                    Value::String(v) => parse_ffi_type_token(v)
                        .ok_or_else(|| VmError::TypeError("ffi_register invalid return type".into()))?,
                    _ => {
                        return Err(VmError::TypeError(
                            "ffi_register return type must be string".into(),
                        ))
                    }
                };
                let params_csv = match &args[4] {
                    Value::String(v) => v,
                    _ => {
                        return Err(VmError::TypeError(
                            "ffi_register param csv must be string".into(),
                        ))
                    }
                };
                let mut params = Vec::new();
                for part in params_csv
                    .split(',')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                {
                    let ty = parse_ffi_type_token(part).ok_or_else(|| {
                        VmError::TypeError(format!("ffi_register invalid param type: {part}"))
                    })?;
                    params.push(ty);
                }
                self.register_system_ffi_capability(
                    cap,
                    lib,
                    symbol,
                    FfiSignature { params, ret },
                );
                Ok(Value::Nil)
            }
        }
    }

    fn get_field(&self, value: Value, key: &str) -> Result<Value, VmError> {
        let (fields, meta) = self.record_parts(&value)?;
        if let Some(v) = fields.get(key) {
            return Ok(v.clone());
        }

        if let Some(meta_value) = meta
            && let Value::Record(meta_id) = meta_value
            && let Some(HeapObject::Record { fields, .. }) = self.heap.get(meta_id)
            && let Some(v) = fields.get(key)
        {
            return Ok(v.clone());
        }

        Err(VmError::UnknownField(key.to_string()))
    }

    fn record_parts(&self, value: &Value) -> Result<(BTreeMap<String, Value>, Option<Value>), VmError> {
        let id = match value {
            Value::Record(id) => *id,
            _ => return Err(VmError::TypeError("expected record".into())),
        };

        match self.heap.get(id) {
            Some(HeapObject::Record { fields, meta }) => Ok((fields.clone(), meta.clone())),
            _ => Err(VmError::TypeError("invalid record object".into())),
        }
    }

    fn install_builtins(&mut self) {
        self.globals
            .insert("print".into(), Value::Builtin(Builtin::Print));
        self.globals
            .insert("self".into(), Value::Builtin(Builtin::SelfPid));
        self.globals
            .insert("send".into(), Value::Builtin(Builtin::Send));
        self.globals
            .insert("spawn".into(), Value::Builtin(Builtin::Spawn));
        self.globals
            .insert("spawn_link".into(), Value::Builtin(Builtin::SpawnLink));
        self.globals
            .insert("spawn_monitor".into(), Value::Builtin(Builtin::SpawnMonitor));
        self.globals
            .insert("exit".into(), Value::Builtin(Builtin::Exit));
        self.globals
            .insert("link".into(), Value::Builtin(Builtin::Link));
        self.globals
            .insert("unlink".into(), Value::Builtin(Builtin::Unlink));
        self.globals
            .insert("monitor".into(), Value::Builtin(Builtin::Monitor));
        self.globals
            .insert("demonitor".into(), Value::Builtin(Builtin::Demonitor));
        self.globals
            .insert("register".into(), Value::Builtin(Builtin::Register));
        self.globals
            .insert("unregister".into(), Value::Builtin(Builtin::Unregister));
        self.globals
            .insert("whereis".into(), Value::Builtin(Builtin::WhereIs));
        self.globals
            .insert("supervisor".into(), Value::Builtin(Builtin::Supervisor));
        self.globals
            .insert("with_meta".into(), Value::Builtin(Builtin::WithMeta));
        self.globals
            .insert("get_meta".into(), Value::Builtin(Builtin::GetMeta));
        self.globals
            .insert("require".into(), Value::Builtin(Builtin::Require));
        self.globals
            .insert("ffi_register".into(), Value::Builtin(Builtin::FfiRegister));
        self.globals.insert("ffi".into(), Value::Builtin(Builtin::Ffi));

        let math = self.heap.alloc_record(
            BTreeMap::from([
                ("abs".into(), Value::Builtin(Builtin::MathAbs)),
                ("max".into(), Value::Builtin(Builtin::MathMax)),
                ("min".into(), Value::Builtin(Builtin::MathMin)),
                ("sqrt".into(), Value::Builtin(Builtin::MathSqrt)),
            ]),
            None,
        );
        self.globals.insert("math".into(), math);

        let string = self.heap.alloc_record(
            BTreeMap::from([
                ("len".into(), Value::Builtin(Builtin::StringLen)),
                ("lower".into(), Value::Builtin(Builtin::StringLower)),
                ("upper".into(), Value::Builtin(Builtin::StringUpper)),
            ]),
            None,
        );
        self.globals.insert("string".into(), string);

        let table = self.heap.alloc_record(
            BTreeMap::from([("len".into(), Value::Builtin(Builtin::TableLen))]),
            None,
        );
        self.globals.insert("table".into(), table);

        let os_record = self.heap.alloc_record(
            BTreeMap::from([
                ("spawn".into(), Value::Builtin(Builtin::OsSpawn)),
                ("exit".into(), Value::Builtin(Builtin::OsExit)),
            ]),
            None,
        );
        self.globals.insert("os".into(), os_record);
    }

    fn spawn_child_from_closure(&mut self, closure: Value) -> Result<ProcessId, VmError> {
        let closure_id = match closure {
            Value::Closure(id) => id,
            _ => return Err(VmError::TypeError("spawn expects function argument".into())),
        };

        let (function_id, captures) = match self.heap.get(closure_id) {
            Some(HeapObject::Closure { function, captures }) => (*function, captures.clone()),
            _ => return Err(VmError::TypeError("invalid closure object".into())),
        };

        let function_arity = self.function(function_id)?.arity;
        if function_arity != 0 {
            return Err(VmError::ArityMismatch {
                expected: 0,
                got: function_arity,
            });
        }

        let child_pid = self.next_pid;
        if self.processes.len() >= self.config.max_processes {
            return Err(VmError::LimitExceeded {
                limit: "max_processes",
                max: self.config.max_processes,
            });
        }
        self.next_pid += 1;

        let child = Process {
            stack: Vec::new(),
            call_stack: vec![CallFrame {
                function: function_id,
                ip: 0,
                locals: vec![Value::Nil; function_arity.max(8)],
                upvalues: captures,
                self_closure: Some(Value::Closure(closure_id)),
            }],
            mailbox: VecDeque::new(),
            unsafe_depth: 0,
            blocked: false,
            halted: false,
            result: None,
            waiting_receive: None,
        };

        self.processes.insert(child_pid, child);
        self.enqueue_ready(child_pid);
        Ok(child_pid)
    }

    fn enqueue_ready(&mut self, pid: ProcessId) {
        if !self.queued.contains(&pid) {
            self.run_queue.push_back(pid);
            self.queued.insert(pid);
        }
    }

    fn wake_timed_out_processes(&mut self) {
        let mut to_wake = Vec::new();
        for (pid, proc) in &self.processes {
            if proc.halted || !proc.blocked {
                continue;
            }
            let Some(wait) = &proc.waiting_receive else {
                continue;
            };
            if let ReceiveWaitStage::WaitingUntil(deadline) = wait.stage
                && self.ticks >= deadline
            {
                to_wake.push(*pid);
            }
        }

        for pid in to_wake {
            if let Some(proc) = self.processes.get_mut(&pid) {
                proc.blocked = false;
            }
            self.enqueue_ready(pid);
        }
    }

    fn next_timeout_deadline(&self) -> Option<u64> {
        self.processes
            .values()
            .filter_map(|proc| match &proc.waiting_receive {
                Some(ReceiveWaitState {
                    stage: ReceiveWaitStage::WaitingUntil(deadline),
                    ..
                }) => Some(*deadline),
                _ => None,
            })
            .min()
    }

    fn timeout_ticks_from_value(&self, value: Value) -> Result<u64, VmError> {
        match value {
            Value::Integer(v) if v >= 0 => Ok(v as u64),
            Value::Float(v) if v >= 0.0 => Ok(v as u64),
            _ => Err(VmError::TimeoutValueInvalid),
        }
    }

    fn link_pair(&mut self, a: ProcessId, b: ProcessId) -> Result<(), VmError> {
        if !self.processes.contains_key(&a) {
            return Err(VmError::ProcessNotFound(a));
        }
        if !self.processes.contains_key(&b) {
            return Err(VmError::ProcessNotFound(b));
        }
        self.links.entry(a).or_default().insert(b);
        self.links.entry(b).or_default().insert(a);
        Ok(())
    }

    fn unlink_pair(&mut self, a: ProcessId, b: ProcessId) {
        if let Some(set) = self.links.get_mut(&a) {
            set.remove(&b);
        }
        if let Some(set) = self.links.get_mut(&b) {
            set.remove(&a);
        }
    }

    fn add_monitor(&mut self, watcher: ProcessId, target: ProcessId) -> Result<MonitorRef, VmError> {
        if !self.processes.contains_key(&watcher) {
            return Err(VmError::ProcessNotFound(watcher));
        }
        if !self.processes.contains_key(&target) {
            return Err(VmError::ProcessNotFound(target));
        }
        let ref_id = self.next_monitor_ref;
        self.next_monitor_ref += 1;
        let monitor = Monitor {
            watcher,
            target,
            ref_id,
        };
        self.monitors_by_target
            .entry(target)
            .or_default()
            .push(monitor.clone());
        self.monitors_by_watcher
            .entry(watcher)
            .or_default()
            .push(monitor);
        Ok(ref_id)
    }

    fn remove_monitor(&mut self, watcher: ProcessId, ref_id: MonitorRef) {
        if let Some(v) = self.monitors_by_watcher.get_mut(&watcher) {
            v.retain(|m| m.ref_id != ref_id);
        }
        for monitors in self.monitors_by_target.values_mut() {
            monitors.retain(|m| !(m.watcher == watcher && m.ref_id == ref_id));
        }
    }

    fn terminate_process(&mut self, pid: ProcessId, reason: Value) -> Result<(), VmError> {
        let proc = self
            .processes
            .get_mut(&pid)
            .ok_or(VmError::ProcessNotFound(pid))?;
        if proc.halted {
            return Ok(());
        }
        proc.halted = true;
        proc.result = Some(reason.clone());
        proc.blocked = false;
        proc.waiting_receive = None;

        if pid == self.main_pid {
            self.halted = true;
            self.result = Some(reason.clone());
        }

        self.notify_links_and_monitors(pid, reason.clone());
        self.cleanup_process_metadata(pid);
        self.try_restart_supervised_child(pid, reason)?;
        Ok(())
    }

    fn cleanup_process_metadata(&mut self, pid: ProcessId) {
        self.queued.remove(&pid);
        self.run_queue.retain(|p| *p != pid);
        self.registry.retain(|_, owner| *owner != pid);
        if let Some(peers) = self.links.remove(&pid) {
            for peer in peers {
                if let Some(set) = self.links.get_mut(&peer) {
                    set.remove(&pid);
                }
            }
        }
        self.monitors_by_watcher.remove(&pid);
    }

    fn notify_links_and_monitors(&mut self, pid: ProcessId, reason: Value) {
        if let Some(linked) = self.links.get(&pid).cloned() {
            for linked_pid in linked {
                let exit_msg = self.heap.alloc_record(
                    BTreeMap::from([
                        ("type".into(), Value::String("EXIT".into())),
                        ("from".into(), Value::Pid(pid)),
                        ("reason".into(), reason.clone()),
                    ]),
                    None,
                );
                if let Some(linked_proc) = self.processes.get_mut(&linked_pid) {
                    if linked_proc.halted {
                        continue;
                    }
                    linked_proc.mailbox.push_back(exit_msg);
                    let should_wake = linked_proc.blocked;
                    if linked_proc.blocked {
                        linked_proc.blocked = false;
                    }
                    if should_wake {
                        self.enqueue_ready(linked_pid);
                    }
                }
            }
        }

        if let Some(monitors) = self.monitors_by_target.remove(&pid) {
            for monitor in monitors {
                if let Some(list) = self.monitors_by_watcher.get_mut(&monitor.watcher) {
                    list.retain(|m| m.ref_id != monitor.ref_id);
                }
                let down_msg = self.heap.alloc_record(
                    BTreeMap::from([
                        ("type".into(), Value::String("DOWN".into())),
                        ("ref".into(), Value::Integer(monitor.ref_id as i64)),
                        ("pid".into(), Value::Pid(monitor.target)),
                        ("reason".into(), reason.clone()),
                    ]),
                    None,
                );
                if let Some(watcher_proc) = self.processes.get_mut(&monitor.watcher) {
                    watcher_proc.mailbox.push_back(down_msg);
                    let should_wake = watcher_proc.blocked;
                    if watcher_proc.blocked {
                        watcher_proc.blocked = false;
                    }
                    if should_wake {
                        self.enqueue_ready(monitor.watcher);
                    }
                }
            }
        }
    }

    fn try_restart_supervised_child(
        &mut self,
        exited_pid: ProcessId,
        reason: Value,
    ) -> Result<(), VmError> {
        let Some(mut spec) = self.supervision.remove(&exited_pid) else {
            return Ok(());
        };

        let should_restart = match spec.policy {
            RestartPolicy::Temporary => false,
            RestartPolicy::Permanent => true,
            RestartPolicy::Transient => match &reason {
                Value::Nil => false,
                Value::String(s) if s == "normal" => false,
                _ => true,
            },
        };
        if !should_restart {
            return Ok(());
        }

        while let Some(t) = spec.restart_ticks.front().copied() {
            if self.ticks.saturating_sub(t) > spec.window_ticks {
                spec.restart_ticks.pop_front();
            } else {
                break;
            }
        }
        if spec.restart_ticks.len() >= spec.max_restarts {
            return Ok(());
        }

        let child_pid = self.spawn_child_from_closure(spec.closure.clone())?;
        let _ = self.link_pair(spec.supervisor, child_pid);
        spec.restart_ticks.push_back(self.ticks);
        self.supervision.insert(child_pid, spec);
        Ok(())
    }

    fn is_unsafe_context(&self, pid: ProcessId) -> Result<bool, VmError> {
        Ok(self.process(pid)?.unsafe_depth > 0)
    }

    fn function(&self, id: FunctionId) -> Result<&Function, VmError> {
        self.module
            .functions
            .get(id.0)
            .ok_or(VmError::FunctionOutOfBounds)
    }

    fn current_function(&self, pid: ProcessId) -> Result<FunctionId, VmError> {
        let proc = self.process(pid)?;
        let frame = proc
            .call_stack
            .last()
            .ok_or(VmError::InvalidInstructionPointer)?;
        Ok(frame.function)
    }

    fn load_constant(&self, function_id: FunctionId, id: ConstId) -> Result<Value, VmError> {
        let function = self.function(function_id)?;
        let constant = function
            .constants
            .get(id.0)
            .ok_or(VmError::InvalidInstructionPointer)?;

        Ok(match constant {
            Constant::Integer(v) => Value::Integer(*v),
            Constant::Float(v) => Value::Float(*v),
            Constant::String(v) => Value::String(v.clone()),
            Constant::Symbol(v) => Value::String(v.clone()),
        })
    }

    fn const_symbol(&self, function_id: FunctionId, id: ConstId) -> Result<String, VmError> {
        let function = self.function(function_id)?;
        let constant = function
            .constants
            .get(id.0)
            .ok_or(VmError::InvalidInstructionPointer)?;
        match constant {
            Constant::Symbol(v) => Ok(v.clone()),
            _ => Err(VmError::TypeError("expected symbol constant".into())),
        }
    }

    fn set_local(
        &mut self,
        pid: ProcessId,
        frame_idx: usize,
        local_idx: usize,
        value: Value,
    ) -> Result<(), VmError> {
        let proc = self.process_mut(pid)?;
        if proc.call_stack[frame_idx].locals.len() <= local_idx {
            proc.call_stack[frame_idx]
                .locals
                .resize(local_idx + 1, Value::Nil);
        }
        proc.call_stack[frame_idx].locals[local_idx] = value;
        Ok(())
    }

    fn capture_values(
        &self,
        pid: ProcessId,
        frame_idx: usize,
        captures: &[CaptureRef],
    ) -> Result<Vec<Value>, VmError> {
        let proc = self.process(pid)?;
        let frame = proc
            .call_stack
            .get(frame_idx)
            .ok_or(VmError::InvalidInstructionPointer)?;
        Ok(captures
            .iter()
            .map(|capture| match capture {
                CaptureRef::Local(id) => frame.locals.get(id.0).cloned().unwrap_or(Value::Nil),
                CaptureRef::Upvalue(id) => frame.upvalues.get(id.0).cloned().unwrap_or(Value::Nil),
                CaptureRef::SelfClosure => frame.self_closure.clone().unwrap_or(Value::Nil),
            })
            .collect())
    }

    fn pop(&mut self, pid: ProcessId) -> Result<Value, VmError> {
        self.process_mut(pid)?.stack.pop().ok_or(VmError::StackUnderflow)
    }

    fn peek(&self, pid: ProcessId) -> Result<&Value, VmError> {
        self.process(pid)?.stack.last().ok_or(VmError::StackUnderflow)
    }

    fn process(&self, pid: ProcessId) -> Result<&Process, VmError> {
        self.processes.get(&pid).ok_or(VmError::ProcessNotFound(pid))
    }

    fn process_mut(&mut self, pid: ProcessId) -> Result<&mut Process, VmError> {
        self.processes
            .get_mut(&pid)
            .ok_or(VmError::ProcessNotFound(pid))
    }

    fn collect_garbage_if_needed(&mut self) {
        let roots = self.collect_roots();
        let _ = self
            .heap
            .maybe_collect(roots.iter(), self.config.gc_slice_budget);
    }

    fn collect_roots(&self) -> Vec<Value> {
        let mut roots = Vec::new();
        roots.extend(self.globals.values().cloned());
        roots.extend(self.native_modules.values().cloned());
        if let Some(v) = &self.result {
            roots.push(v.clone());
        }

        for proc in self.processes.values() {
            roots.extend(proc.stack.iter().cloned());
            roots.extend(proc.mailbox.iter().cloned());
            if let Some(v) = &proc.result {
                roots.push(v.clone());
            }
            if let Some(wait) = &proc.waiting_receive {
                roots.extend(wait.upvalues.iter().cloned());
            }
            for frame in &proc.call_stack {
                roots.extend(frame.locals.iter().cloned());
                roots.extend(frame.upvalues.iter().cloned());
            }
        }

        for spec in self.supervision.values() {
            roots.push(spec.closure.clone());
        }
        roots
    }

    fn num_bin(
        &self,
        left: Value,
        right: Value,
        int_fn: impl FnOnce(i64, i64) -> i64,
        float_fn: impl FnOnce(f64, f64) -> f64,
    ) -> Result<Value, VmError> {
        match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(int_fn(a, b))),
            (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(float_fn(a as f64, b))),
            (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(float_fn(a, b as f64))),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(float_fn(a, b))),
            _ => Err(VmError::TypeError("numeric operator expects numbers".into())),
        }
    }

    fn cmp_bin(
        &self,
        left: Value,
        right: Value,
        cmp: impl FnOnce(f64, f64) -> bool,
    ) -> Result<Value, VmError> {
        match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Ok(Value::Bool(cmp(a as f64, b as f64))),
            (Value::Integer(a), Value::Float(b)) => Ok(Value::Bool(cmp(a as f64, b))),
            (Value::Float(a), Value::Integer(b)) => Ok(Value::Bool(cmp(a, b as f64))),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Bool(cmp(a, b))),
            _ => Err(VmError::TypeError("comparison expects numeric operands".into())),
        }
    }

    fn call_system_ffi_capability_or_unrestricted(
        &mut self,
        args: &[Value],
    ) -> Result<Value, VmError> {
        if args.is_empty() {
            return Err(VmError::ArityMismatch {
                expected: 1,
                got: 0,
            });
        }
        let name = match &args[0] {
            Value::String(v) => v.as_str(),
            _ => return Err(VmError::TypeError("ffi expects capability name string".into())),
        };

        if let Some(cap) = self.ffi_caps.get(name).cloned() {
            return self.call_system_ffi_typed(&cap.lib, &cap.symbol, &cap.signature, &args[1..]);
        }

        if !self.config.allow_unrestricted_system_ffi {
            return Err(VmError::SecurityViolation(format!(
                "ffi capability not allowed: {name}"
            )));
        }

        if args.len() < 3 {
            return Err(VmError::ArityMismatch {
                expected: 3,
                got: args.len(),
            });
        }

        let lib = match &args[0] {
            Value::String(v) => v.clone(),
            _ => return Err(VmError::TypeError("ffi expects library name".into())),
        };
        let symbol = match &args[1] {
            Value::String(v) => v.clone(),
            _ => return Err(VmError::TypeError("ffi expects symbol name".into())),
        };
        let signature = infer_unrestricted_signature(&args[2..]);
        self.call_system_ffi_typed(&lib, &symbol, &signature, &args[2..])
    }

    fn call_system_ffi_typed(
        &mut self,
        lib_name: &str,
        symbol: &str,
        signature: &FfiSignature,
        args: &[Value],
    ) -> Result<Value, VmError> {
        if args.len() != signature.params.len() {
            return Err(VmError::ArityMismatch {
                expected: signature.params.len(),
                got: args.len(),
            });
        }
        let handle = self.open_ffi_library(lib_name)?;
        let sym = self.lookup_ffi_symbol(handle, symbol)?;
        let native_args = self.to_native_ffi_args_typed(args, &signature.params)?;
        self.invoke_ffi_symbol_typed(sym, &native_args, signature.ret)
    }

    fn to_native_ffi_args_typed(
        &self,
        args: &[Value],
        param_types: &[FfiType],
    ) -> Result<NativeFfiArgs, VmError> {
        let mut converted = Vec::with_capacity(args.len());
        let mut string_storage = Vec::new();
        for (value, ty) in args.iter().zip(param_types.iter()) {
            let native = match ty {
                FfiType::Int64 => match value {
                    Value::Integer(v) => *v as usize,
                    _ => {
                        return Err(VmError::TypeError(
                            "ffi int64 argument expects integer".into(),
                        ))
                    }
                },
                FfiType::UInt64 | FfiType::Ptr => match value {
                    Value::Integer(v) if *v >= 0 => *v as usize,
                    Value::Pid(v) => *v as usize,
                    Value::Nil => 0usize,
                    _ => {
                        return Err(VmError::TypeError(
                            "ffi uint64/ptr argument expects non-negative integer/pid/nil".into(),
                        ))
                    }
                },
                FfiType::Bool => match value {
                    Value::Bool(v) => usize::from(*v),
                    _ => return Err(VmError::TypeError("ffi bool argument expects bool".into())),
                },
                FfiType::CString => match value {
                    Value::String(s) => {
                        let c = CString::new(s.as_str()).map_err(|_| {
                            VmError::TypeError("ffi cstring arg contains interior NUL byte".into())
                        })?;
                        let ptr = c.as_ptr() as usize;
                        string_storage.push(c);
                        ptr
                    }
                    Value::Nil => 0usize,
                    _ => {
                        return Err(VmError::TypeError(
                            "ffi cstring argument expects string or nil".into(),
                        ))
                    }
                },
                FfiType::Void => {
                    return Err(VmError::TypeError(
                        "ffi void is invalid for parameter type".into(),
                    ))
                }
            };
            converted.push(native);
        }
        Ok(NativeFfiArgs {
            words: converted,
            string_storage,
        })
    }

    #[cfg(unix)]
    fn open_ffi_library(&mut self, lib_name: &str) -> Result<*mut c_void, VmError> {
        if let Some(lib) = self.ffi_libs.get(lib_name) {
            return Ok(lib.handle);
        }
        let name = CString::new(lib_name)
            .map_err(|_| VmError::TypeError("ffi library path contains interior NUL byte".into()))?;
        let handle = unsafe { dlopen(name.as_ptr(), RTLD_NOW) };
        if handle.is_null() {
            return Err(VmError::TypeError(format!("failed to load library: {lib_name}")));
        }
        self.ffi_libs
            .insert(lib_name.to_string(), NativeLibrary { handle });
        Ok(handle)
    }

    #[cfg(not(unix))]
    fn open_ffi_library(&mut self, lib_name: &str) -> Result<*mut c_void, VmError> {
        let _ = lib_name;
        Err(VmError::TypeError(
            "system ffi dynamic loading is only supported on unix in this build".into(),
        ))
    }

    #[cfg(unix)]
    fn lookup_ffi_symbol(&self, handle: *mut c_void, symbol: &str) -> Result<*mut c_void, VmError> {
        let symbol_name = CString::new(symbol)
            .map_err(|_| VmError::TypeError("ffi symbol contains interior NUL byte".into()))?;
        let sym = unsafe { dlsym(handle, symbol_name.as_ptr()) };
        if sym.is_null() {
            return Err(VmError::TypeError(format!(
                "failed to resolve symbol: {symbol}"
            )));
        }
        Ok(sym)
    }

    #[cfg(not(unix))]
    fn lookup_ffi_symbol(&self, handle: *mut c_void, symbol: &str) -> Result<*mut c_void, VmError> {
        let _ = (handle, symbol);
        Err(VmError::TypeError(
            "system ffi dynamic loading is only supported on unix in this build".into(),
        ))
    }

    fn invoke_ffi_symbol_typed(
        &self,
        sym: *mut c_void,
        args: &NativeFfiArgs,
        ret: FfiType,
    ) -> Result<Value, VmError> {
        let _keep_alive = &args.string_storage;
        let out = unsafe {
            match args.words.len() {
                0 => {
                    let f: extern "C" fn() -> usize = std::mem::transmute(sym);
                    f()
                }
                1 => {
                    let f: extern "C" fn(usize) -> usize = std::mem::transmute(sym);
                    f(args.words[0])
                }
                2 => {
                    let f: extern "C" fn(usize, usize) -> usize = std::mem::transmute(sym);
                    f(args.words[0], args.words[1])
                }
                3 => {
                    let f: extern "C" fn(usize, usize, usize) -> usize = std::mem::transmute(sym);
                    f(args.words[0], args.words[1], args.words[2])
                }
                4 => {
                    let f: extern "C" fn(usize, usize, usize, usize) -> usize =
                        std::mem::transmute(sym);
                    f(args.words[0], args.words[1], args.words[2], args.words[3])
                }
                5 => {
                    let f: extern "C" fn(usize, usize, usize, usize, usize) -> usize =
                        std::mem::transmute(sym);
                    f(
                        args.words[0],
                        args.words[1],
                        args.words[2],
                        args.words[3],
                        args.words[4],
                    )
                }
                6 => {
                    let f: extern "C" fn(usize, usize, usize, usize, usize, usize) -> usize =
                        std::mem::transmute(sym);
                    f(
                        args.words[0],
                        args.words[1],
                        args.words[2],
                        args.words[3],
                        args.words[4],
                        args.words[5],
                    )
                }
                _ => {
                    return Err(VmError::TypeError(
                        "ffi supports up to 6 native arguments in v0.1".into(),
                    ))
                }
            }
        };
        match ret {
            FfiType::Void => Ok(Value::Nil),
            FfiType::Bool => Ok(Value::Bool(out != 0)),
            FfiType::Int64 => Ok(Value::Integer(out as i64)),
            FfiType::UInt64 | FfiType::Ptr => Ok(Value::Integer(out as i64)),
            FfiType::CString => {
                if out == 0 {
                    Ok(Value::Nil)
                } else {
                    let s = unsafe {
                        let ptr = out as *const c_char;
                        std::ffi::CStr::from_ptr(ptr)
                            .to_str()
                            .map_err(|_| VmError::TypeError("ffi returned invalid UTF-8".into()))?
                            .to_string()
                    };
                    Ok(Value::String(s))
                }
            }
        }
    }

    fn load_module_from_search_path(&mut self, name: &str) -> Result<Value, VmError> {
        for base in &self.config.module_search_paths {
            let mut source_path = base.clone();
            source_path.push(format!("{name}.rua"));
            if source_path.exists() {
                return self.load_source_module(name, &source_path);
            }
            let mut bytecode_path = base.clone();
            bytecode_path.push(format!("{name}.ruac"));
            if bytecode_path.exists() {
                return self.load_bytecode_module(name, &bytecode_path);
            }
        }
        Err(VmError::UnknownGlobal(format!("module:{name}")))
    }

    fn load_source_module(&mut self, name: &str, path: &Path) -> Result<Value, VmError> {
        let bytes = fs::read(path).map_err(|e| VmError::TypeError(format!("module read error: {e}")))?;
        if bytes.len() > self.config.max_module_bytes {
            return Err(VmError::LimitExceeded {
                limit: "max_module_bytes",
                max: self.config.max_module_bytes,
            });
        }
        self.verify_module_if_required(name, &bytes, path)?;
        let source =
            String::from_utf8(bytes).map_err(|_| VmError::TypeError("module source is not UTF-8".into()))?;
        let module =
            compile_source(&source).map_err(|e| VmError::TypeError(format!("compile error: {e}")))?;
        let mut nested = Vm::with_config(module, self.config.clone());
        nested.host_functions = std::mem::take(&mut self.host_functions);
        let value = nested.run();
        self.host_functions = std::mem::take(&mut nested.host_functions);
        let value = value?;
        self.import_external_value(&nested, &value)
    }

    fn load_bytecode_module(&mut self, name: &str, path: &Path) -> Result<Value, VmError> {
        let bytes = fs::read(path).map_err(|e| VmError::TypeError(format!("module read error: {e}")))?;
        if bytes.len() > self.config.max_module_bytes {
            return Err(VmError::LimitExceeded {
                limit: "max_module_bytes",
                max: self.config.max_module_bytes,
            });
        }
        self.verify_module_if_required(name, &bytes, path)?;
        let module = decode_module(&bytes)
            .map_err(|e| VmError::InvalidBytecode(e.to_string()))?;
        validate_module(&module).map_err(|e| VmError::InvalidBytecode(e.to_string()))?;
        let mut nested = Vm::with_config(module, self.config.clone());
        nested.host_functions = std::mem::take(&mut self.host_functions);
        let value = nested.run();
        self.host_functions = std::mem::take(&mut nested.host_functions);
        let value = value?;
        self.import_external_value(&nested, &value)
    }

    fn import_external_value(&mut self, other: &Vm, value: &Value) -> Result<Value, VmError> {
        match value {
            Value::Integer(v) => Ok(Value::Integer(*v)),
            Value::Float(v) => Ok(Value::Float(*v)),
            Value::String(v) => Ok(Value::String(v.clone())),
            Value::Bool(v) => Ok(Value::Bool(*v)),
            Value::Nil => Ok(Value::Nil),
            Value::Pid(v) => Ok(Value::Pid(*v)),
            Value::Builtin(_) | Value::Closure(_) => Err(VmError::SecurityViolation(
                "module exports cannot contain closures/builtins".into(),
            )),
            Value::List(id) => {
                let Some(HeapObject::List(items)) = other.heap.get(*id) else {
                    return Err(VmError::TypeError("invalid external list object".into()));
                };
                let mut imported = Vec::with_capacity(items.len());
                for item in items {
                    imported.push(self.import_external_value(other, item)?);
                }
                Ok(self.heap.alloc_list(imported))
            }
            Value::Record(id) => {
                let Some(HeapObject::Record { fields, meta }) = other.heap.get(*id) else {
                    return Err(VmError::TypeError("invalid external record object".into()));
                };
                let mut imported = BTreeMap::new();
                for (k, v) in fields {
                    imported.insert(k.clone(), self.import_external_value(other, v)?);
                }
                let imported_meta = match meta {
                    Some(v) => Some(self.import_external_value(other, v)?),
                    None => None,
                };
                Ok(self.heap.alloc_record(imported, imported_meta))
            }
        }
    }

    fn verify_module_if_required(
        &self,
        module_name: &str,
        bytes: &[u8],
        module_path: &Path,
    ) -> Result<(), VmError> {
        #[cfg(not(feature = "builtin_sha256_verify"))]
        let _ = bytes;
        let sig_path = module_path.with_extension(format!(
            "{}.sig",
            module_path
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or_default()
        ));
        let sig = fs::read(&sig_path).unwrap_or_default();
        if self.config.require_signed_modules && sig.is_empty() {
            return Err(VmError::ModuleVerificationFailed(module_name.into()));
        }
        if sig.is_empty() {
            return Ok(());
        }

        if let Some(verifier) = &self.module_verifier {
            if verifier(module_name, bytes, &sig) {
                return Ok(());
            }
            return Err(VmError::ModuleVerificationFailed(module_name.into()));
        }

        #[cfg(feature = "builtin_sha256_verify")]
        {
            let mut hasher = Sha256::new();
            hasher.update(bytes);
            let digest = hasher.finalize();
            let expected = sig
                .iter()
                .copied()
                .filter(|b| !b.is_ascii_whitespace())
                .collect::<Vec<_>>();
            let hex = hex_lower(&digest);
            if expected == hex.as_bytes() {
                return Ok(());
            }
        }

        Err(VmError::ModuleVerificationFailed(module_name.into()))
    }

    fn enforce_limits(&self) -> Result<(), VmError> {
        if self.processes.len() > self.config.max_processes {
            return Err(VmError::LimitExceeded {
                limit: "max_processes",
                max: self.config.max_processes,
            });
        }
        if self.heap.live_objects() > self.config.max_heap_objects {
            return Err(VmError::LimitExceeded {
                limit: "max_heap_objects",
                max: self.config.max_heap_objects,
            });
        }
        for process in self.processes.values() {
            if process.stack.len() > self.config.max_stack_values {
                return Err(VmError::LimitExceeded {
                    limit: "max_stack_values",
                    max: self.config.max_stack_values,
                });
            }
            if process.call_stack.len() > self.config.max_call_depth {
                return Err(VmError::LimitExceeded {
                    limit: "max_call_depth",
                    max: self.config.max_call_depth,
                });
            }
            if process.mailbox.len() > self.config.max_mailbox_messages {
                return Err(VmError::LimitExceeded {
                    limit: "max_mailbox_messages",
                    max: self.config.max_mailbox_messages,
                });
            }
        }
        Ok(())
    }

    fn fail<T>(&mut self, err: VmError) -> Result<T, VmError> {
        let process_id = self.main_pid;
        let stack_trace = self
            .process(process_id)
            .map(|p| {
                p.call_stack
                    .iter()
                    .rev()
                    .map(|frame| VmFrameTrace {
                        function: self
                            .module
                            .functions
                            .get(frame.function.0)
                            .and_then(|f| f.name.clone()),
                        function_id: frame.function.0,
                        ip: frame.ip,
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        self.last_diagnostic = Some(VmDiagnostic {
            code: vm_error_code(&err),
            message: err.to_string(),
            process_id,
            stack_trace,
        });
        Err(err)
    }
}

fn vm_error_code(err: &VmError) -> &'static str {
    match err {
        VmError::Halted => "E_HALTED",
        VmError::InvalidInstructionPointer => "E_IP",
        VmError::StackUnderflow => "E_STACK",
        VmError::TypeError(_) => "E_TYPE",
        VmError::UnknownGlobal(_) => "E_UNKNOWN_GLOBAL",
        VmError::UnknownField(_) => "E_UNKNOWN_FIELD",
        VmError::InvalidCallTarget => "E_CALL_TARGET",
        VmError::ArityMismatch { .. } => "E_ARITY",
        VmError::FunctionOutOfBounds => "E_FUNCTION_BOUNDS",
        VmError::ReceiveBlocked => "E_RECEIVE_BLOCKED",
        VmError::InvalidJumpTarget(_) => "E_JUMP",
        VmError::TimeoutValueInvalid => "E_TIMEOUT",
        VmError::ProcessNotFound(_) => "E_PROCESS",
        VmError::InvalidRestartStrategy(_) => "E_RESTART",
        VmError::LimitExceeded { .. } => "E_LIMIT",
        VmError::SecurityViolation(_) => "E_SECURITY",
        VmError::InvalidBytecode(_) => "E_BYTECODE",
        VmError::ModuleVerificationFailed(_) => "E_MODULE_VERIFY",
    }
}

fn infer_unrestricted_signature(args: &[Value]) -> FfiSignature {
    let params = args
        .iter()
        .map(|v| match v {
            Value::String(_) => FfiType::CString,
            Value::Bool(_) => FfiType::Bool,
            _ => FfiType::UInt64,
        })
        .collect::<Vec<_>>();
    FfiSignature {
        params,
        ret: FfiType::UInt64,
    }
}

fn parse_ffi_type_token(token: &str) -> Option<FfiType> {
    match token {
        "i64" => Some(FfiType::Int64),
        "u64" => Some(FfiType::UInt64),
        "bool" => Some(FfiType::Bool),
        "cstring" => Some(FfiType::CString),
        "ptr" => Some(FfiType::Ptr),
        "void" => Some(FfiType::Void),
        _ => None,
    }
}

#[cfg(feature = "builtin_sha256_verify")]
fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(&mut out, "{:02x}", b);
    }
    out
}

struct NativeFfiArgs {
    words: Vec<usize>,
    string_storage: Vec<CString>,
}

impl Drop for Vm {
    fn drop(&mut self) {
        #[cfg(unix)]
        for lib in self.ffi_libs.values() {
            if !lib.handle.is_null() {
                unsafe {
                    let _ = dlclose(lib.handle);
                }
            }
        }
    }
}
