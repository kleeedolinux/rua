use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum VmError {
    Halted,
    InvalidInstructionPointer,
    StackUnderflow,
    TypeError(String),
    UnknownGlobal(String),
    UnknownField(String),
    InvalidCallTarget,
    ArityMismatch { expected: usize, got: usize },
    FunctionOutOfBounds,
    ReceiveBlocked,
    InvalidJumpTarget(usize),
    TimeoutValueInvalid,
    ProcessNotFound(u64),
    InvalidRestartStrategy(String),
    LimitExceeded { limit: &'static str, max: usize },
    SecurityViolation(String),
    InvalidBytecode(String),
    ModuleVerificationFailed(String),
}

impl fmt::Display for VmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Halted => write!(f, "vm already halted"),
            Self::InvalidInstructionPointer => write!(f, "invalid instruction pointer"),
            Self::StackUnderflow => write!(f, "stack underflow"),
            Self::TypeError(msg) => write!(f, "type error: {msg}"),
            Self::UnknownGlobal(name) => write!(f, "unknown global: {name}"),
            Self::UnknownField(name) => write!(f, "unknown field: {name}"),
            Self::InvalidCallTarget => write!(f, "attempted to call non-function value"),
            Self::ArityMismatch { expected, got } => {
                write!(f, "arity mismatch: expected {expected}, got {got}")
            }
            Self::FunctionOutOfBounds => write!(f, "function id out of bounds"),
            Self::ReceiveBlocked => write!(f, "receive blocked waiting for matching message"),
            Self::InvalidJumpTarget(target) => write!(f, "invalid jump target: {target}"),
            Self::TimeoutValueInvalid => write!(f, "receive after expects a non-negative integer timeout"),
            Self::ProcessNotFound(pid) => write!(f, "process not found: {pid}"),
            Self::InvalidRestartStrategy(s) => write!(f, "invalid restart strategy: {s}"),
            Self::LimitExceeded { limit, max } => {
                write!(f, "limit exceeded: {limit} (max {max})")
            }
            Self::SecurityViolation(msg) => write!(f, "security violation: {msg}"),
            Self::InvalidBytecode(msg) => write!(f, "invalid bytecode: {msg}"),
            Self::ModuleVerificationFailed(name) => {
                write!(f, "module verification failed: {name}")
            }
        }
    }
}

impl std::error::Error for VmError {}
