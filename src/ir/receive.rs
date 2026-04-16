use super::{CaptureRef, FunctionId, Pattern};

#[derive(Debug, Clone, PartialEq)]
pub struct ReceiveCase {
    pub pattern: Pattern,
    pub bindings: usize,
    pub handler: FunctionId,
    pub captures: Vec<CaptureRef>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReceiveAfter {
    pub timeout_handler: FunctionId,
    pub timeout_captures: Vec<CaptureRef>,
    pub body_handler: FunctionId,
    pub body_captures: Vec<CaptureRef>,
}
