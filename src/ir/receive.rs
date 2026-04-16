use super::{FunctionId, Pattern};

#[derive(Debug, Clone, PartialEq)]
pub struct ReceiveCase {
    pub pattern: Pattern,
    pub bindings: usize,
    pub handler: FunctionId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReceiveAfter {
    pub timeout_handler: FunctionId,
    pub body_handler: FunctionId,
}
