use super::{Function, FunctionId};

#[derive(Debug, Clone, PartialEq)]
pub struct Module {
    pub functions: Vec<Function>,
    pub entry: FunctionId,
}
