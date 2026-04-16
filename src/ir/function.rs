use super::{Constant, Instr};

#[derive(Debug, Clone, PartialEq)]
pub struct Function {
    pub name: Option<String>,
    pub arity: usize,
    pub upvalue_count: usize,
    pub constants: Vec<Constant>,
    pub code: Vec<Instr>,
}
