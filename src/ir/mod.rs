mod constant;
mod function;
mod ids;
mod instr;
mod module;
mod pattern;
mod receive;

pub use constant::Constant;
pub use function::Function;
pub use ids::{ConstId, FunctionId, LocalId, UpvalueId};
pub use instr::{CaptureRef, Instr};
pub use module::Module;
pub use pattern::Pattern;
pub use receive::{ReceiveAfter, ReceiveCase};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stores_function_metadata() {
        let f = Function {
            name: Some("main".into()),
            arity: 0,
            upvalue_count: 0,
            constants: vec![Constant::Integer(1)],
            code: vec![Instr::LoadConst(ConstId(0)), Instr::Return],
        };

        assert_eq!(f.constants.len(), 1);
        assert_eq!(f.code.len(), 2);
    }
}
