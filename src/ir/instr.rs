use crate::ast::{BinaryOp, UnaryOp};

use super::{ConstId, FunctionId, LocalId, ReceiveAfter, ReceiveCase, UpvalueId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureRef {
    Local(LocalId),
    Upvalue(UpvalueId),
    SelfClosure,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Instr {
    LoadConst(ConstId),
    PushBool(bool),
    PushNil,
    LoadLocal(LocalId),
    LoadUpvalue(UpvalueId),
    LoadSelf,
    LoadGlobal(ConstId),
    BindLocal(LocalId),
    Unary(UnaryOp),
    Binary(BinaryOp),
    MakeList(usize),
    MakeRecord(Vec<ConstId>),
    RecordUpdate(Vec<ConstId>),
    GetField(ConstId),
    Call(usize),
    MakeClosure {
        function: FunctionId,
        captures: Vec<CaptureRef>,
    },
    UnsafeBegin,
    UnsafeEnd,
    Receive {
        cases: Vec<ReceiveCase>,
        after: Option<ReceiveAfter>,
    },
    JumpIfFalse(usize),
    Jump(usize),
    Pop,
    Return,
}
