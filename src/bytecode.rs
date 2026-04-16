use crate::ast::{BinaryOp, UnaryOp};
use crate::ir::{
    CaptureRef, ConstId, Constant, Function, FunctionId, Instr, LocalId, Module, Pattern,
    ReceiveAfter, ReceiveCase, UpvalueId,
};

pub const RUA_BYTECODE_MAGIC: &[u8; 4] = b"RUAC";
pub const RUA_BYTECODE_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BytecodeError {
    InvalidMagic,
    UnsupportedVersion(u16),
    Truncated,
    InvalidTag(&'static str, u8),
    InvalidUtf8,
    Validation(String),
}

impl std::fmt::Display for BytecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidMagic => write!(f, "invalid bytecode magic"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported bytecode version: {v}"),
            Self::Truncated => write!(f, "truncated bytecode stream"),
            Self::InvalidTag(kind, tag) => write!(f, "invalid {kind} tag: {tag}"),
            Self::InvalidUtf8 => write!(f, "invalid UTF-8 in bytecode"),
            Self::Validation(msg) => write!(f, "bytecode validation failed: {msg}"),
        }
    }
}

impl std::error::Error for BytecodeError {}

pub fn encode_module(module: &Module) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(RUA_BYTECODE_MAGIC);
    write_u16(&mut out, RUA_BYTECODE_VERSION);
    write_u32(&mut out, module.functions.len() as u32);
    write_u32(&mut out, module.entry.0 as u32);

    for function in &module.functions {
        write_opt_string(&mut out, function.name.as_deref());
        write_u32(&mut out, function.arity as u32);
        write_u32(&mut out, function.upvalue_count as u32);
        write_u32(&mut out, function.constants.len() as u32);
        for constant in &function.constants {
            encode_constant(&mut out, constant);
        }
        write_u32(&mut out, function.code.len() as u32);
        for instr in &function.code {
            encode_instr(&mut out, instr);
        }
    }
    out
}

pub fn decode_module(bytes: &[u8]) -> Result<Module, BytecodeError> {
    let mut rd = Reader::new(bytes);
    let mut magic = [0u8; 4];
    for b in &mut magic {
        *b = rd.read_u8()?;
    }
    if &magic != RUA_BYTECODE_MAGIC {
        return Err(BytecodeError::InvalidMagic);
    }
    let version = rd.read_u16()?;
    if version != RUA_BYTECODE_VERSION {
        return Err(BytecodeError::UnsupportedVersion(version));
    }

    let fn_count = rd.read_u32()? as usize;
    let entry = FunctionId(rd.read_u32()? as usize);
    let mut functions = Vec::with_capacity(fn_count);
    for _ in 0..fn_count {
        let name = rd.read_opt_string()?;
        let arity = rd.read_u32()? as usize;
        let upvalue_count = rd.read_u32()? as usize;
        let const_count = rd.read_u32()? as usize;
        let mut constants = Vec::with_capacity(const_count);
        for _ in 0..const_count {
            constants.push(decode_constant(&mut rd)?);
        }
        let code_len = rd.read_u32()? as usize;
        let mut code = Vec::with_capacity(code_len);
        for _ in 0..code_len {
            code.push(decode_instr(&mut rd)?);
        }
        functions.push(Function {
            name,
            arity,
            upvalue_count,
            constants,
            code,
        });
    }

    let module = Module { functions, entry };
    validate_module(&module)?;
    Ok(module)
}

pub fn validate_module(module: &Module) -> Result<(), BytecodeError> {
    if module.functions.is_empty() {
        return Err(BytecodeError::Validation("module has no functions".into()));
    }
    if module.entry.0 >= module.functions.len() {
        return Err(BytecodeError::Validation("entry function out of bounds".into()));
    }

    for (fidx, func) in module.functions.iter().enumerate() {
        for instr in &func.code {
            match instr {
                Instr::LoadConst(id) | Instr::LoadGlobal(id) | Instr::GetField(id) => {
                    if id.0 >= func.constants.len() {
                        return Err(BytecodeError::Validation(format!(
                            "function {fidx}: constant index {} out of bounds",
                            id.0
                        )));
                    }
                }
                Instr::BindLocal(LocalId(_))
                | Instr::LoadLocal(LocalId(_))
                | Instr::LoadUpvalue(UpvalueId(_)) => {}
                Instr::Jump(target) | Instr::JumpIfFalse(target) => {
                    if *target >= func.code.len() {
                        return Err(BytecodeError::Validation(format!(
                            "function {fidx}: jump target {target} out of bounds"
                        )));
                    }
                }
                Instr::MakeClosure { function, .. } => {
                    if function.0 >= module.functions.len() {
                        return Err(BytecodeError::Validation(format!(
                            "function {fidx}: closure function {} out of bounds",
                            function.0
                        )));
                    }
                }
                Instr::Receive { cases, after } => {
                    for case in cases {
                        if case.handler.0 >= module.functions.len() {
                            return Err(BytecodeError::Validation(format!(
                                "function {fidx}: receive case handler {} out of bounds",
                                case.handler.0
                            )));
                        }
                    }
                    if let Some(a) = after {
                        if a.timeout_handler.0 >= module.functions.len()
                            || a.body_handler.0 >= module.functions.len()
                        {
                            return Err(BytecodeError::Validation(format!(
                                "function {fidx}: receive after handler out of bounds"
                            )));
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn encode_constant(out: &mut Vec<u8>, c: &Constant) {
    match c {
        Constant::Integer(v) => {
            out.push(0);
            write_i64(out, *v);
        }
        Constant::Float(v) => {
            out.push(1);
            write_f64(out, *v);
        }
        Constant::String(v) => {
            out.push(2);
            write_string(out, v);
        }
        Constant::Symbol(v) => {
            out.push(3);
            write_string(out, v);
        }
    }
}

fn decode_constant(rd: &mut Reader<'_>) -> Result<Constant, BytecodeError> {
    match rd.read_u8()? {
        0 => Ok(Constant::Integer(rd.read_i64()?)),
        1 => Ok(Constant::Float(rd.read_f64()?)),
        2 => Ok(Constant::String(rd.read_string()?)),
        3 => Ok(Constant::Symbol(rd.read_string()?)),
        tag => Err(BytecodeError::InvalidTag("constant", tag)),
    }
}

fn encode_capture(out: &mut Vec<u8>, c: &CaptureRef) {
    match c {
        CaptureRef::Local(LocalId(id)) => {
            out.push(0);
            write_u32(out, *id as u32);
        }
        CaptureRef::Upvalue(UpvalueId(id)) => {
            out.push(1);
            write_u32(out, *id as u32);
        }
        CaptureRef::SelfClosure => out.push(2),
    }
}

fn decode_capture(rd: &mut Reader<'_>) -> Result<CaptureRef, BytecodeError> {
    match rd.read_u8()? {
        0 => Ok(CaptureRef::Local(LocalId(rd.read_u32()? as usize))),
        1 => Ok(CaptureRef::Upvalue(UpvalueId(rd.read_u32()? as usize))),
        2 => Ok(CaptureRef::SelfClosure),
        tag => Err(BytecodeError::InvalidTag("capture", tag)),
    }
}

fn encode_pattern(out: &mut Vec<u8>, p: &Pattern) {
    match p {
        Pattern::Wildcard => out.push(0),
        Pattern::Binding => out.push(1),
        Pattern::Literal(ConstId(id)) => {
            out.push(2);
            write_u32(out, *id as u32);
        }
        Pattern::Bool(v) => {
            out.push(3);
            out.push(u8::from(*v));
        }
        Pattern::Nil => out.push(4),
        Pattern::Record(fields) => {
            out.push(5);
            write_u32(out, fields.len() as u32);
            for (k, sub) in fields {
                write_u32(out, k.0 as u32);
                encode_pattern(out, sub);
            }
        }
    }
}

fn decode_pattern(rd: &mut Reader<'_>) -> Result<Pattern, BytecodeError> {
    match rd.read_u8()? {
        0 => Ok(Pattern::Wildcard),
        1 => Ok(Pattern::Binding),
        2 => Ok(Pattern::Literal(ConstId(rd.read_u32()? as usize))),
        3 => Ok(Pattern::Bool(rd.read_u8()? != 0)),
        4 => Ok(Pattern::Nil),
        5 => {
            let n = rd.read_u32()? as usize;
            let mut fields = Vec::with_capacity(n);
            for _ in 0..n {
                let k = ConstId(rd.read_u32()? as usize);
                let sub = decode_pattern(rd)?;
                fields.push((k, sub));
            }
            Ok(Pattern::Record(fields))
        }
        tag => Err(BytecodeError::InvalidTag("pattern", tag)),
    }
}

fn encode_unary(out: &mut Vec<u8>, op: UnaryOp) {
    out.push(match op {
        UnaryOp::Neg => 0,
        UnaryOp::Not => 1,
    });
}

fn decode_unary(rd: &mut Reader<'_>) -> Result<UnaryOp, BytecodeError> {
    match rd.read_u8()? {
        0 => Ok(UnaryOp::Neg),
        1 => Ok(UnaryOp::Not),
        tag => Err(BytecodeError::InvalidTag("unary op", tag)),
    }
}

fn encode_binary(out: &mut Vec<u8>, op: BinaryOp) {
    out.push(match op {
        BinaryOp::Add => 0,
        BinaryOp::Concat => 1,
        BinaryOp::Sub => 2,
        BinaryOp::Mul => 3,
        BinaryOp::Div => 4,
        BinaryOp::Mod => 5,
        BinaryOp::Eq => 6,
        BinaryOp::Ne => 7,
        BinaryOp::Lt => 8,
        BinaryOp::Le => 9,
        BinaryOp::Gt => 10,
        BinaryOp::Ge => 11,
        BinaryOp::And => 12,
        BinaryOp::Or => 13,
    });
}

fn decode_binary(rd: &mut Reader<'_>) -> Result<BinaryOp, BytecodeError> {
    match rd.read_u8()? {
        0 => Ok(BinaryOp::Add),
        1 => Ok(BinaryOp::Concat),
        2 => Ok(BinaryOp::Sub),
        3 => Ok(BinaryOp::Mul),
        4 => Ok(BinaryOp::Div),
        5 => Ok(BinaryOp::Mod),
        6 => Ok(BinaryOp::Eq),
        7 => Ok(BinaryOp::Ne),
        8 => Ok(BinaryOp::Lt),
        9 => Ok(BinaryOp::Le),
        10 => Ok(BinaryOp::Gt),
        11 => Ok(BinaryOp::Ge),
        12 => Ok(BinaryOp::And),
        13 => Ok(BinaryOp::Or),
        tag => Err(BytecodeError::InvalidTag("binary op", tag)),
    }
}

fn encode_instr(out: &mut Vec<u8>, i: &Instr) {
    match i {
        Instr::LoadConst(ConstId(id)) => {
            out.push(0);
            write_u32(out, *id as u32);
        }
        Instr::PushBool(v) => {
            out.push(1);
            out.push(u8::from(*v));
        }
        Instr::PushNil => out.push(2),
        Instr::LoadLocal(LocalId(id)) => {
            out.push(3);
            write_u32(out, *id as u32);
        }
        Instr::LoadUpvalue(UpvalueId(id)) => {
            out.push(4);
            write_u32(out, *id as u32);
        }
        Instr::LoadSelf => out.push(5),
        Instr::LoadGlobal(ConstId(id)) => {
            out.push(6);
            write_u32(out, *id as u32);
        }
        Instr::BindLocal(LocalId(id)) => {
            out.push(7);
            write_u32(out, *id as u32);
        }
        Instr::Unary(op) => {
            out.push(8);
            encode_unary(out, *op);
        }
        Instr::Binary(op) => {
            out.push(9);
            encode_binary(out, *op);
        }
        Instr::MakeList(n) => {
            out.push(10);
            write_u32(out, *n as u32);
        }
        Instr::MakeRecord(fields) => {
            out.push(11);
            write_u32(out, fields.len() as u32);
            for field in fields {
                write_u32(out, field.0 as u32);
            }
        }
        Instr::RecordUpdate(fields) => {
            out.push(12);
            write_u32(out, fields.len() as u32);
            for field in fields {
                write_u32(out, field.0 as u32);
            }
        }
        Instr::GetField(ConstId(id)) => {
            out.push(13);
            write_u32(out, *id as u32);
        }
        Instr::Call(argc) => {
            out.push(14);
            write_u32(out, *argc as u32);
        }
        Instr::MakeClosure { function, captures } => {
            out.push(15);
            write_u32(out, function.0 as u32);
            write_u32(out, captures.len() as u32);
            for capture in captures {
                encode_capture(out, capture);
            }
        }
        Instr::UnsafeBegin => out.push(16),
        Instr::UnsafeEnd => out.push(17),
        Instr::Receive { cases, after } => {
            out.push(18);
            write_u32(out, cases.len() as u32);
            for case in cases {
                encode_pattern(out, &case.pattern);
                write_u32(out, case.bindings as u32);
                write_u32(out, case.handler.0 as u32);
                write_u32(out, case.captures.len() as u32);
                for capture in &case.captures {
                    encode_capture(out, capture);
                }
            }
            match after {
                Some(after) => {
                    out.push(1);
                    write_u32(out, after.timeout_handler.0 as u32);
                    write_u32(out, after.timeout_captures.len() as u32);
                    for c in &after.timeout_captures {
                        encode_capture(out, c);
                    }
                    write_u32(out, after.body_handler.0 as u32);
                    write_u32(out, after.body_captures.len() as u32);
                    for c in &after.body_captures {
                        encode_capture(out, c);
                    }
                }
                None => out.push(0),
            }
        }
        Instr::JumpIfFalse(target) => {
            out.push(19);
            write_u32(out, *target as u32);
        }
        Instr::Jump(target) => {
            out.push(20);
            write_u32(out, *target as u32);
        }
        Instr::Pop => out.push(21),
        Instr::Return => out.push(22),
    }
}

fn decode_instr(rd: &mut Reader<'_>) -> Result<Instr, BytecodeError> {
    match rd.read_u8()? {
        0 => Ok(Instr::LoadConst(ConstId(rd.read_u32()? as usize))),
        1 => Ok(Instr::PushBool(rd.read_u8()? != 0)),
        2 => Ok(Instr::PushNil),
        3 => Ok(Instr::LoadLocal(LocalId(rd.read_u32()? as usize))),
        4 => Ok(Instr::LoadUpvalue(UpvalueId(rd.read_u32()? as usize))),
        5 => Ok(Instr::LoadSelf),
        6 => Ok(Instr::LoadGlobal(ConstId(rd.read_u32()? as usize))),
        7 => Ok(Instr::BindLocal(LocalId(rd.read_u32()? as usize))),
        8 => Ok(Instr::Unary(decode_unary(rd)?)),
        9 => Ok(Instr::Binary(decode_binary(rd)?)),
        10 => Ok(Instr::MakeList(rd.read_u32()? as usize)),
        11 => {
            let n = rd.read_u32()? as usize;
            let mut fields = Vec::with_capacity(n);
            for _ in 0..n {
                fields.push(ConstId(rd.read_u32()? as usize));
            }
            Ok(Instr::MakeRecord(fields))
        }
        12 => {
            let n = rd.read_u32()? as usize;
            let mut fields = Vec::with_capacity(n);
            for _ in 0..n {
                fields.push(ConstId(rd.read_u32()? as usize));
            }
            Ok(Instr::RecordUpdate(fields))
        }
        13 => Ok(Instr::GetField(ConstId(rd.read_u32()? as usize))),
        14 => Ok(Instr::Call(rd.read_u32()? as usize)),
        15 => {
            let function = FunctionId(rd.read_u32()? as usize);
            let n = rd.read_u32()? as usize;
            let mut captures = Vec::with_capacity(n);
            for _ in 0..n {
                captures.push(decode_capture(rd)?);
            }
            Ok(Instr::MakeClosure { function, captures })
        }
        16 => Ok(Instr::UnsafeBegin),
        17 => Ok(Instr::UnsafeEnd),
        18 => {
            let n = rd.read_u32()? as usize;
            let mut cases = Vec::with_capacity(n);
            for _ in 0..n {
                let pattern = decode_pattern(rd)?;
                let bindings = rd.read_u32()? as usize;
                let handler = FunctionId(rd.read_u32()? as usize);
                let cn = rd.read_u32()? as usize;
                let mut captures = Vec::with_capacity(cn);
                for _ in 0..cn {
                    captures.push(decode_capture(rd)?);
                }
                cases.push(ReceiveCase {
                    pattern,
                    bindings,
                    handler,
                    captures,
                });
            }
            let after = match rd.read_u8()? {
                0 => None,
                1 => {
                    let timeout_handler = FunctionId(rd.read_u32()? as usize);
                    let tn = rd.read_u32()? as usize;
                    let mut timeout_captures = Vec::with_capacity(tn);
                    for _ in 0..tn {
                        timeout_captures.push(decode_capture(rd)?);
                    }
                    let body_handler = FunctionId(rd.read_u32()? as usize);
                    let bn = rd.read_u32()? as usize;
                    let mut body_captures = Vec::with_capacity(bn);
                    for _ in 0..bn {
                        body_captures.push(decode_capture(rd)?);
                    }
                    Some(ReceiveAfter {
                        timeout_handler,
                        timeout_captures,
                        body_handler,
                        body_captures,
                    })
                }
                tag => return Err(BytecodeError::InvalidTag("receive-after marker", tag)),
            };
            Ok(Instr::Receive { cases, after })
        }
        19 => Ok(Instr::JumpIfFalse(rd.read_u32()? as usize)),
        20 => Ok(Instr::Jump(rd.read_u32()? as usize)),
        21 => Ok(Instr::Pop),
        22 => Ok(Instr::Return),
        tag => Err(BytecodeError::InvalidTag("instruction", tag)),
    }
}

fn write_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn write_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn write_i64(out: &mut Vec<u8>, v: i64) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn write_f64(out: &mut Vec<u8>, v: f64) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn write_string(out: &mut Vec<u8>, s: &str) {
    write_u32(out, s.len() as u32);
    out.extend_from_slice(s.as_bytes());
}
fn write_opt_string(out: &mut Vec<u8>, s: Option<&str>) {
    match s {
        Some(v) => {
            out.push(1);
            write_string(out, v);
        }
        None => out.push(0),
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn read_exact(&mut self, n: usize) -> Result<&'a [u8], BytecodeError> {
        if self.pos + n > self.bytes.len() {
            return Err(BytecodeError::Truncated);
        }
        let out = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }
    fn read_u8(&mut self) -> Result<u8, BytecodeError> {
        Ok(self.read_exact(1)?[0])
    }
    fn read_u16(&mut self) -> Result<u16, BytecodeError> {
        let mut b = [0u8; 2];
        b.copy_from_slice(self.read_exact(2)?);
        Ok(u16::from_le_bytes(b))
    }
    fn read_u32(&mut self) -> Result<u32, BytecodeError> {
        let mut b = [0u8; 4];
        b.copy_from_slice(self.read_exact(4)?);
        Ok(u32::from_le_bytes(b))
    }
    fn read_i64(&mut self) -> Result<i64, BytecodeError> {
        let mut b = [0u8; 8];
        b.copy_from_slice(self.read_exact(8)?);
        Ok(i64::from_le_bytes(b))
    }
    fn read_f64(&mut self) -> Result<f64, BytecodeError> {
        let mut b = [0u8; 8];
        b.copy_from_slice(self.read_exact(8)?);
        Ok(f64::from_le_bytes(b))
    }
    fn read_string(&mut self) -> Result<String, BytecodeError> {
        let n = self.read_u32()? as usize;
        let bytes = self.read_exact(n)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| BytecodeError::InvalidUtf8)
    }
    fn read_opt_string(&mut self) -> Result<Option<String>, BytecodeError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_string()?)),
            tag => Err(BytecodeError::InvalidTag("optional string marker", tag)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frontend::compile_source;

    #[test]
    fn roundtrip_module() {
        let module = compile_source("local f = fn(x) x + 1 end f(2)").unwrap();
        let bytes = encode_module(&module);
        let decoded = decode_module(&bytes).unwrap();
        assert_eq!(module, decoded);
    }

    #[test]
    fn rejects_bad_magic() {
        let mut bytes = encode_module(&compile_source("1").unwrap());
        bytes[0] = b'X';
        let err = decode_module(&bytes).unwrap_err();
        assert!(matches!(err, BytecodeError::InvalidMagic));
    }
}
