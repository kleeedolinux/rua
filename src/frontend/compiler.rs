use std::collections::HashMap;

use crate::ast::{Expr, Item, Pattern as AstPattern, Program, ReceiveAfter as AstReceiveAfter, ReceiveCase as AstReceiveCase};
use crate::ir::{
    CaptureRef, ConstId, Constant, Function, FunctionId, Instr, LocalId, Module,
    Pattern as IrPattern, ReceiveAfter as IrReceiveAfter, ReceiveCase as IrReceiveCase, UpvalueId,
};
use crate::parser::parse_program;

use super::error::FrontendError;

pub fn compile_source(source: &str) -> Result<Module, FrontendError> {
    let program = parse_program(source)?;
    Ok(compile_program(&program))
}

pub fn compile_program(program: &Program) -> Module {
    let mut builder = ModuleBuilder::new();
    let entry = builder.compile_entry(program);
    Module {
        functions: builder.functions,
        entry,
    }
}

#[derive(Clone, Copy)]
enum Resolved {
    Local(LocalId),
    Upvalue(UpvalueId),
}

#[derive(Default)]
struct Scope {
    frames: Vec<HashMap<String, LocalId>>,
    next_local: usize,
    parent: Option<ScopeRef>,
}

#[derive(Clone)]
struct ScopeRef {
    locals: HashMap<String, LocalId>,
    upvalues: HashMap<String, UpvalueId>,
}

struct FunctionBuilder {
    name: Option<String>,
    arity: usize,
    upvalue_count: usize,
    constants: Vec<Constant>,
    code: Vec<Instr>,
    scope: Scope,
    upvalue_map: HashMap<String, UpvalueId>,
    captures: Vec<CaptureRef>,
}

impl FunctionBuilder {
    fn new(name: Option<String>, arity: usize, parent: Option<ScopeRef>) -> Self {
        let mut scope = Scope {
            frames: vec![HashMap::new()],
            next_local: 0,
            parent,
        };

        for i in 0..arity {
            scope.next_local = i + 1;
        }

        Self {
            name,
            arity,
            upvalue_count: 0,
            constants: Vec::new(),
            code: Vec::new(),
            scope,
            upvalue_map: HashMap::new(),
            captures: Vec::new(),
        }
    }

    fn add_const(&mut self, c: Constant) -> ConstId {
        let id = ConstId(self.constants.len());
        self.constants.push(c);
        id
    }

    fn emit(&mut self, instr: Instr) -> usize {
        self.code.push(instr);
        self.code.len() - 1
    }

    fn patch_jump_target(&mut self, at: usize, target: usize) {
        match self.code.get_mut(at) {
            Some(Instr::JumpIfFalse(t)) => *t = target,
            Some(Instr::Jump(t)) => *t = target,
            _ => panic!("attempted to patch non-jump opcode"),
        }
    }

    fn reserve_local(&mut self, name: String) -> LocalId {
        let local = LocalId(self.scope.next_local);
        self.scope.next_local += 1;
        self.scope
            .frames
            .last_mut()
            .expect("scope has at least one frame")
            .insert(name, local);
        local
    }

    fn push_scope(&mut self) {
        self.scope.frames.push(HashMap::new());
    }

    fn pop_scope(&mut self) {
        let popped = self.scope.frames.pop();
        assert!(popped.is_some(), "scope pop on empty frame stack");
        if self.scope.frames.is_empty() {
            self.scope.frames.push(HashMap::new());
        }
    }

    fn resolve(&mut self, name: &str) -> Option<Resolved> {
        for frame in self.scope.frames.iter().rev() {
            if let Some(local) = frame.get(name).copied() {
                return Some(Resolved::Local(local));
            }
        }

        let parent = self.scope.parent.as_ref()?;

        if let Some(local) = parent.locals.get(name).copied() {
            let upvalue = self.intern_upvalue(name.to_string(), CaptureRef::Local(local));
            return Some(Resolved::Upvalue(upvalue));
        }

        if let Some(upvalue) = parent.upvalues.get(name).copied() {
            let mapped = self.intern_upvalue(name.to_string(), CaptureRef::Upvalue(upvalue));
            return Some(Resolved::Upvalue(mapped));
        }

        None
    }

    fn intern_upvalue(&mut self, name: String, capture: CaptureRef) -> UpvalueId {
        if let Some(id) = self.upvalue_map.get(&name).copied() {
            return id;
        }

        let id = UpvalueId(self.upvalue_count);
        self.upvalue_count += 1;
        self.upvalue_map.insert(name, id);
        self.captures.push(capture);
        id
    }

    fn to_scope_ref(&self) -> ScopeRef {
        let mut locals = HashMap::new();
        for frame in &self.scope.frames {
            for (k, v) in frame {
                locals.insert(k.clone(), *v);
            }
        }
        ScopeRef {
            locals,
            upvalues: self.upvalue_map.clone(),
        }
    }

    fn finish(mut self) -> Function {
        if !matches!(self.code.last(), Some(Instr::Return)) {
            self.emit(Instr::Return);
        }

        Function {
            name: self.name,
            arity: self.arity,
            upvalue_count: self.upvalue_count,
            constants: self.constants,
            code: self.code,
        }
    }
}

struct ModuleBuilder {
    functions: Vec<Function>,
}

impl ModuleBuilder {
    fn new() -> Self {
        Self {
            functions: Vec::new(),
        }
    }

    fn compile_entry(&mut self, program: &Program) -> FunctionId {
        let mut fb = FunctionBuilder::new(Some("<entry>".into()), 0, None);
        self.compile_items(&mut fb, &program.items);
        let entry_id = FunctionId(self.functions.len());
        self.functions.push(fb.finish());
        entry_id
    }

    fn compile_items(&mut self, fb: &mut FunctionBuilder, items: &[Item]) {
        self.compile_items_value(fb, items);
        fb.emit(Instr::Return);
    }

    fn compile_items_value(&mut self, fb: &mut FunctionBuilder, items: &[Item]) {
        let last_expr_index = items
            .iter()
            .enumerate()
            .filter(|(_, item)| matches!(item, Item::Expr(_)))
            .map(|(i, _)| i)
            .next_back();

        for (idx, item) in items.iter().enumerate() {
            match item {
                Item::Local(binding) => {
                    let local = fb.reserve_local(binding.name.clone());
                    self.compile_expr(fb, &binding.value);
                    fb.emit(Instr::BindLocal(local));
                }
                Item::Expr(expr) => {
                    self.compile_expr(fb, expr);
                    if Some(idx) != last_expr_index {
                        fb.emit(Instr::Pop);
                    }
                }
            }
        }

        if last_expr_index.is_none() {
            fb.emit(Instr::PushNil);
        }
    }

    fn compile_expr(&mut self, fb: &mut FunctionBuilder, expr: &Expr) {
        match expr {
            Expr::Integer(v) => {
                let c = fb.add_const(Constant::Integer(*v));
                fb.emit(Instr::LoadConst(c));
            }
            Expr::Float(v) => {
                let c = fb.add_const(Constant::Float(*v));
                fb.emit(Instr::LoadConst(c));
            }
            Expr::String(v) => {
                let c = fb.add_const(Constant::String(v.clone()));
                fb.emit(Instr::LoadConst(c));
            }
            Expr::Bool(v) => {
                fb.emit(Instr::PushBool(*v));
            }
            Expr::Nil => {
                fb.emit(Instr::PushNil);
            }
            Expr::Identifier(name) => {
                if let Some(resolved) = fb.resolve(name) {
                    match resolved {
                        Resolved::Local(local) => {
                            fb.emit(Instr::LoadLocal(local));
                        }
                        Resolved::Upvalue(upvalue) => {
                            fb.emit(Instr::LoadUpvalue(upvalue));
                        }
                    }
                } else {
                    let c = fb.add_const(Constant::Symbol(name.clone()));
                    fb.emit(Instr::LoadGlobal(c));
                }
            }
            Expr::Fn { params, body } => {
                let mut nested = FunctionBuilder::new(None, params.len(), Some(fb.to_scope_ref()));
                for (i, param) in params.iter().enumerate() {
                    nested
                        .scope
                        .frames
                        .last_mut()
                        .expect("scope has at least one frame")
                        .insert(param.clone(), LocalId(i));
                }

                self.compile_expr(&mut nested, body);
                nested.emit(Instr::Return);

                let captures = nested.captures.clone();
                let func_id = FunctionId(self.functions.len());
                self.functions.push(nested.finish());
                fb.emit(Instr::MakeClosure {
                    function: func_id,
                    captures,
                });
            }
            Expr::If {
                condition,
                then_branch,
                else_branch,
            } => {
                self.compile_expr(fb, condition);
                let jmp_false = fb.emit(Instr::JumpIfFalse(usize::MAX));
                self.compile_expr(fb, then_branch);
                let jmp_end = fb.emit(Instr::Jump(usize::MAX));
                let else_target = fb.code.len();
                fb.patch_jump_target(jmp_false, else_target);
                self.compile_expr(fb, else_branch);
                let end_target = fb.code.len();
                fb.patch_jump_target(jmp_end, end_target);
            }
            Expr::Unsafe(inner) => {
                fb.emit(Instr::UnsafeBegin);
                self.compile_expr(fb, inner);
                fb.emit(Instr::UnsafeEnd);
            }
            Expr::Receive { cases, after } => {
                let compiled_cases = cases
                    .iter()
                    .map(|c| self.compile_receive_case(fb, c))
                    .collect::<Vec<_>>();
                let compiled_after = after
                    .as_ref()
                    .map(|a| self.compile_receive_after(fb, a));
                fb.emit(Instr::Receive {
                    cases: compiled_cases,
                    after: compiled_after,
                });
            }
            Expr::Block(items) => {
                fb.push_scope();
                self.compile_items_value(fb, items);
                fb.pop_scope();
            }
            Expr::Unary { op, expr } => {
                self.compile_expr(fb, expr);
                fb.emit(Instr::Unary(*op));
            }
            Expr::Binary { left, op, right } => match op {
                crate::ast::BinaryOp::And => {
                    self.compile_expr(fb, left);
                    let jmp_false = fb.emit(Instr::JumpIfFalse(usize::MAX));
                    fb.emit(Instr::Pop);
                    self.compile_expr(fb, right);
                    let end = fb.code.len();
                    fb.patch_jump_target(jmp_false, end);
                }
                crate::ast::BinaryOp::Or => {
                    self.compile_expr(fb, left);
                    let jmp_false = fb.emit(Instr::JumpIfFalse(usize::MAX));
                    let jmp_end = fb.emit(Instr::Jump(usize::MAX));
                    let right_start = fb.code.len();
                    fb.patch_jump_target(jmp_false, right_start);
                    fb.emit(Instr::Pop);
                    self.compile_expr(fb, right);
                    let end = fb.code.len();
                    fb.patch_jump_target(jmp_end, end);
                }
                _ => {
                    self.compile_expr(fb, left);
                    self.compile_expr(fb, right);
                    fb.emit(Instr::Binary(*op));
                }
            },
            Expr::Call { callee, args } => {
                self.compile_expr(fb, callee);
                for arg in args {
                    self.compile_expr(fb, arg);
                }
                fb.emit(Instr::Call(args.len()));
            }
            Expr::FieldAccess { expr, field } => {
                self.compile_expr(fb, expr);
                let field_id = fb.add_const(Constant::Symbol(field.clone()));
                fb.emit(Instr::GetField(field_id));
            }
            Expr::List(items) => {
                for item in items {
                    self.compile_expr(fb, item);
                }
                fb.emit(Instr::MakeList(items.len()));
            }
            Expr::Record(fields) => {
                let mut names = Vec::with_capacity(fields.len());
                for (name, value) in fields {
                    self.compile_expr(fb, value);
                    names.push(fb.add_const(Constant::Symbol(name.clone())));
                }
                fb.emit(Instr::MakeRecord(names));
            }
            Expr::RecordUpdate { base, updates } => {
                self.compile_expr(fb, base);
                let mut names = Vec::with_capacity(updates.len());
                for (name, value) in updates {
                    self.compile_expr(fb, value);
                    names.push(fb.add_const(Constant::Symbol(name.clone())));
                }
                fb.emit(Instr::RecordUpdate(names));
            }
        }
    }

    fn compile_receive_case(
        &mut self,
        fb: &mut FunctionBuilder,
        case: &AstReceiveCase,
    ) -> IrReceiveCase {
        let mut bindings = Vec::new();
        let pattern = self.compile_pattern(fb, &case.pattern, &mut bindings);
        let handler = self.compile_case_handler(fb, &bindings, &case.body);
        IrReceiveCase {
            pattern,
            bindings: bindings.len(),
            handler,
        }
    }

    fn compile_receive_after(
        &mut self,
        fb: &mut FunctionBuilder,
        after: &AstReceiveAfter,
    ) -> IrReceiveAfter {
        let timeout_handler = self.compile_case_handler(fb, &[], &after.timeout);
        let body_handler = self.compile_case_handler(fb, &[], &after.body);
        IrReceiveAfter {
            timeout_handler,
            body_handler,
        }
    }

    fn compile_case_handler(
        &mut self,
        fb: &FunctionBuilder,
        bindings: &[String],
        body: &Expr,
    ) -> FunctionId {
        let mut nested = FunctionBuilder::new(None, bindings.len(), Some(fb.to_scope_ref()));
        for (i, name) in bindings.iter().enumerate() {
            nested
                .scope
                .frames
                .last_mut()
                .expect("scope has at least one frame")
                .insert(name.clone(), LocalId(i));
        }
        self.compile_expr(&mut nested, body);
        nested.emit(Instr::Return);
        let func_id = FunctionId(self.functions.len());
        self.functions.push(nested.finish());
        func_id
    }

    fn compile_pattern(
        &mut self,
        fb: &mut FunctionBuilder,
        pattern: &AstPattern,
        bindings: &mut Vec<String>,
    ) -> IrPattern {
        match pattern {
            AstPattern::Wildcard => IrPattern::Wildcard,
            AstPattern::Binding(name) => {
                bindings.push(name.clone());
                IrPattern::Binding
            }
            AstPattern::Integer(v) => IrPattern::Literal(fb.add_const(Constant::Integer(*v))),
            AstPattern::Float(v) => IrPattern::Literal(fb.add_const(Constant::Float(*v))),
            AstPattern::String(v) => IrPattern::Literal(fb.add_const(Constant::String(v.clone()))),
            AstPattern::Bool(v) => IrPattern::Bool(*v),
            AstPattern::Nil => IrPattern::Nil,
            AstPattern::Record(fields) => {
                let mut out = Vec::with_capacity(fields.len());
                for (name, sub) in fields {
                    let field = fb.add_const(Constant::Symbol(name.clone()));
                    let sub = self.compile_pattern(fb, sub, bindings);
                    out.push((field, sub));
                }
                IrPattern::Record(out)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ir::Instr;

    use super::*;

    fn entry(module: &Module) -> &Function {
        &module.functions[module.entry.0]
    }

    #[test]
    fn compiles_entry_and_return() {
        let module = compile_source("1 + 2").unwrap();
        let entry = entry(&module);
        assert!(entry.code.iter().any(|i| matches!(i, Instr::Binary(_))));
        assert!(matches!(entry.code.last(), Some(Instr::Return)));
    }

    #[test]
    fn compiles_if_to_jumps() {
        let module = compile_source("if true then 1 else 2 end").unwrap();
        let code = &entry(&module).code;
        assert!(code.iter().any(|i| matches!(i, Instr::JumpIfFalse(_))));
        assert!(code.iter().any(|i| matches!(i, Instr::Jump(_))));
    }

    #[test]
    fn compiles_lists_records_and_updates() {
        let module = compile_source("local p = { nome = \"Lia\" } { p with idade = 21 }").unwrap();
        let code = &entry(&module).code;
        assert!(code.iter().any(|i| matches!(i, Instr::MakeRecord(_))));
        assert!(code.iter().any(|i| matches!(i, Instr::RecordUpdate(_))));
    }

    #[test]
    fn compiles_short_circuit_logic() {
        let module = compile_source("a and b or c").unwrap();
        let code = &entry(&module).code;
        let jump_count = code
            .iter()
            .filter(|i| matches!(i, Instr::JumpIfFalse(_) | Instr::Jump(_)))
            .count();
        assert!(jump_count >= 2);
    }

    #[test]
    fn compiles_nested_fn_with_capture() {
        let source = "local x = 10 local f = fn(a) a + x end f(1)";
        let module = compile_source(source).unwrap();
        assert!(module.functions.len() >= 2);

        let nested = &module.functions[0];
        assert_eq!(nested.upvalue_count, 1);
        assert!(nested
            .code
            .iter()
            .any(|i| matches!(i, Instr::LoadUpvalue(_))));

        assert!(entry(&module)
            .code
            .iter()
            .any(|i| matches!(i, Instr::MakeClosure { .. })));
    }

    #[test]
    fn compiles_recursive_binding_capture() {
        let source = "local fat = fn(n) if n == 0 then 1 else n * fat(n - 1) end end fat(5)";
        let module = compile_source(source).unwrap();
        assert!(module.functions.len() >= 2);
        let nested = &module.functions[0];
        assert!(nested.upvalue_count >= 1);
    }

    #[test]
    fn compiles_receive_cases_and_after() {
        let source = "receive case { type = \"ping\", from = from } -> send(from, \"pong\") case _ -> nil after 1000 -> \"timeout\" end";
        let module = compile_source(source).unwrap();
        let entry_code = &entry(&module).code;
        let receive = entry_code
            .iter()
            .find(|i| matches!(i, Instr::Receive { .. }))
            .expect("missing receive instruction");

        match receive {
            Instr::Receive { cases, after } => {
                assert_eq!(cases.len(), 2);
                assert!(cases[0].bindings >= 1);
                assert!(after.is_some());
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn compiles_metatable_style_with_concat() {
        let source = "local pessoa = with_meta({ nome = \"Lia\" }, { show = fn(self) \"Pessoa(\" .. self.nome .. \")\" end })";
        let module = compile_source(source).unwrap();
        let has_concat = module
            .functions
            .iter()
            .flat_map(|f| f.code.iter())
            .any(|i| matches!(i, Instr::Binary(crate::ast::BinaryOp::Concat)));
        assert!(has_concat);
    }

    #[test]
    fn compiles_function_block_with_locals() {
        let source = "local f = fn(x) local y = x + 1 y * 2 end f(10)";
        let module = compile_source(source).unwrap();
        assert!(module.functions.len() >= 2);
        let fn_code = &module.functions[0].code;
        assert!(fn_code.iter().any(|i| matches!(i, Instr::BindLocal(_))));
    }

    #[test]
    fn compiles_receive_case_multiline_body() {
        let source = "receive case { type = \"ping\", from = from } -> send(from, { type = \"pong\" }) nil end";
        let module = compile_source(source).unwrap();
        let entry_code = &entry(&module).code;
        let receive = entry_code
            .iter()
            .find(|i| matches!(i, Instr::Receive { .. }))
            .expect("missing receive instruction");
        match receive {
            Instr::Receive { cases, .. } => {
                let handler = cases[0].handler.0;
                let handler_code = &module.functions[handler].code;
                let pop_count = handler_code
                    .iter()
                    .filter(|i| matches!(i, Instr::Pop))
                    .count();
                assert!(pop_count >= 1);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn compiles_unsafe_markers() {
        let source = "unsafe ffi(\"host_read_file\", \"a.txt\")";
        let module = compile_source(source).unwrap();
        let code = &entry(&module).code;
        assert!(code.iter().any(|i| matches!(i, Instr::UnsafeBegin)));
        assert!(code.iter().any(|i| matches!(i, Instr::UnsafeEnd)));
    }
}
