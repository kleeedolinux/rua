mod error;
mod gc;
mod value;
mod vm;

pub use error::VmError;
pub use value::{ObjRef, Value};
pub use vm::{ProcessId, Vm, VmConfig, VmState};

#[cfg(test)]
mod tests {
    use rua::frontend::compile_source;

    use crate::{Value, Vm};

    fn eval(source: &str) -> Value {
        let module = compile_source(source).expect("compile should succeed");
        let mut vm = Vm::new(module);
        vm.run().expect("vm run should succeed")
    }

    #[test]
    fn evals_arithmetic_and_if() {
        let value = eval("if 1 + 1 == 2 then 40 + 2 else 0 end");
        assert_eq!(value, Value::Integer(42));
    }

    #[test]
    fn evals_function_and_closure_capture() {
        let value = eval("local x = 10 local f = fn(a) a + x end f(5)");
        assert_eq!(value, Value::Integer(15));
    }

    #[test]
    fn evals_records_and_with_meta_lookup() {
        let source = "local pessoa = with_meta({ nome = \"Lia\" }, { show = fn(self) \"Pessoa(\" .. self.nome .. \")\" end }) pessoa.show(pessoa)";
        let value = eval(source);
        assert_eq!(value, Value::String("Pessoa(Lia)".into()));
    }

    #[test]
    fn receives_message_with_pattern() {
        let source = "send(self(), { type = \"ping\", value = 7 }) receive case { type = \"ping\", value = x } -> x end";
        let value = eval(source);
        assert_eq!(value, Value::Integer(7));
    }

    #[test]
    fn spawn_process_executes_and_replies() {
        let source = "local parent = self() spawn(fn() send(parent, { type = \"done\", value = 42 }) end) receive case { type = \"done\", value = x } -> x end";
        let value = eval(source);
        assert_eq!(value, Value::Integer(42));
    }

    #[test]
    fn unsafe_ffi_calls_host_function() {
        let module = compile_source("unsafe ffi(\"host_add\", 40, 2)").unwrap();
        let mut vm = Vm::new(module);
        vm.register_host_function("host_add", |args| {
            let a = match args.first() {
                Some(Value::Integer(v)) => *v,
                _ => 0,
            };
            let b = match args.get(1) {
                Some(Value::Integer(v)) => *v,
                _ => 0,
            };
            Ok(Value::Integer(a + b))
        });
        let value = vm.run().unwrap();
        assert_eq!(value, Value::Integer(42));
    }

    #[test]
    fn unsafe_os_spawn_works() {
        let source = "local parent = self() unsafe os.spawn(fn() send(parent, { type = \"done\", value = 9 }) end) receive case { type = \"done\", value = x } -> x end";
        let value = eval(source);
        assert_eq!(value, Value::Integer(9));
    }

    #[test]
    fn receive_after_respects_timeout() {
        let source = "receive case { type = \"x\" } -> 1 after 3 -> 9 end";
        let value = eval(source);
        assert_eq!(value, Value::Integer(9));
    }

    #[test]
    fn receive_after_prefers_message_before_timeout() {
        let source = "send(self(), { type = \"x\", value = 7 }) receive case { type = \"x\", value = v } -> v after 100 -> 0 end";
        let value = eval(source);
        assert_eq!(value, Value::Integer(7));
    }

    #[test]
    fn monitor_receives_down_message() {
        let source = "local p = spawn(fn() exit(\"boom\") end) local r = monitor(p) receive case { type = \"DOWN\", ref = ref, pid = pid, reason = reason } -> reason end";
        let value = eval(source);
        assert_eq!(value, Value::String("boom".into()));
    }

    #[test]
    fn supervisor_restarts_permanent_child() {
        let source = "local parent = self() local worker = fn() send(parent, { type = \"up\" }) exit(\"boom\") end local _pid = supervisor(worker, \"permanent\", 1, 100) local a = receive case { type = \"up\" } -> 1 end local b = receive case { type = \"up\" } -> 2 end a + b";
        let value = eval(source);
        assert_eq!(value, Value::Integer(3));
    }

    #[test]
    fn require_loads_registered_module() {
        let module = compile_source("local m = require(\"m\") m + 1").unwrap();
        let mut vm = Vm::new(module);
        let m = rua::frontend::compile_source("41").unwrap();
        let mut module_vm = Vm::new(m);
        let mv = module_vm.run().unwrap();
        vm.register_native_module("m", mv);
        let value = vm.run().unwrap();
        assert_eq!(value, Value::Integer(42));
    }
}
