use std::env;
use std::fs;
use std::process::ExitCode;

use rua::frontend::compile_source;
use rua_vm::Vm;

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let Some(path) = args.next() else {
        eprintln!("usage: rua <script.rua>");
        return ExitCode::from(2);
    };

    if args.next().is_some() {
        eprintln!("usage: rua <script.rua>");
        return ExitCode::from(2);
    }

    let source = match fs::read_to_string(&path) {
        Ok(src) => src,
        Err(err) => {
            eprintln!("error reading {path}: {err}");
            return ExitCode::from(1);
        }
    };

    let module = match compile_source(&source) {
        Ok(module) => module,
        Err(err) => {
            eprintln!("compile error: {err}");
            return ExitCode::from(1);
        }
    };

    let mut vm = Vm::new(module);
    match vm.run() {
        Ok(value) => {
            println!("{}", vm.format_value(&value));
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("runtime error: {err}");
            ExitCode::from(1)
        }
    }
}
