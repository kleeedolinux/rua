use std::{env, fs};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use rua::bytecode::decode_module;
use rua::frontend::compile_source;
use rua_vm::{FfiSignature, FfiType, Vm};

#[derive(Debug, Clone)]
struct CliFfiCap {
    name: String,
    lib: String,
    symbol: String,
    ret: FfiType,
    params: Vec<FfiType>,
}

fn parse_ffi_type(s: &str) -> Option<FfiType> {
    match s {
        "i64" => Some(FfiType::Int64),
        "u64" => Some(FfiType::UInt64),
        "bool" => Some(FfiType::Bool),
        "cstring" => Some(FfiType::CString),
        "ptr" => Some(FfiType::Ptr),
        "void" => Some(FfiType::Void),
        _ => None,
    }
}

fn parse_cap(spec: &str) -> Result<CliFfiCap, String> {
    let (name, rhs) = spec
        .split_once('=')
        .ok_or_else(|| "ffi cap must be name=lib:symbol:ret:param1,param2".to_string())?;
    let mut parts = rhs.splitn(4, ':');
    let lib = parts.next().ok_or_else(|| "missing lib".to_string())?;
    let symbol = parts.next().ok_or_else(|| "missing symbol".to_string())?;
    let ret_s = parts.next().ok_or_else(|| "missing ret".to_string())?;
    let params_csv = parts.next().unwrap_or("");
    let ret = parse_ffi_type(ret_s).ok_or_else(|| format!("invalid ret type: {ret_s}"))?;
    let mut params = Vec::new();
    for p in params_csv.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        let ty = parse_ffi_type(p).ok_or_else(|| format!("invalid param type: {p}"))?;
        params.push(ty);
    }
    Ok(CliFfiCap {
        name: name.to_string(),
        lib: lib.to_string(),
        symbol: symbol.to_string(),
        ret,
        params,
    })
}

fn main() -> ExitCode {
    let mut args = env::args().skip(1).peekable();
    let Some(path) = args.next() else {
        eprintln!("usage: rua <script.rua|script.ruac> [--module-path PATH]* [--ffi-cap SPEC]* [--allow-unrestricted-ffi]");
        return ExitCode::from(2);
    };
    let mut extra_module_paths = Vec::new();
    let mut ffi_caps = Vec::new();
    let mut allow_unrestricted_ffi = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--module-path" => {
                let Some(path) = args.next() else {
                    eprintln!("missing value for --module-path");
                    return ExitCode::from(2);
                };
                extra_module_paths.push(PathBuf::from(path));
            }
            "--ffi-cap" => {
                let Some(spec) = args.next() else {
                    eprintln!("missing value for --ffi-cap");
                    return ExitCode::from(2);
                };
                match parse_cap(&spec) {
                    Ok(cap) => ffi_caps.push(cap),
                    Err(e) => {
                        eprintln!("invalid --ffi-cap: {e}");
                        return ExitCode::from(2);
                    }
                }
            }
            "--allow-unrestricted-ffi" => {
                allow_unrestricted_ffi = true;
            }
            _ => {
                eprintln!("unknown arg: {arg}");
                return ExitCode::from(2);
            }
        }
    }

    let bytes = match fs::read(&path) {
        Ok(src) => src,
        Err(err) => {
            eprintln!("error reading {path}: {err}");
            return ExitCode::from(1);
        }
    };

    let module = if Path::new(&path)
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("ruac"))
    {
        match decode_module(&bytes) {
            Ok(m) => m,
            Err(err) => {
                eprintln!("bytecode decode error: {err}");
                return ExitCode::from(1);
            }
        }
    } else {
        let source = match String::from_utf8(bytes) {
            Ok(v) => v,
            Err(_) => {
                eprintln!("source file is not UTF-8");
                return ExitCode::from(1);
            }
        };
        match compile_source(&source) {
            Ok(module) => module,
            Err(err) => {
                eprintln!("compile error: {err}");
                return ExitCode::from(1);
            }
        }
    };

    let mut vm = Vm::new(module);
    let script_path = PathBuf::from(&path);
    let script_dir = script_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    vm.add_module_search_path(script_dir.clone());
    vm.add_module_search_path(script_dir.join("modules"));
    for p in extra_module_paths {
        vm.add_module_search_path(p);
    }
    vm.set_allow_unrestricted_system_ffi(allow_unrestricted_ffi);
    for cap in ffi_caps {
        vm.register_system_ffi_capability(
            cap.name,
            cap.lib,
            cap.symbol,
            FfiSignature {
                params: cap.params,
                ret: cap.ret,
            },
        );
    }

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
