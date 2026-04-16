#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObjRef(pub u32);

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Nil,
    Pid(u64),
    List(ObjRef),
    Record(ObjRef),
    Closure(ObjRef),
    Builtin(Builtin),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Builtin {
    Print,
    SelfPid,
    Send,
    Spawn,
    SpawnLink,
    SpawnMonitor,
    OsSpawn,
    Exit,
    OsExit,
    Link,
    Unlink,
    Monitor,
    Demonitor,
    Register,
    Unregister,
    WhereIs,
    Supervisor,
    WithMeta,
    GetMeta,
    Require,
    MathAbs,
    MathMax,
    MathMin,
    MathSqrt,
    StringLen,
    StringLower,
    StringUpper,
    TableLen,
    Ffi,
}

impl Value {
    pub fn truthy(&self) -> Result<bool, String> {
        match self {
            Self::Bool(v) => Ok(*v),
            _ => Err("logical operations require booleans".into()),
        }
    }

    pub fn to_concat_string(&self) -> String {
        match self {
            Self::Integer(v) => v.to_string(),
            Self::Float(v) => v.to_string(),
            Self::String(v) => v.clone(),
            Self::Bool(v) => v.to_string(),
            Self::Nil => "nil".into(),
            Self::Pid(v) => format!("pid({v})"),
            Self::List(id) => format!("[list#{}]", id.0),
            Self::Record(id) => format!("{{record#{}}}", id.0),
            Self::Closure(id) => format!("<closure#{}>", id.0),
            Self::Builtin(_) => "<builtin>".into(),
        }
    }
}
