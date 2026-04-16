#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FunctionId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConstId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpvalueId(pub usize);
