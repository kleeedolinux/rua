use super::ConstId;

#[derive(Debug, Clone, PartialEq)]
pub enum Pattern {
    Wildcard,
    Binding,
    Literal(ConstId),
    Bool(bool),
    Nil,
    Record(Vec<(ConstId, Pattern)>),
}
