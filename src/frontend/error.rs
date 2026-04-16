use std::fmt;

use crate::parser::ParseError;

#[derive(Debug, Clone, PartialEq)]
pub enum FrontendError {
    Parse(ParseError),
}

impl fmt::Display for FrontendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for FrontendError {}

impl From<ParseError> for FrontendError {
    fn from(value: ParseError) -> Self {
        Self::Parse(value)
    }
}
