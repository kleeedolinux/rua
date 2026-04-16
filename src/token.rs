#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    Identifier(String),
    Integer(i64),
    Float(f64),
    String(String),

    Local,
    Fn,
    If,
    Then,
    Else,
    End,
    Receive,
    Case,
    After,
    With,
    True,
    False,
    Nil,
    And,
    Or,
    Not,
    Unsafe,

    LParen,
    RParen,
    LBrace,
    RBrace,
    Comma,
    Dot,
    Concat,

    Plus,
    Minus,
    Arrow,
    Star,
    Slash,
    Percent,

    Assign,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,

    Eof,
}

impl TokenKind {
    pub fn keyword_or_ident(text: &str) -> Self {
        match text {
            "local" => Self::Local,
            "fn" => Self::Fn,
            "if" => Self::If,
            "then" => Self::Then,
            "else" => Self::Else,
            "end" => Self::End,
            "receive" => Self::Receive,
            "case" => Self::Case,
            "after" => Self::After,
            "with" => Self::With,
            "true" => Self::True,
            "false" => Self::False,
            "nil" => Self::Nil,
            "and" => Self::And,
            "or" => Self::Or,
            "not" => Self::Not,
            "unsafe" => Self::Unsafe,
            _ => Self::Identifier(text.to_string()),
        }
    }
}
