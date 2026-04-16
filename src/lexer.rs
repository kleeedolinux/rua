use std::fmt;

use crate::token::{Span, Token, TokenKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LexError {
    pub message: String,
    pub position: usize,
}

impl fmt::Display for LexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at byte {}", self.message, self.position)
    }
}

impl std::error::Error for LexError {}

pub fn lex(input: &str) -> Result<Vec<Token>, LexError> {
    let mut lexer = Lexer::new(input);
    let mut tokens = Vec::new();

    loop {
        let token = lexer.next_token()?;
        let is_eof = matches!(token.kind, TokenKind::Eof);
        tokens.push(token);
        if is_eof {
            break;
        }
    }

    Ok(tokens)
}

struct Lexer<'a> {
    chars: Vec<char>,
    pos: usize,
    _source: &'a str,
}

impl<'a> Lexer<'a> {
    fn new(source: &'a str) -> Self {
        Self {
            chars: source.chars().collect(),
            pos: 0,
            _source: source,
        }
    }

    fn next_token(&mut self) -> Result<Token, LexError> {
        self.skip_whitespace_and_comments();

        let start = self.pos;
        let Some(ch) = self.peek() else {
            return Ok(Token {
                kind: TokenKind::Eof,
                span: Span { start, end: start },
            });
        };

        let kind = match ch {
            'a'..='z' | 'A'..='Z' | '_' => self.lex_identifier_or_keyword(),
            '0'..='9' => self.lex_number()?,
            '"' => self.lex_string()?,
            '(' => {
                self.advance();
                TokenKind::LParen
            }
            ')' => {
                self.advance();
                TokenKind::RParen
            }
            '{' => {
                self.advance();
                TokenKind::LBrace
            }
            '}' => {
                self.advance();
                TokenKind::RBrace
            }
            ',' => {
                self.advance();
                TokenKind::Comma
            }
            '.' => {
                self.advance();
                if self.match_char('.') {
                    TokenKind::Concat
                } else {
                    TokenKind::Dot
                }
            }
            '+' => {
                self.advance();
                TokenKind::Plus
            }
            '-' => {
                self.advance();
                if self.match_char('>') {
                    TokenKind::Arrow
                } else {
                    TokenKind::Minus
                }
            }
            '*' => {
                self.advance();
                TokenKind::Star
            }
            '/' => {
                self.advance();
                TokenKind::Slash
            }
            '%' => {
                self.advance();
                TokenKind::Percent
            }
            '=' => {
                self.advance();
                if self.match_char('=') {
                    TokenKind::Equal
                } else {
                    TokenKind::Assign
                }
            }
            '!' => {
                self.advance();
                if self.match_char('=') {
                    TokenKind::NotEqual
                } else {
                    return Err(self.error("unexpected '!'"));
                }
            }
            '<' => {
                self.advance();
                if self.match_char('=') {
                    TokenKind::LessEqual
                } else {
                    TokenKind::Less
                }
            }
            '>' => {
                self.advance();
                if self.match_char('=') {
                    TokenKind::GreaterEqual
                } else {
                    TokenKind::Greater
                }
            }
            _ => {
                return Err(self.error(&format!("unexpected character '{}'", ch)));
            }
        };

        Ok(Token {
            kind,
            span: Span {
                start,
                end: self.pos,
            },
        })
    }

    fn lex_identifier_or_keyword(&mut self) -> TokenKind {
        let start = self.pos;
        self.advance();
        while let Some(ch) = self.peek() {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                self.advance();
            } else {
                break;
            }
        }
        let text: String = self.chars[start..self.pos].iter().collect();
        TokenKind::keyword_or_ident(&text)
    }

    fn lex_number(&mut self) -> Result<TokenKind, LexError> {
        let start = self.pos;
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            self.advance();
        }

        if self.peek() == Some('.') && self.peek_n(1).is_some_and(|c| c.is_ascii_digit()) {
            self.advance();
            while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                self.advance();
            }
            let text: String = self.chars[start..self.pos].iter().collect();
            let value = text
                .parse::<f64>()
                .map_err(|_| self.error("invalid float literal"))?;
            return Ok(TokenKind::Float(value));
        }

        let text: String = self.chars[start..self.pos].iter().collect();
        let value = text
            .parse::<i64>()
            .map_err(|_| self.error("invalid integer literal"))?;
        Ok(TokenKind::Integer(value))
    }

    fn lex_string(&mut self) -> Result<TokenKind, LexError> {
        self.advance(); // opening quote
        let mut result = String::new();

        while let Some(ch) = self.peek() {
            match ch {
                '"' => {
                    self.advance();
                    return Ok(TokenKind::String(result));
                }
                '\\' => {
                    self.advance();
                    let Some(escaped) = self.peek() else {
                        return Err(self.error("unterminated string escape"));
                    };
                    let resolved = match escaped {
                        'n' => '\n',
                        't' => '\t',
                        'r' => '\r',
                        '"' => '"',
                        '\\' => '\\',
                        _ => return Err(self.error("invalid string escape")),
                    };
                    result.push(resolved);
                    self.advance();
                }
                _ => {
                    result.push(ch);
                    self.advance();
                }
            }
        }

        Err(self.error("unterminated string"))
    }

    fn skip_whitespace_and_comments(&mut self) {
        loop {
            while self.peek().is_some_and(|c| c.is_whitespace()) {
                self.advance();
            }

            if self.peek() == Some('-') && self.peek_n(1) == Some('-') {
                self.advance();
                self.advance();
                while let Some(ch) = self.peek() {
                    self.advance();
                    if ch == '\n' {
                        break;
                    }
                }
                continue;
            }

            break;
        }
    }

    fn error(&self, message: &str) -> LexError {
        LexError {
            message: message.to_string(),
            position: self.pos,
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn peek_n(&self, n: usize) -> Option<char> {
        self.chars.get(self.pos + n).copied()
    }

    fn advance(&mut self) {
        self.pos += 1;
    }

    fn match_char(&mut self, expected: char) -> bool {
        if self.peek() == Some(expected) {
            self.advance();
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(input: &str) -> Vec<TokenKind> {
        lex(input)
            .unwrap()
            .into_iter()
            .map(|t| t.kind)
            .collect::<Vec<_>>()
    }

    #[test]
    fn lexes_keywords_and_identifiers() {
        let ks = kinds("local x = fn(a) unsafe receive case true -> a after 1 -> nil end end");
        assert_eq!(
            ks,
            vec![
                TokenKind::Local,
                TokenKind::Identifier("x".into()),
                TokenKind::Assign,
                TokenKind::Fn,
                TokenKind::LParen,
                TokenKind::Identifier("a".into()),
                TokenKind::RParen,
                TokenKind::Unsafe,
                TokenKind::Receive,
                TokenKind::Case,
                TokenKind::True,
                TokenKind::Arrow,
                TokenKind::Identifier("a".into()),
                TokenKind::After,
                TokenKind::Integer(1),
                TokenKind::Arrow,
                TokenKind::Nil,
                TokenKind::End,
                TokenKind::End,
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn lexes_numbers_and_operators() {
        let ks = kinds("1 + 2.5 * 3 % 2 == 0 != 1 <= 2 >= 3 < 4 > 5 -> ..");
        assert_eq!(
            ks,
            vec![
                TokenKind::Integer(1),
                TokenKind::Plus,
                TokenKind::Float(2.5),
                TokenKind::Star,
                TokenKind::Integer(3),
                TokenKind::Percent,
                TokenKind::Integer(2),
                TokenKind::Equal,
                TokenKind::Integer(0),
                TokenKind::NotEqual,
                TokenKind::Integer(1),
                TokenKind::LessEqual,
                TokenKind::Integer(2),
                TokenKind::GreaterEqual,
                TokenKind::Integer(3),
                TokenKind::Less,
                TokenKind::Integer(4),
                TokenKind::Greater,
                TokenKind::Integer(5),
                TokenKind::Arrow,
                TokenKind::Concat,
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn lexes_strings_and_escapes() {
        let tokens = lex("\"hi\\nthere\"").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::String("hi\nthere".into()));
    }

    #[test]
    fn skips_comments_and_whitespace() {
        let ks = kinds("-- c\nlocal x = 1 -- d\n");
        assert_eq!(
            ks,
            vec![
                TokenKind::Local,
                TokenKind::Identifier("x".into()),
                TokenKind::Assign,
                TokenKind::Integer(1),
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn errors_on_unterminated_string() {
        let err = lex("\"abc").unwrap_err();
        assert!(err.message.contains("unterminated string"));
    }
}
