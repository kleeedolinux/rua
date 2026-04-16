use std::fmt;

use crate::ast::{
    BinaryOp, Expr, Item, LocalBinding, Pattern, Program, ReceiveAfter, ReceiveCase, UnaryOp,
};
use crate::lexer::{lex, LexError};
use crate::token::{Token, TokenKind};

#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    Lex(LexError),
    UnexpectedToken { expected: String, found: TokenKind },
    Message(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lex(err) => write!(f, "{err}"),
            Self::UnexpectedToken { expected, found } => {
                write!(f, "expected {expected}, found {found:?}")
            }
            Self::Message(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for ParseError {}

impl From<LexError> for ParseError {
    fn from(value: LexError) -> Self {
        Self::Lex(value)
    }
}

pub fn parse_program(input: &str) -> Result<Program, ParseError> {
    let tokens = lex(input)?;
    Parser::new(tokens).parse_program()
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn parse_program(&mut self) -> Result<Program, ParseError> {
        let mut items = Vec::new();

        while !self.at_end() {
            if self.match_kind(&TokenKind::Local) {
                items.push(Item::Local(self.parse_local_binding()?));
            } else {
                items.push(Item::Expr(self.parse_expression()?));
            }
        }

        Ok(Program { items })
    }

    fn parse_local_binding(&mut self) -> Result<LocalBinding, ParseError> {
        let name = self.expect_identifier()?;
        self.expect(&TokenKind::Assign)?;
        let value = self.parse_expression()?;
        Ok(LocalBinding { name, value })
    }

    fn parse_expression(&mut self) -> Result<Expr, ParseError> {
        self.parse_or()
    }

    fn parse_block_expression_until(&mut self, terminators: &[TokenKind]) -> Result<Expr, ParseError> {
        let mut items = Vec::new();
        while !self.at_end() && !self.check_any(terminators) {
            if self.match_kind(&TokenKind::Local) {
                items.push(Item::Local(self.parse_local_binding()?));
            } else {
                items.push(Item::Expr(self.parse_expression()?));
            }
        }

        if items.is_empty() {
            return Err(ParseError::Message("expected expression in block".into()));
        }

        Ok(Expr::Block(items))
    }

    fn parse_or(&mut self) -> Result<Expr, ParseError> {
        let mut expr = self.parse_and()?;
        while self.match_kind(&TokenKind::Or) {
            let right = self.parse_and()?;
            expr = Expr::Binary {
                left: Box::new(expr),
                op: BinaryOp::Or,
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_and(&mut self) -> Result<Expr, ParseError> {
        let mut expr = self.parse_equality()?;
        while self.match_kind(&TokenKind::And) {
            let right = self.parse_equality()?;
            expr = Expr::Binary {
                left: Box::new(expr),
                op: BinaryOp::And,
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_equality(&mut self) -> Result<Expr, ParseError> {
        let mut expr = self.parse_comparison()?;

        loop {
            let op = if self.match_kind(&TokenKind::Equal) {
                Some(BinaryOp::Eq)
            } else if self.match_kind(&TokenKind::NotEqual) {
                Some(BinaryOp::Ne)
            } else {
                None
            };

            if let Some(op) = op {
                let right = self.parse_comparison()?;
                expr = Expr::Binary {
                    left: Box::new(expr),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(expr)
    }

    fn parse_comparison(&mut self) -> Result<Expr, ParseError> {
        let mut expr = self.parse_concat()?;

        loop {
            let op = if self.match_kind(&TokenKind::Less) {
                Some(BinaryOp::Lt)
            } else if self.match_kind(&TokenKind::LessEqual) {
                Some(BinaryOp::Le)
            } else if self.match_kind(&TokenKind::Greater) {
                Some(BinaryOp::Gt)
            } else if self.match_kind(&TokenKind::GreaterEqual) {
                Some(BinaryOp::Ge)
            } else {
                None
            };

            if let Some(op) = op {
                let right = self.parse_concat()?;
                expr = Expr::Binary {
                    left: Box::new(expr),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(expr)
    }

    fn parse_concat(&mut self) -> Result<Expr, ParseError> {
        let left = self.parse_term()?;
        if self.match_kind(&TokenKind::Concat) {
            let right = self.parse_concat()?;
            Ok(Expr::Binary {
                left: Box::new(left),
                op: BinaryOp::Concat,
                right: Box::new(right),
            })
        } else {
            Ok(left)
        }
    }

    fn parse_term(&mut self) -> Result<Expr, ParseError> {
        let mut expr = self.parse_factor()?;

        loop {
            let op = if self.match_kind(&TokenKind::Plus) {
                Some(BinaryOp::Add)
            } else if self.match_kind(&TokenKind::Minus) {
                Some(BinaryOp::Sub)
            } else {
                None
            };

            if let Some(op) = op {
                let right = self.parse_factor()?;
                expr = Expr::Binary {
                    left: Box::new(expr),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(expr)
    }

    fn parse_factor(&mut self) -> Result<Expr, ParseError> {
        let mut expr = self.parse_unary()?;

        loop {
            let op = if self.match_kind(&TokenKind::Star) {
                Some(BinaryOp::Mul)
            } else if self.match_kind(&TokenKind::Slash) {
                Some(BinaryOp::Div)
            } else if self.match_kind(&TokenKind::Percent) {
                Some(BinaryOp::Mod)
            } else {
                None
            };

            if let Some(op) = op {
                let right = self.parse_unary()?;
                expr = Expr::Binary {
                    left: Box::new(expr),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(expr)
    }

    fn parse_unary(&mut self) -> Result<Expr, ParseError> {
        if self.match_kind(&TokenKind::Minus) {
            return Ok(Expr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(self.parse_unary()?),
            });
        }

        if self.match_kind(&TokenKind::Not) {
            return Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(self.parse_unary()?),
            });
        }

        if self.match_kind(&TokenKind::Unsafe) {
            return Ok(Expr::Unsafe(Box::new(self.parse_unary()?)));
        }

        self.parse_postfix()
    }

    fn parse_postfix(&mut self) -> Result<Expr, ParseError> {
        let mut expr = self.parse_primary()?;

        loop {
            if self.match_kind(&TokenKind::LParen) {
                let mut args = Vec::new();
                if !self.check(&TokenKind::RParen) {
                    loop {
                        args.push(self.parse_expression()?);
                        if !self.match_kind(&TokenKind::Comma) {
                            break;
                        }
                    }
                }
                self.expect(&TokenKind::RParen)?;
                expr = Expr::Call {
                    callee: Box::new(expr),
                    args,
                };
            } else if self.match_kind(&TokenKind::Dot) {
                let field = self.expect_identifier()?;
                expr = Expr::FieldAccess {
                    expr: Box::new(expr),
                    field,
                };
            } else {
                break;
            }
        }

        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        if self.match_kind(&TokenKind::If) {
            let condition = self.parse_expression()?;
            self.expect(&TokenKind::Then)?;
            let then_branch = self.parse_block_expression_until(&[TokenKind::Else])?;
            self.expect(&TokenKind::Else)?;
            let else_branch = self.parse_block_expression_until(&[TokenKind::End])?;
            self.expect(&TokenKind::End)?;
            return Ok(Expr::If {
                condition: Box::new(condition),
                then_branch: Box::new(then_branch),
                else_branch: Box::new(else_branch),
            });
        }

        if self.match_kind(&TokenKind::Receive) {
            return self.parse_receive_expression();
        }

        if self.match_kind(&TokenKind::Fn) {
            self.expect(&TokenKind::LParen)?;
            let mut params = Vec::new();
            if !self.check(&TokenKind::RParen) {
                loop {
                    params.push(self.expect_identifier()?);
                    if !self.match_kind(&TokenKind::Comma) {
                        break;
                    }
                }
            }
            self.expect(&TokenKind::RParen)?;
            let body = self.parse_block_expression_until(&[TokenKind::End])?;
            self.expect(&TokenKind::End)?;
            return Ok(Expr::Fn {
                params,
                body: Box::new(body),
            });
        }

        if self.match_kind(&TokenKind::LBrace) {
            return self.parse_brace_literal();
        }

        if self.match_kind(&TokenKind::LParen) {
            let expr = self.parse_expression()?;
            self.expect(&TokenKind::RParen)?;
            return Ok(expr);
        }

        if self.match_kind(&TokenKind::True) {
            return Ok(Expr::Bool(true));
        }
        if self.match_kind(&TokenKind::False) {
            return Ok(Expr::Bool(false));
        }
        if self.match_kind(&TokenKind::Nil) {
            return Ok(Expr::Nil);
        }

        match self.advance().cloned() {
            Some(Token {
                kind: TokenKind::Integer(value),
                ..
            }) => Ok(Expr::Integer(value)),
            Some(Token {
                kind: TokenKind::Float(value),
                ..
            }) => Ok(Expr::Float(value)),
            Some(Token {
                kind: TokenKind::String(value),
                ..
            }) => Ok(Expr::String(value)),
            Some(Token {
                kind: TokenKind::Identifier(value),
                ..
            }) => Ok(Expr::Identifier(value)),
            Some(Token { kind, .. }) => Err(ParseError::UnexpectedToken {
                expected: "expression".into(),
                found: kind,
            }),
            None => Err(ParseError::Message("unexpected end of input".into())),
        }
    }

    fn parse_brace_literal(&mut self) -> Result<Expr, ParseError> {
        if self.match_kind(&TokenKind::RBrace) {
            return Ok(Expr::List(Vec::new()));
        }

        let first_expr = self.parse_expression()?;

        if self.match_kind(&TokenKind::With) {
            let updates = self.parse_record_fields(false)?;
            self.expect(&TokenKind::RBrace)?;
            return Ok(Expr::RecordUpdate {
                base: Box::new(first_expr),
                updates,
            });
        }

        if self.match_kind(&TokenKind::Assign) {
            let first_name = match first_expr {
                Expr::Identifier(name) => name,
                _ => {
                    return Err(ParseError::Message(
                        "record field name must be an identifier".into(),
                    ));
                }
            };
            let first_value = self.parse_expression()?;
            let mut fields = vec![(first_name, first_value)];
            if self.match_kind(&TokenKind::Comma) {
                fields.extend(self.parse_record_fields(true)?);
            }
            self.expect(&TokenKind::RBrace)?;
            return Ok(Expr::Record(fields));
        }

        let mut items = vec![first_expr];
        while self.match_kind(&TokenKind::Comma) {
            if self.check(&TokenKind::RBrace) {
                break;
            }
            items.push(self.parse_expression()?);
        }
        self.expect(&TokenKind::RBrace)?;
        Ok(Expr::List(items))
    }

    fn parse_receive_expression(&mut self) -> Result<Expr, ParseError> {
        let mut cases = Vec::new();
        while self.match_kind(&TokenKind::Case) {
            let pattern = self.parse_pattern()?;
            self.expect(&TokenKind::Arrow)?;
            let body = self.parse_block_expression_until(&[
                TokenKind::Case,
                TokenKind::After,
                TokenKind::End,
            ])?;
            cases.push(ReceiveCase { pattern, body });
        }

        if cases.is_empty() {
            return Err(ParseError::Message(
                "receive must contain at least one case".into(),
            ));
        }

        let after = if self.match_kind(&TokenKind::After) {
            let timeout = self.parse_expression()?;
            self.expect(&TokenKind::Arrow)?;
            let body = self.parse_block_expression_until(&[TokenKind::End])?;
            Some(Box::new(ReceiveAfter { timeout, body }))
        } else {
            None
        };

        self.expect(&TokenKind::End)?;
        Ok(Expr::Receive { cases, after })
    }

    fn parse_pattern(&mut self) -> Result<Pattern, ParseError> {
        if self.match_kind(&TokenKind::LBrace) {
            let mut fields = Vec::new();
            if !self.match_kind(&TokenKind::RBrace) {
                loop {
                    let name = self.expect_identifier()?;
                    self.expect(&TokenKind::Assign)?;
                    let pattern = self.parse_pattern()?;
                    fields.push((name, pattern));
                    if !self.match_kind(&TokenKind::Comma) {
                        break;
                    }
                }
                self.expect(&TokenKind::RBrace)?;
            }
            return Ok(Pattern::Record(fields));
        }

        if self.match_kind(&TokenKind::True) {
            return Ok(Pattern::Bool(true));
        }
        if self.match_kind(&TokenKind::False) {
            return Ok(Pattern::Bool(false));
        }
        if self.match_kind(&TokenKind::Nil) {
            return Ok(Pattern::Nil);
        }

        match self.advance().cloned() {
            Some(Token {
                kind: TokenKind::Integer(value),
                ..
            }) => Ok(Pattern::Integer(value)),
            Some(Token {
                kind: TokenKind::Float(value),
                ..
            }) => Ok(Pattern::Float(value)),
            Some(Token {
                kind: TokenKind::String(value),
                ..
            }) => Ok(Pattern::String(value)),
            Some(Token {
                kind: TokenKind::Identifier(value),
                ..
            }) => {
                if value == "_" {
                    Ok(Pattern::Wildcard)
                } else {
                    Ok(Pattern::Binding(value))
                }
            }
            Some(Token { kind, .. }) => Err(ParseError::UnexpectedToken {
                expected: "pattern".into(),
                found: kind,
            }),
            None => Err(ParseError::Message("unexpected end of input".into())),
        }
    }

    fn parse_record_fields(&mut self, allow_empty: bool) -> Result<Vec<(String, Expr)>, ParseError> {
        let mut fields = Vec::new();

        if allow_empty && self.check(&TokenKind::RBrace) {
            return Ok(fields);
        }

        loop {
            let name = self.expect_identifier()?;
            self.expect(&TokenKind::Assign)?;
            let value = self.parse_expression()?;
            fields.push((name, value));

            if !self.match_kind(&TokenKind::Comma) {
                break;
            }
            if self.check(&TokenKind::RBrace) {
                break;
            }
        }

        Ok(fields)
    }

    fn expect_identifier(&mut self) -> Result<String, ParseError> {
        match self.advance().cloned() {
            Some(Token {
                kind: TokenKind::Identifier(name),
                ..
            }) => Ok(name),
            Some(Token { kind, .. }) => Err(ParseError::UnexpectedToken {
                expected: "identifier".into(),
                found: kind,
            }),
            None => Err(ParseError::Message("unexpected end of input".into())),
        }
    }

    fn expect(&mut self, expected: &TokenKind) -> Result<(), ParseError> {
        let token = self.advance().cloned();
        match token {
            Some(Token { kind, .. }) if &kind == expected => Ok(()),
            Some(Token { kind, .. }) => Err(ParseError::UnexpectedToken {
                expected: format!("{expected:?}"),
                found: kind,
            }),
            None => Err(ParseError::Message("unexpected end of input".into())),
        }
    }

    fn check(&self, expected: &TokenKind) -> bool {
        self.current()
            .map(|t| &t.kind == expected)
            .unwrap_or(false)
    }

    fn match_kind(&mut self, expected: &TokenKind) -> bool {
        if self.check(expected) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn check_any(&self, expected: &[TokenKind]) -> bool {
        expected.iter().any(|k| self.check(k))
    }

    fn at_end(&self) -> bool {
        self.current()
            .map(|t| matches!(t.kind, TokenKind::Eof))
            .unwrap_or(true)
    }

    fn current(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<&Token> {
        let token = self.tokens.get(self.pos);
        if token.is_some() {
            self.pos += 1;
        }
        token
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_expr(input: &str) -> Expr {
        parse_program(input)
            .unwrap()
            .items
            .into_iter()
            .next()
            .map(|item| match item {
                Item::Expr(expr) => expr,
                Item::Local(binding) => binding.value,
            })
            .unwrap()
    }

    #[test]
    fn parses_local_and_call() {
        let program = parse_program("local dobro = fn(x) x * 2 end\ndobro(10)").unwrap();
        assert_eq!(program.items.len(), 2);

        match &program.items[0] {
            Item::Local(binding) => assert_eq!(binding.name, "dobro"),
            _ => panic!("expected local binding"),
        }

        match &program.items[1] {
            Item::Expr(Expr::Call { args, .. }) => assert_eq!(args.len(), 1),
            _ => panic!("expected call expression"),
        }
    }

    #[test]
    fn parses_if_expression() {
        let expr = parse_expr("if x > 10 then x * 2 else x + 1 end");
        match expr {
            Expr::If {
                condition,
                then_branch,
                else_branch,
            } => {
                assert!(matches!(*condition, Expr::Binary { .. }));
                assert!(matches!(*then_branch, Expr::Block(_)));
                assert!(matches!(*else_branch, Expr::Block(_)));
            }
            _ => panic!("expected if expression"),
        }
    }

    #[test]
    fn parses_record_and_update() {
        let record = parse_expr("{ nome = \"Lia\", idade = 20 }");
        match record {
            Expr::Record(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].0, "nome");
            }
            _ => panic!("expected record"),
        }

        let update = parse_expr("{ pessoa with idade = 21, nome = \"Ana\" }");
        match update {
            Expr::RecordUpdate { updates, .. } => assert_eq!(updates.len(), 2),
            _ => panic!("expected record update"),
        }
    }

    #[test]
    fn parses_list_and_empty_list() {
        let empty = parse_expr("{}");
        assert_eq!(empty, Expr::List(vec![]));

        let list = parse_expr("{1, 2, 3}");
        match list {
            Expr::List(items) => assert_eq!(items.len(), 3),
            _ => panic!("expected list"),
        }
    }

    #[test]
    fn parses_precedence_and_unary() {
        let expr = parse_expr("not a and b or c + 2 * -d");
        match expr {
            Expr::Binary {
                op: BinaryOp::Or, ..
            } => {}
            _ => panic!("expected or at top level"),
        }
    }

    #[test]
    fn parses_field_access_and_chained_calls() {
        let expr = parse_expr("f(1)(2).x");
        assert!(matches!(expr, Expr::FieldAccess { .. }));
    }

    #[test]
    fn errors_on_invalid_field_access_syntax() {
        let err = parse_program("pessoa.").unwrap_err();
        assert!(matches!(err, ParseError::UnexpectedToken { .. }));
    }

    #[test]
    fn parses_receive_with_patterns_and_after() {
        let expr = parse_expr(
            "receive case { type = \"ping\", from = from } -> send(from, \"pong\") case _ -> nil after 1000 -> \"timeout\" end",
        );
        match expr {
            Expr::Receive { cases, after } => {
                assert_eq!(cases.len(), 2);
                assert!(matches!(cases[1].pattern, Pattern::Wildcard));
                assert!(after.is_some());
            }
            _ => panic!("expected receive expression"),
        }
    }

    #[test]
    fn parses_metatable_style_function_concat() {
        let expr = parse_expr(
            "with_meta({ nome = \"Lia\" }, { show = fn(self) \"Pessoa(\" .. self.nome .. \")\" end })",
        );
        match expr {
            Expr::Call { args, .. } => {
                assert_eq!(args.len(), 2);
            }
            _ => panic!("expected with_meta call"),
        }
    }

    #[test]
    fn parses_multiline_function_body_block() {
        let expr = parse_expr("fn(x) local y = x + 1 y * 2 end");
        match expr {
            Expr::Fn { body, .. } => match *body {
                Expr::Block(items) => assert_eq!(items.len(), 2),
                _ => panic!("expected block body"),
            },
            _ => panic!("expected function expression"),
        }
    }

    #[test]
    fn parses_receive_case_multiline_body() {
        let expr = parse_expr(
            "receive case { type = \"ping\", from = from } -> send(from, { type = \"pong\" }) nil end",
        );
        match expr {
            Expr::Receive { cases, .. } => match &cases[0].body {
                Expr::Block(items) => assert_eq!(items.len(), 2),
                _ => panic!("expected block in case body"),
            },
            _ => panic!("expected receive"),
        }
    }

    #[test]
    fn parses_unsafe_ffi_and_os_spawn() {
        let expr = parse_expr("unsafe ffi(\"host_read_file\", path)");
        assert!(matches!(expr, Expr::Unsafe(_)));

        let expr2 = parse_expr("unsafe os.spawn(fn() nil end)");
        assert!(matches!(expr2, Expr::Unsafe(_)));
    }
}
