#[derive(Debug, Clone, PartialEq)]
pub struct Program {
    pub items: Vec<Item>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Item {
    Local(LocalBinding),
    Expr(Expr),
}

#[derive(Debug, Clone, PartialEq)]
pub struct LocalBinding {
    pub name: String,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Block(Vec<Item>),
    Integer(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Nil,
    Identifier(String),
    Fn {
        params: Vec<String>,
        body: Box<Expr>,
    },
    If {
        condition: Box<Expr>,
        then_branch: Box<Expr>,
        else_branch: Box<Expr>,
    },
    Receive {
        cases: Vec<ReceiveCase>,
        after: Option<Box<ReceiveAfter>>,
    },
    Unsafe(Box<Expr>),
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    Call {
        callee: Box<Expr>,
        args: Vec<Expr>,
    },
    FieldAccess {
        expr: Box<Expr>,
        field: String,
    },
    List(Vec<Expr>),
    Record(Vec<(String, Expr)>),
    RecordUpdate {
        base: Box<Expr>,
        updates: Vec<(String, Expr)>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReceiveCase {
    pub pattern: Pattern,
    pub body: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReceiveAfter {
    pub timeout: Expr,
    pub body: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Pattern {
    Wildcard,
    Binding(String),
    Integer(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Nil,
    Record(Vec<(String, Pattern)>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Concat,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructs_program_and_expressions() {
        let program = Program {
            items: vec![Item::Local(LocalBinding {
                name: "x".into(),
                value: Expr::Binary {
                    left: Box::new(Expr::Integer(1)),
                    op: BinaryOp::Add,
                    right: Box::new(Expr::Integer(2)),
                },
            })],
        };

        assert_eq!(program.items.len(), 1);
    }

    #[test]
    fn equality_for_record_update() {
        let expr = Expr::RecordUpdate {
            base: Box::new(Expr::Identifier("pessoa".into())),
            updates: vec![("idade".into(), Expr::Integer(21))],
        };

        assert_eq!(
            expr,
            Expr::RecordUpdate {
                base: Box::new(Expr::Identifier("pessoa".into())),
                updates: vec![("idade".into(), Expr::Integer(21))],
            }
        );
    }

    #[test]
    fn equality_for_receive_pattern() {
        let expr = Expr::Receive {
            cases: vec![ReceiveCase {
                pattern: Pattern::Record(vec![("type".into(), Pattern::String("ping".into()))]),
                body: Expr::String("pong".into()),
            }],
            after: Some(Box::new(ReceiveAfter {
                timeout: Expr::Integer(1000),
                body: Expr::String("timeout".into()),
            })),
        };

        assert!(matches!(expr, Expr::Receive { .. }));
    }
}
