use std::fmt::Display;

use crate::prelude::SqlAST;

#[derive(Debug, Clone, PartialEq)]
pub struct IrAST {
    pub operation: Operation,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operation {
    Select(SelectOperation),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectOperation {
    pub projections: Vec<Projection>,
    pub source: Source,
    pub filter: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Projection {
    pub expression: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Source {
    Table(String),
    // Future: Subquery, Join, etc.
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Column(String),
    Literal(Literal),
    BinaryOp(Box<BinaryOp>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Integer(i64),
    // Future: Float, String, etc.
}

#[derive(Debug, Clone, PartialEq)]
pub struct BinaryOp {
    pub left: Expression,
    pub operator: IrOperator,
    pub right: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IrOperator {
    GreaterThan,
    LessThan,
    Equals,
    // Future: Add, Subtract, etc.
}

// Conversion from SQL AST to IR
impl IrAST {
    pub fn parse(query: &SqlAST) -> Self {
        IrAST {
            operation: Operation::Select(SelectOperation {
                projections: vec![Projection {
                    expression: Expression::Column(query.select.variable.clone()),
                }],
                source: Source::Table(query.from.table.clone()),
                filter: Some(Expression::BinaryOp(Box::new(BinaryOp {
                    left: Expression::Column(query.filter.condition.variable.clone()),
                    operator: match query.filter.condition.operator {
                        crate::dsl::parsers::sql::ast::ComparisonOp::GreaterThan => IrOperator::GreaterThan,
                        crate::dsl::parsers::sql::ast::ComparisonOp::LessThan => IrOperator::LessThan,
                        crate::dsl::parsers::sql::ast::ComparisonOp::Equals => IrOperator::Equals,
                    },
                    right: Expression::Literal(Literal::Integer(
                        query.filter.condition.value,
                    )),
                }))),
            }),
        }
    }
}

use std::fmt;

impl Display for IrAST {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.operation)
    }
}

impl Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Operation::Select(select) => write!(f, "{}", select),
        }
    }
}

impl Display for SelectOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SELECT ")?;
        for (i, proj) in self.projections.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", proj)?;
        }
        write!(f, " FROM {}", self.source)?;
        if let Some(filter) = &self.filter {
            write!(f, " WHERE {}", filter)?;
        }
        Ok(())
    }
}

impl Display for Projection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expression)
    }
}

impl Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Source::Table(name) => write!(f, "{}", name),
        }
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::Column(name) => write!(f, "{}", name),
            Expression::Literal(lit) => write!(f, "{}", lit),
            Expression::BinaryOp(op) => write!(f, "{}", op),
        }
    }
}

impl Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Integer(n) => write!(f, "{}", n),
        }
    }
}

impl Display for BinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.operator, self.right)
    }
}

impl Display for IrOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IrOperator::GreaterThan => write!(f, ">"),
            IrOperator::LessThan => write!(f, "<"),
            IrOperator::Equals => write!(f, "="),
        }
    }
}