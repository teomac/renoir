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
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Column(String),
    Literal(Literal),
    BinaryOp(Box<BinaryOp>),
    AggregateOp(Box<AggregateOp>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Integer(i64),
}

#[derive(Debug, Clone, PartialEq)]
pub struct BinaryOp {
    pub left: Expression,
    pub operator: IrOperator,
    pub right: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateOp {
    pub function: AggregateFunction,
    pub expression: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Max,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IrOperator {
    GreaterThan,
    LessThan,
    Equals,
}

impl IrAST {
    pub fn parse(query: &SqlAST) -> Self {
        IrAST {
            operation: Operation::Select(SelectOperation {
                projections: vec![Projection {
                    expression: match &query.select.selection {
                        crate::dsl::parsers::sql::ast::SelectType::Simple(var) => {
                            Expression::Column(var.clone())
                        }
                        crate::dsl::parsers::sql::ast::SelectType::Aggregate(func, var) => {
                            Expression::AggregateOp(Box::new(AggregateOp {
                                function: match func {
                                    crate::dsl::parsers::sql::ast::AggregateFunction::Max => AggregateFunction::Max,
                                },
                                expression: Expression::Column(var.clone()),
                            }))
                        }
                    },
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