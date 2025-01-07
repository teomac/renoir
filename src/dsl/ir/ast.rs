/*use crate::prelude::SqlAST;
use crate::dsl::parsers::sql::ast::*;

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
    ComplexOP(String, char, Literal),
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
    pub function: AggregateFun,
    pub expression: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFun {
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
                        SelectType::Aggregate(func, var) => {
                            Expression::AggregateOp(Box::new(AggregateOp {
                                function: match func {
                                    AggregateFunction::Max => AggregateFun::Max,
                                },
                                expression: Expression::Column(var.clone()),
                            }))
                        }
                        
                        SelectType::ComplexValue(var1, op, var2) => {
                            Expression::ComplexOP(var1.clone(), *op, Literal::Integer(*var2))
                        }
                        SelectType::Simple(var) => {
                            Expression::Column(var.clone())
                        }
                    },
                    
                }],
                source: Source::Table(query.from.table.clone()),
                filter: query.filter.as_ref().map(|where_clause| Expression::BinaryOp(Box::new(BinaryOp {
                    left: Expression::Column(where_clause.condition.variable.clone()),
                    operator: match where_clause.condition.operator {
                        ComparisonOp::GreaterThan => IrOperator::GreaterThan,
                        ComparisonOp::LessThan => IrOperator::LessThan,
                        ComparisonOp::Equals => IrOperator::Equals,
                    },
                    right: Expression::Literal(Literal::Integer(
                        where_clause.condition.value,
                    )),
                }))),
            }),
        }
    }
}*/