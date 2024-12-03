use core::panic;
use crate::dsl::ir::IrAST;
use crate::dsl::parsers::sql::SqlAST;
use crate::operator::{ExchangeData, Operator};
use crate::stream::Stream;

use super::ir::{Expression, IrOperator, Literal, Operation, AggregateFunction};

/// Extension trait for applying queries to streams
pub trait QueryExt<Op: Operator> {
    fn query(self, query: &str) -> Stream<impl Operator<Out = Op::Out>>
    where
    Op::Out: ExchangeData + PartialOrd + Into<i64> + Ord + 'static;
}

impl<Op> QueryExt<Op> for Stream<Op> 
where
    Op: Operator + 'static,
    Op::Out: ExchangeData + PartialOrd + Into<i64> + Ord + 'static,
{
    fn query(self, query_str: &str) -> Stream<impl Operator<Out = Op::Out>> {
        let sql_ast = SqlAST::parse(query_str).expect("Failed to parse query");
        let ir = IrAST::parse(&sql_ast);

        match ir.operation {
            Operation::Select(select) => {
                // First apply any filters
                let filtered_stream = match select.filter {
                    Some(Expression::BinaryOp(op)) => {
                        let value = op.right.as_integer();
                        let filter = move |x: &Op::Out| match op.operator {
                            IrOperator::GreaterThan => x.clone().into() > value,
                            IrOperator::LessThan => x.clone().into() < value,
                            IrOperator::Equals => x.clone().into() == value,
                        };
                        self.filter(filter)
                    },
                    _ => panic!("Expected filter expression in query"),
                };

                // Then apply any aggregations
                match &select.projections[0].expression {
                    Expression::AggregateOp(agg_op) => {
                        match agg_op.function {
                            AggregateFunction::Max => {
                            
                                filtered_stream
                                .fold(
                                    None,
                                    |acc: &mut Option<Op::Out>, x| {
                                        match acc {
                                            None => *acc = Some(x),
                                            Some(curr) => {
                                                if &x > curr {
                                                    *acc = Some(x);
                                                }
                                            }
                                        }
                                    }
                                )
                                .map(|opt| opt.expect("Expected at least one element"))
                            }
                        }
                    },
                    _ => panic!("Expected aggregate expression in query"),
                }
            }
        }
    }
}

trait AsInteger {
    fn as_integer(&self) -> i64;
}

impl AsInteger for Expression {
    fn as_integer(&self) -> i64 {
        match self {
            Expression::Literal(Literal::Integer(n)) => *n,
            _ => panic!("Expected integer literal"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StreamContext;

    #[test]
    fn test_query_filter() {
        let ctx = StreamContext::new_local();
        let input = 0..10;
        let result = ctx
            .stream_iter(input)
            .query("SELECT a FROM input WHERE a > 5")
            .collect_vec();
            
        ctx.execute_blocking();

        let result = result.get().unwrap();
        assert_eq!(result, vec![6, 7, 8, 9]);
    }

    #[test]
    fn test_query_max() {
        let ctx = StreamContext::new_local();
        let input = 0..10;
        let result = ctx
            .stream_iter(input)
            .query("SELECT MAX(a) FROM input WHERE a > 5")
            .collect_vec();
            
        ctx.execute_blocking();

        let result = result.get().unwrap();
        assert_eq!(result, vec![9]);
    }
}