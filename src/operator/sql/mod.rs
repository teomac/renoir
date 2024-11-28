//! SQL translation functionality for Renoir streams.

use crate::operator::{Data, Operator}; 
use crate::stream::Stream;

mod ast;
use ast::{Query, Operator as SqlOperator};

/// Extension trait for SQL-like filtering capabilities
pub trait SqlFilterExt<Op: Operator> {
    fn from_sql(self, query: &str) -> Stream<impl Operator<Out = Op::Out>>
    where
        Op::Out: Data + PartialOrd + Into<i64>;
}

impl<Op> SqlFilterExt<Op> for Stream<Op>
where
    Op: Operator + 'static,
    Op::Out: Data + PartialOrd + Into<i64>,
{
    fn from_sql(self, query_str: &str) -> Stream<impl Operator<Out = Op::Out>> {
        let query = Query::parse(query_str).expect("Failed to parse SQL query");
        let condition_value = query.where_clause.condition.value;
        
        self.filter(move |x: &Op::Out| {
            let x_val: i64 = (*x).clone().into();
            match query.where_clause.condition.operator {
                SqlOperator::GreaterThan => x_val > condition_value,
                SqlOperator::LessThan => x_val < condition_value,
                SqlOperator::Equals => x_val == condition_value,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StreamContext;

    #[test]
    fn test_sql_filter() {
        let ctx = StreamContext::new_local();
        
        let sql_result = ctx
            .stream_iter(0..10)
            .from_sql("SELECT a FROM input WHERE a > 5")
            .collect_vec();

        ctx.execute_blocking();
        
        assert_eq!(sql_result.get().unwrap(), vec![6, 7, 8, 9]);
    }
}