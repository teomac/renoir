pub mod ast;
pub use ast::*;
use crate::dsl::parsers::sql::SqlAST;
use crate::stream::Stream;
use crate::operator::{ExchangeData, Operator};



pub trait Query<Op: Operator> {
    fn query_to_string_aqua(self, query_str: &str) -> String;
}

impl<Op> Query<Op> for Stream<Op> 
where   
    Op: Operator + 'static,
    Op::Out: ExchangeData + PartialOrd + Into<i64> + Ord + 'static,
{
    fn query_to_string_aqua(self, query_str: &str) -> String {
        println!("Input SQL query: {}", query_str);
        
        let sql_ast = SqlAST::parse(query_str).expect("Failed to parse query");
        println!("SQL AST: {:?}", sql_ast);
        
        let aqua_string = sql_ast.to_aqua_string();
        println!("Generated Aqua string:\n{}", aqua_string);
        
        let ast = AquaAST::parse(&aqua_string).expect("Failed to parse aqua string");
        println!("Aqua AST: {:?}", ast);
        
        let mut final_string = String::new();
        
        
        // Add filter if present
        if let Some(condition) = ast.filter {
            let operator_str = match condition.operator {
                ComparisonOp::GreaterThan => ">",
                ComparisonOp::LessThan => "<",
                ComparisonOp::Equals => "==",
                ComparisonOp::GreaterThanEquals => ">=",
                ComparisonOp::LessThanEquals => "<=",
            };
            
            final_string.push_str(&format!(
                ".filter(|{}| {} {} {})",
                condition.variable,
                condition.variable,
                operator_str,
                condition.value
            ));
        }
        
        // Add aggregation or column selection
        match ast.select {
            SelectClause::Column(col) => {
                if col != "*" {
                    final_string.push_str(&format!(".map(|x| {})", col));
                }
            },
            SelectClause::Aggregate(agg) => {
                let agg_str = match agg.function {
                    AggregateType::Max => "max",
                    AggregateType::Min => "min",
                    AggregateType::Avg => "avg",
                };
                
                if agg.column == "*" {
                    final_string.push_str(&format!(".{}()", agg_str));
                } else {
                    final_string.push_str(&format!(".{}()", agg_str));
                }
            }
            SelectClause::ComplexOp(col,char  ,col2 )=> {
                final_string.push_str(&format!(".map(|{}| {} {} {})", col, col,char,col2));
            }
            SelectClause::ComplexValue(col, char ,val  )=> {
                final_string.push_str(&format!(".map(|{}| {} {} {})", col, col,char,val));
            }
        }
        
        println!("Final string: {}", final_string);
        final_string
    }
}