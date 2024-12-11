pub mod ast;
pub use ast::*;
use crate::dsl::parsers::sql::SqlAST;






    pub fn query_to_string_aqua(query_str: &str) -> String {
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
                ".filter(|{}| {} {} &{})",
                condition.variable,
                condition.variable,
                operator_str,
                condition.value
            ));
        }
        
        // Add aggregation or column selection
        match ast.select {
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
            SelectClause::ComplexValue(col, char ,val  )=> {
                if char == '^' {
                    // convert val from i64 to u32
                    let val = val as u32;
                    final_string.push_str(&format!(".map(|{}| {}.pow({}))", col, col,val));
                } else {
                    final_string.push_str(&format!(".map(|{}| {} {} {})", col, col,char,val));
                }
            }
            SelectClause::ComplexOp(col,char  ,col2 )=> {
                final_string.push_str(&format!(".map(|{}| {} {} {})", col, col,char,col2));
            }
            SelectClause::Column(col) => {
                if col != "*" {
                    final_string.push_str(&format!(".map(|x| {})", col));
                }
            }
        }
        
        println!("Final string: {}", final_string);
        final_string
    }