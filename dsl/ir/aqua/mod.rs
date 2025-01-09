pub mod ast;
use std::collections::HashMap;

pub use ast::*;
use crate::dsl::parsers::sql::SqlAST;






    pub fn query_to_string_aqua(query_str: &str, hash_map: HashMap<String, String>) -> String {
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

            let value = match condition.value {
                
                AquaLiteral::Float(val) => format!("{:.2}", val),
                AquaLiteral::Integer(val) => val.to_string(),
            };
            
            final_string.push_str(&format!(
                ".filter(|x| x.{}.unwrap() {} {})",
                hash_map[&condition.variable],
                operator_str,
                value
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
                let value = match &val {
                    AquaLiteral::Float(val) => format!("{:.2}", val),
                    AquaLiteral::Integer(val) => val.to_string(),
                };
                if char == '^' {
                    final_string.push_str(&format!(".map(|x| x.{}.unwrap().pow({}))", hash_map[&col],value));
                } else {
                    final_string.push_str(&format!(".map(|x| x.{}.unwrap() {} {})", hash_map[&col], char, value));
                }
            }
            SelectClause::ComplexOp(col,char  ,col2 )=> {
                final_string.push_str(&format!(".map(|x| x.{}.unwrap() {} x.{}.unwrap())", hash_map[&col], char, hash_map[&col2]));
            }
            SelectClause::Column(col) => {
                if col != "*" {
                    final_string.push_str(&format!(".map(|x| x.{}.unwrap())", hash_map[&col]));
                }
            }
        }
        
        println!("Final string: {}", final_string);
        final_string
    }