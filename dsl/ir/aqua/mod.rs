pub mod ast;
use std::collections::HashMap;

pub use ast::*;
use crate::dsl::parsers::sql::SqlAST;






pub fn query_to_string_aqua(query_str: &str, hash_map: &HashMap<String, String>) -> String {
    println!("Input SQL query: {}", query_str);
    
    let sql_ast = SqlAST::parse(query_str).expect("Failed to parse query");
    println!("SQL AST: {:?}", sql_ast);
    
    let aqua_string = sql_ast.to_aqua_string();
    println!("Generated Aqua string:\n{}", aqua_string);
    
    let ast = AquaAST::parse(&aqua_string).expect("Failed to parse aqua string");
    println!("Aqua AST: {:?}", ast);
    
    let mut final_string = String::new();

    /*
    if let Some(where_clause) = ast.filter {
        final_string.push_str(&format!(
            ".filter(|x| {})",
            process_where_clause(&where_clause, hash_map)
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
        SelectClause::ComplexValue(col, str ,val  )=> {
            let value = match &val {
                AquaLiteral::Float(val) => format!("{:.2}", val),
                AquaLiteral::Integer(val) => val.to_string(),
                _ => unreachable!(), // TODO: remove unreachable
            };
            if str == "^" {
                final_string.push_str(&format!(".map(|x| x.{}.unwrap().pow({}))", hash_map[&col],value));
            } else {
                final_string.push_str(&format!(".map(|x| x.{}.unwrap() {} {})", hash_map[&col], str, value));
            }
        }
        SelectClause::ComplexOp(col,str  ,col2 )=> {
            final_string.push_str(&format!(".map(|x| x.{}.unwrap() {} x.{}.unwrap())", hash_map[&col], str, hash_map[&col2]));
        }
        SelectClause::Column(col) => {
            if col != "*" {
                final_string.push_str(&format!(".map(|x| x.{}.unwrap())", hash_map[&col]));
            }
        }
    }
    
    println!("Final string: {}", final_string);*/
    final_string
}
/* 
// Helper function to recursively process where conditions
fn process_where_clause(clause: &WhereClause, hash_map: &HashMap<String, String>) -> String {
    let mut current = clause;
    let mut conditions = Vec::new();
    
    // Process first condition
    conditions.push(process_condition(&current.condition, hash_map));
    
    // Process remaining conditions
    while let (Some(op), Some(next)) = (&current.binary_op, &current.next) {
        let op_str = match op {
            BinaryOp::And => "&&",
            BinaryOp::Or => "||",
        };
        conditions.push(op_str.to_string());
        conditions.push(process_condition(&next.condition, hash_map));
        current = next;
    }
    
    conditions.join(" ")
}

// Helper function to process a single condition
fn process_condition(condition: &Condition, hash_map: &HashMap<String, String>) -> String {
    let operator_str = match condition.operator {
        ComparisonOp::GreaterThan => ">",
        ComparisonOp::LessThan => "<",
        ComparisonOp::Equal => "==",
        ComparisonOp::GreaterThanEquals => ">=",
        ComparisonOp::LessThanEquals => "<=",
        ComparisonOp::NotEqual => "!=",
    };

    let value = match &condition.value {
        AquaLiteral::Float(val) => format!("{:.2}", val),
        AquaLiteral::Integer(val) => val.to_string(),
        AquaLiteral::String(val) => val.to_string(),
        AquaLiteral::Boolean(val) => val.to_string(),
    };

    format!(
        "x.{}.unwrap() {} {}",
        hash_map[&condition.variable],
        operator_str,
        value
    )
}*/