use std::collections::HashMap;
use crate::dsl::ir::aqua::{ast_parser::ast_structure::AquaAST, AggregateType, AquaLiteral, BinaryOp, ColumnRef, ComparisonOp, Condition, SelectClause, WhereClause};

pub struct AquaToRenoir;

impl AquaToRenoir {
    pub fn convert(ast: &AquaAST, hash_maps: &Vec<HashMap<String, String>>) -> String {
        let mut final_string = String::new();

        if let Some(where_clause) = &ast.filter {
            final_string.push_str(&format!(
                ".filter(|x| {})",
                Self::process_where_clause(&where_clause, hash_map)
            ));
        }
        
        // Add aggregation or column selection
        match &ast.select {
            SelectClause::Aggregate(agg) => {
                let agg_str = match agg.function {
                    AggregateType::Max => "max",
                    AggregateType::Min => "min",
                    AggregateType::Avg => "avg",
                };
                
                
                final_string.push_str(&format!(".{}()", agg_str));
                //TODO, find out how max works in renoir and fix this
                
            }
            SelectClause::ComplexValue(col, char ,val  )=> {
                let value = match &val {
                    AquaLiteral::Float(val) => format!("{:.2}", val),
                    AquaLiteral::Integer(val) => val.to_string(),
                    _ => unreachable!(), // TODO: remove unreachable
                };
                if char == "^" {
                    final_string.push_str(&format!(".map(|x| x.{}.unwrap().pow({}))", hash_map[&col.column],value));
                } else {
                    final_string.push_str(&format!(".map(|x| x.{}.unwrap() {} {})", hash_map[&col.column], char, value));
                }
            }
            SelectClause::Column(col) => {
                if col.column != "*" {
                    final_string.push_str(&format!(".map(|x| x.{}.unwrap())", hash_map[&col.column]));
                }
            }
    }
    
    println!("Final string: {}", final_string);
    final_string
}

// Helper function to recursively process where conditions
fn process_where_clause(clause: &WhereClause, hash_map: &HashMap<String, String>) -> String {
    let mut current = clause;
    let mut conditions = Vec::new();
    
    // Process first condition
    conditions.push(Self::process_condition(&current.condition, hash_map));
    
    // Process remaining conditions
    while let (Some(op), Some(next)) = (&current.binary_op, &current.next) {
        let op_str = match op {
            BinaryOp::And => "&&",
            BinaryOp::Or => "||",
        };
        conditions.push(op_str.to_string());
        conditions.push(Self::process_condition(&next.condition, hash_map));
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
        AquaLiteral::ColumnRef(column_ref) => Self::convert_column_ref(&column_ref),
    };

    format!(
        "x.{}.unwrap() {} {}",
        hash_map[&condition.variable.column],
        operator_str,
        value
    )

    }

    // helper function to convert column reference to string
    fn convert_column_ref(column_ref: &ColumnRef) -> String {
        match &column_ref.table {
            Some(table) => format!("{}.{}", table, column_ref.column),
            None => column_ref.column.clone(),
        }
    }
} 