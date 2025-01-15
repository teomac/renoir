use std::collections::HashMap;
use crate::dsl::ir::aqua::ast_parser::ast_structure::*;

pub struct OperatorChain;

impl OperatorChain {
    pub fn process_where_clause(clause: &WhereClause, hash_map: &HashMap<String, String>) -> String {
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
            AquaLiteral::String(val) => format!("\"{}\"", val),
            AquaLiteral::Boolean(val) => val.to_string(),
            AquaLiteral::ColumnRef(col_ref) => {
                if let Some(table) = &col_ref.table {
                    format!("{}.{}", table, hash_map[&col_ref.column])
                } else {
                    format!("x.{}.unwrap()", hash_map[&col_ref.column])
                }
            }
        };

        if let Some(table) = &condition.variable.table {
            format!(
                "{}.{}.unwrap() {} {}",
                table,
                hash_map[&condition.variable.column],
                operator_str,
                value
            )
        } else {
            format!(
                "x.{}.unwrap() {} {}",
                hash_map[&condition.variable.column],
                operator_str,
                value
            )
        }
    }

    pub fn process_join(join_clause: &JoinClause, hash_map: &HashMap<String, String>) -> String {
        let join_condition = &join_clause.condition;
        
        format!(
            "join {} in input2 on {}.{} == {}.{}",
            join_clause.scan.stream_name,
            join_condition.left_col.table.as_ref().unwrap(),
            hash_map[&join_condition.left_col.column],
            join_condition.right_col.table.as_ref().unwrap(),
            hash_map[&join_condition.right_col.column]
        )
    }

    pub fn process_select(select: &SelectClause, hash_map: &HashMap<String, String>) -> String {
        match select {
            SelectClause::Column(col_ref) => {
                if let Some(table) = &col_ref.table {
                    format!("select {}.{}", table, hash_map[&col_ref.column])
                } else {
                    format!("select {}", hash_map[&col_ref.column])
                }
            },
            SelectClause::Aggregate(agg) => {
                let agg_str = match agg.function {
                    AggregateType::Max => "max",
                    AggregateType::Min => "min",
                    AggregateType::Avg => "avg",
                };
                
                if let Some(table) = &agg.column.table {
                    format!("select {}({}.{})", agg_str, table, hash_map[&agg.column.column])
                } else {
                    format!("select {}({})", agg_str, hash_map[&agg.column.column])
                }
            },
            SelectClause::ComplexValue(col_ref, op, val) => {
                let value = match val {
                    AquaLiteral::Float(val) => format!("{:.2}", val),
                    AquaLiteral::Integer(val) => val.to_string(),
                    AquaLiteral::String(val) => format!("\"{}\"", val),
                    AquaLiteral::Boolean(val) => val.to_string(),
                    AquaLiteral::ColumnRef(ref_col) => {
                        if let Some(table) = &ref_col.table {
                            format!("{}.{}", table, hash_map[&ref_col.column])
                        } else {
                            hash_map[&ref_col.column].clone()
                        }
                    }
                };

                if let Some(table) = &col_ref.table {
                    format!("select {}.{} {} {}", table, hash_map[&col_ref.column], op, value)
                } else {
                    format!("select {} {} {}", hash_map[&col_ref.column], op, value)
                }
            }
        }
    }
}