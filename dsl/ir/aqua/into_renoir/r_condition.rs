use crate::dsl::ir::aqua::BinaryOp;
use crate::dsl::ir::aqua::WhereClause;
use crate::dsl::ir::aqua::QueryObject;
use crate::dsl::ir::aqua::{AquaLiteral,ComparisonOp, Condition};
use crate::dsl::ir::aqua::r_utils::*;



// Helper function to recursively process where conditions
pub fn process_where_clause(clause: &WhereClause, query_object: &QueryObject) -> String {
    let mut current = clause;
    let mut conditions = Vec::new();
    
    // Process first condition
    conditions.push(process_condition(&current.condition, query_object));
    
    // Process remaining conditions
    while let (Some(op), Some(next)) = (&current.binary_op, &current.next) {
        let op_str = match op {
            BinaryOp::And => "&&",
            BinaryOp::Or => "||",
        };
        conditions.push(op_str.to_string());
        conditions.push(process_condition(&next.condition, query_object));
        current = next;
    }
    
    conditions.join(" ")
}

// Helper function to process a single condition
fn process_condition(condition: &Condition, query_object: &QueryObject) -> String {
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
        AquaLiteral::ColumnRef(column_ref) => convert_column_ref(&column_ref, query_object),
    };


    let table_names = query_object.get_all_table_names();

    if !query_object.has_join {
        return format!(
            "x.{}.unwrap() {} {}",
            query_object.table_to_struct.get(table_names.first().unwrap()).unwrap().get(&condition.variable.column).unwrap(),
            operator_str,
            value
        );
    }
    else {
        let table_name = check_alias(&condition.variable.table.clone().unwrap(), query_object);
        return format!(
            "x.{}.{}.unwrap() {} {}",
            query_object.table_to_struct_name.get(&table_name).unwrap().chars().last().unwrap(),
            query_object.table_to_struct.get(&table_name).unwrap().get(&condition.variable.column).unwrap(),
            operator_str,
            value
        );
    }

}