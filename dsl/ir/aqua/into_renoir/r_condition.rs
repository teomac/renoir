use crate::dsl::ir::aqua::BinaryOp;
use crate::dsl::ir::aqua::WhereClause;
use crate::dsl::ir::aqua::QueryObject;
use crate::dsl::ir::aqua::{AquaLiteral,ComparisonOp, Condition};
use crate::dsl::ir::aqua::r_utils::*;

/// Processes a `WhereClause` and generates a string representation of the conditions.
///
/// This function recursively processes the conditions in the `WhereClause` and converts them
/// into a string format that contains renoir operators. It handles both the initial condition
/// and any subsequent conditions connected by binary operators (AND/OR).
///
/// # Arguments
///
/// * `clause` - A reference to the `WhereClause` to be processed.
/// * `query_object` - A reference to the `QueryObject` containing metadata about the query.
///
/// # Returns
///
/// A `String` representing the processed where clause conditions.
    // function implementation

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


/// Processes a single `Condition` and generates a string representation of the condition.
///
/// This function converts a single `Condition` into a string format that contains renoir operators.
/// It handles different types of comparison operators and literal values, and formats them
/// appropriately based on whether the query involves a join or not.
///
/// # Arguments
///
/// * `condition` - A reference to the `Condition` to be processed.
/// * `query_object` - A reference to the `QueryObject` containing metadata about the query.
///
/// # Returns
///
/// A `String` representing the processed condition.
    // function implementation

fn process_condition(condition: &Condition, query_object: &QueryObject) -> String {
    let operator_str = match condition.operator {
        ComparisonOp::GreaterThan => ">",
        ComparisonOp::LessThan => "<",
        ComparisonOp::Equal => "==",
        ComparisonOp::GreaterThanEquals => ">=",
        ComparisonOp::LessThanEquals => "<=",
        ComparisonOp::NotEqual => "!=",
    };

    let mut is_string: bool = false;

    let value = match &condition.value {
        AquaLiteral::Float(val) => format!("{:.2}", val),
        AquaLiteral::Integer(val) => val.to_string(),
        AquaLiteral::String(val) => {
            is_string = true;
            format!("\"{}\"", val)},
        AquaLiteral::Boolean(val) => val.to_string(),
        AquaLiteral::ColumnRef(column_ref) => convert_column_ref(&column_ref, query_object),
    };

    let table_names = query_object.get_all_table_names();

    if !query_object.has_join {
        return format!(
            "x.{}{}.unwrap() {} {}",
            query_object.table_to_struct.get(table_names.first().unwrap()).unwrap().get(&condition.variable.column).unwrap(),
            if is_string { ".clone()" } else { "" },
            operator_str,
            value
        );
    }
    else {
        let table_name = check_alias(&condition.variable.table.clone().unwrap(), query_object);
        return format!(
            "x{}.{}{}.unwrap() {} {}",
            query_object.table_to_tuple_access.get(&table_name).unwrap(),
            query_object.table_to_struct.get(&table_name).unwrap().get(&condition.variable.column).unwrap(),
            if is_string { ".clone()" } else { "" },
            operator_str,
            value
        );
    }

}