use crate::dsl::ir::ir_ast_structure::{GroupByClause, GroupCondition};
use crate::dsl::ir::ir_ast_structure::{GroupConditionType, NullOp};
use crate::dsl::ir::r_utils::{check_alias, convert_literal};
use crate::dsl::ir::{BinaryOp, ComparisonOp};
use crate::dsl::ir::{ColumnRef, QueryObject};

/// Process the GroupByClause from Ir AST and generate the corresponding Renoir operator string.
///
/// # Arguments
///
/// * `group_by` - The GroupByClause from the Ir AST containing group by columns and having conditions
/// * `query_object` - The QueryObject containing metadata about tables and columns
///
/// # Returns
///
/// A String containing the Renoir operator chain for the group by operation
pub fn process_group_by(group_by: &GroupByClause, query_object: &QueryObject) -> String {
    let mut group_string = String::new();

    // Process group by keys
    group_string.push_str(&format!(
        ".group_by(|x| ({}))",
        process_group_by_keys(&group_by.columns, query_object)
    ));

    // Process having clause if present
    if let Some(having) = &group_by.group_condition {
        group_string.push_str(&process_having_clause(having, query_object, group_by));
    }

    // Drop key as we're not maintaining Keyed streams
    group_string.push_str(".drop_key()");

    group_string
}

/// Process the group by keys and generate the corresponding tuple of column references.
///
/// # Arguments
///
/// * `columns` - Vector of ColumnRef representing the group by columns
/// * `query_object` - The QueryObject containing metadata about tables and columns
///
/// # Returns
///
/// A String containing the tuple of column references for group by
fn process_group_by_keys(columns: &Vec<ColumnRef>, query_object: &QueryObject) -> String {
    if !query_object.has_join {
        // No joins - simple reference to columns
        columns
            .iter()
            .map(|col| {
                //validate column_ref
                query_object.check_column_validity(col, &String::new());
                format!("x.{}.clone()", col.column)})
            .collect::<Vec<_>>()
            .join(", ")
    } else {
        // With joins - need to handle tuple access
        columns
            .iter()
            .map(|col| {
                let table = col.table.as_ref().unwrap();
                let table_name = check_alias(table, query_object);

                //validate column_ref
                query_object.check_column_validity(col, &table_name);

                format!(
                    "x{}.{}.clone()",
                    query_object.table_to_tuple_access.get(&table_name).unwrap(),
                    col.column
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Process the having clause and generate the corresponding filter operator.
///
/// # Arguments
///
/// * `having` - The GroupCondition containing the having clause conditions
/// * `query_object` - The QueryObject containing metadata about tables and columns
///
/// # Returns
///
/// A String containing the filter operator for the having clause
fn process_having_clause(
    having: &GroupCondition,
    query_object: &QueryObject,
    group_by: &GroupByClause,
) -> String {
    let mut having_string = String::new();

    // Start with .filter
    having_string.push_str(".filter(|x| ");

    // Process the conditions recursively
    having_string.push_str(&process_having_condition(having, query_object, group_by));

    // Close the filter
    having_string.push_str(")");

    having_string
}

/// Process a single having condition recursively.
///
/// # Arguments
///
/// * `condition` - The GroupCondition containing the condition to process
/// * `query_object` - The QueryObject containing metadata about tables and columns
///
/// # Returns
///
/// A String containing the condition expression for the filter
// In r_group.rs, modify process_having_condition function

fn process_having_condition(
    condition: &GroupCondition,
    query_object: &QueryObject,
    group_by: &GroupByClause,
) -> String {
    let mut condition_string: String;

    match &condition.condition {
        GroupConditionType::Comparison(comp) => {
            let operator_str = match comp.operator {
                ComparisonOp::GreaterThan => ">",
                ComparisonOp::LessThan => "<",
                ComparisonOp::Equal => "==",
                ComparisonOp::GreaterThanEquals => ">=",
                ComparisonOp::LessThanEquals => "<=",
                ComparisonOp::NotEqual => "!=",
            };

            let is_left_column = comp.left_field.column_ref.is_some();
            let is_right_column = comp.right_field.column_ref.is_some();

            // Case 1: Both sides are columns
            if is_left_column && is_right_column {
                let left_col = comp.left_field.column_ref.as_ref().unwrap();
                let right_col = comp.right_field.column_ref.as_ref().unwrap();
                
                // Add column validation
                query_object.check_column_validity(left_col, &String::new());
                query_object.check_column_validity(right_col, &String::new());
                
                let is_left_key = group_by.columns.iter().any(|c| c.column == left_col.column);
                let is_right_key = group_by.columns.iter().any(|c| c.column == right_col.column);

                let left_access = if is_left_key {
                    format!("x.0")
                } else {
                    format!("x.1.{}", left_col.column)
                };

                let right_access = if is_right_key {
                    format!("x.0")
                } else {
                    format!("x.1.{}", right_col.column)
                };

                condition_string = format!(
                    "if {}.is_some() && {}.is_some() {{ {}{}.unwrap() {} {}{}.unwrap() }} else {{ false }}", 
                    left_access,
                    right_access,
                    left_access,
                    if query_object.get_type(left_col).contains("String") { ".as_ref()" } else { "" },
                    operator_str,
                    right_access,
                    if query_object.get_type(right_col).contains("String") { ".as_ref()" } else { "" }
                );
            }
            // Case 2: Left is column, right is literal
            else if is_left_column {
                let left_col = comp.left_field.column_ref.as_ref().unwrap();
                let right_val = convert_literal(&comp.right_field.literal.as_ref().unwrap());
                
                // Add column validation
                query_object.check_column_validity(left_col, &String::new());
                
                let is_left_key = group_by.columns.iter().any(|c| c.column == left_col.column);
                let left_access = if is_left_key {
                    format!("x.0")
                } else {
                    format!("x.1.{}", left_col.column)
                };

                condition_string = format!(
                    "if {}.is_some() {{ {}{}.unwrap() {} {} }} else {{ false }}", 
                    left_access,
                    left_access,
                    if query_object.get_type(left_col).contains("String") { ".as_ref()" } else { "" },
                    operator_str,
                    right_val
                );
            }
            // Case 3: Right is column, left is literal
            else if is_right_column {
                let right_col = comp.right_field.column_ref.as_ref().unwrap();
                let left_val = convert_literal(&comp.left_field.literal.as_ref().unwrap());
                
                // Add column validation
                query_object.check_column_validity(right_col, &String::new());
                
                let is_right_key = group_by.columns.iter().any(|c| c.column == right_col.column);
                let right_access = if is_right_key {
                    format!("x.0")
                } else {
                    format!("x.1.{}", right_col.column)
                };

                condition_string = format!(
                    "if {}.is_some() {{ {} {} {}{}.unwrap() }} else {{ false }}", 
                    right_access,
                    left_val,
                    operator_str,
                    right_access,
                    if query_object.get_type(right_col).contains("String") { ".as_ref()" } else { "" }
                );
            }
            // Case 4: Both sides are literals
            else {
                let left_val = convert_literal(&comp.left_field.literal.as_ref().unwrap());
                let right_val = convert_literal(&comp.right_field.literal.as_ref().unwrap());
                condition_string = format!("{} {} {}", left_val, operator_str, right_val);
            }
        },
        GroupConditionType::NullCheck(null_check) => {
            let field = if null_check.field.column_ref.is_some() {
                // Validate column
                let col_ref = null_check.field.column_ref.as_ref().unwrap();
                query_object.check_column_validity(col_ref, &String::new());
                
                // Check if it's a group key column
                let is_key = group_by.columns.iter().any(|c| c.column == col_ref.column);
                if is_key {
                    format!("x.0")
                } else {
                    format!("x.1.{}", col_ref.column)
                }
            } else {
                panic!("Invalid null check condition - missing column reference")
            };

            match null_check.operator {
                NullOp::IsNull => condition_string = format!("{}.is_none()", field),
                NullOp::IsNotNull => condition_string = format!("{}.is_some()", field),
            }
        }
    }

    // Process binary operator and next condition if present
    if let (Some(op), Some(next)) = (&condition.binary_op, &condition.next) {
        let op_str = match op {
            BinaryOp::And => " && ",
            BinaryOp::Or => " || ",
        };
        condition_string.push_str(op_str);
        condition_string.push_str(&process_having_condition(next, query_object, group_by));
    }

    condition_string
}
