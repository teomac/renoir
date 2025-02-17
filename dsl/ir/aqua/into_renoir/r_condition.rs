use crate::dsl::ir::aqua::ir_ast_structure::{WhereConditionType, NullCondition, NullOp};
use crate::dsl::ir::aqua::BinaryOp;
use crate::dsl::ir::aqua::WhereClause;
use crate::dsl::ir::aqua::QueryObject;
use crate::dsl::ir::aqua::{ComparisonOp, Condition};
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

/// Process a condition which can be either a comparison or a null check
fn process_condition(condition: &WhereConditionType, query_object: &QueryObject) -> String {
    match condition {
        WhereConditionType::Comparison(comparison) => {
            process_comparison_condition(comparison, query_object)
        },
        WhereConditionType::NullCheck(null_check) => {
            process_null_check_condition(null_check, query_object)
        }
    }
}

/// Process a null check condition (IS NULL or IS NOT NULL)
fn process_null_check_condition(condition: &NullCondition, query_object: &QueryObject) -> String {
    if !query_object.has_join {
        // Simple case - no joins
        let field = if condition.field.column_ref.is_some() {
            //validate column
            query_object.check_column_validity(&condition.field.column_ref.as_ref().unwrap(), query_object.get_all_table_names().first().unwrap());
            format!(
                "x.{}", 
                condition.field.column_ref.as_ref().unwrap().column
            )
        } else {
            panic!("Invalid null check condition - missing column reference")
        };

        match condition.operator {
            NullOp::IsNull => format!("{}.is_none()", field),
            NullOp::IsNotNull => format!("{}.is_some()", field),
        }
    } else {
        // Case with joins
        let field = if condition.field.column_ref.is_some() {
            let col_ref = condition.field.column_ref.as_ref().unwrap();
            let table_name = check_alias(
                &col_ref.table.clone().unwrap(), 
                query_object
            );
            //validate column
            query_object.check_column_validity(&col_ref, &table_name);
            
            format!(
                "x{}.{}", 
                query_object.table_to_tuple_access.get(&table_name).unwrap(),
                col_ref.column
            )
        } else {
            panic!("Invalid null check condition - missing column reference")
        };

        match condition.operator {
            NullOp::IsNull => format!("{}.is_none()", field),
            NullOp::IsNotNull => format!("{}.is_some()", field),
        }
    }
}

/// Process a comparison condition (>, <, =, etc.)
fn process_comparison_condition(condition: &Condition, query_object: &QueryObject) -> String {
    let operator_str = match condition.operator {
        ComparisonOp::GreaterThan => ">",
        ComparisonOp::LessThan => "<",
        ComparisonOp::Equal => "==",
        ComparisonOp::GreaterThanEquals => ">=",
        ComparisonOp::LessThanEquals => "<=",
        ComparisonOp::NotEqual => "!=",
    };

    let is_left_column = condition.left_field.column_ref.is_some();
    let is_right_column = condition.right_field.column_ref.is_some();

    if !query_object.has_join {
        // Case 1: Both sides are columns
        if is_left_column && is_right_column {
            let left_col = &condition.left_field.column_ref.as_ref().unwrap();
            let right_col = &condition.right_field.column_ref.as_ref().unwrap();
            
            //validate columns
            query_object.check_column_validity(left_col, query_object.get_all_table_names().first().unwrap());
            query_object.check_column_validity(right_col, query_object.get_all_table_names().first().unwrap());

            return format!(
                "if x.{}.is_some() && x.{}.is_some() {{ x.{}{}.unwrap() {} x.{}{}.unwrap() }} else {{ false }}", 
                left_col.column,
                right_col.column,
                left_col.column,
                if query_object.get_type(left_col).contains("String") { ".as_ref()" } else { "" },
                operator_str,
                right_col.column,
                if query_object.get_type(right_col).contains("String") { ".as_ref()" } else { "" }
            );
        }

        // Case 2: Left is column, right is literal
        if is_left_column {
            let left_col = &condition.left_field.column_ref.as_ref().unwrap();
            let right_val = convert_literal(&condition.right_field.literal.as_ref().unwrap());
            
            //validate column
            query_object.check_column_validity(left_col, query_object.get_all_table_names().first().unwrap());

            return format!(
                "if x.{}.is_some() {{ x.{}{}.unwrap() {} {} }} else {{ false }}", 
                left_col.column,
                left_col.column,
                if query_object.get_type(left_col).contains("String") { ".as_ref()" } else { "" },
                operator_str,
                right_val
            );
        }

        // Case 3: Right is column, left is literal
        if is_right_column {
            let right_col = &condition.right_field.column_ref.as_ref().unwrap();
            let left_val = convert_literal(&condition.left_field.literal.as_ref().unwrap());
            
            //validate column
            query_object.check_column_validity(right_col, query_object.get_all_table_names().first().unwrap());

            return format!(
                "if x.{}.is_some() {{ {} {} x.{}{}.unwrap() }} else {{ false }}", 
                right_col.column,
                left_val,
                operator_str,
                right_col.column,
                if query_object.get_type(right_col).contains("String") { ".as_ref()" } else { "" }
            );
        }

        // Case 4: Both sides are literals
        let left_val = convert_literal(&condition.left_field.literal.as_ref().unwrap());
        let right_val = convert_literal(&condition.right_field.literal.as_ref().unwrap());
        return format!("{} {} {}", left_val, operator_str, right_val);
    } else {
        // JOIN CASES
        
        // Case 1: Both sides are columns
        if is_left_column && is_right_column {
            let left_col = &condition.left_field.column_ref.as_ref().unwrap();
            let right_col = &condition.right_field.column_ref.as_ref().unwrap();
            
            let left_table_name = check_alias(&left_col.table.clone().unwrap(), query_object);
            let right_table_name = check_alias(&right_col.table.clone().unwrap(), query_object);
            
            //validate columns
            query_object.check_column_validity(left_col, &left_table_name);
            query_object.check_column_validity(right_col, &right_table_name);

            return format!(
                "if x{}.{}.is_some() && x{}.{}.is_some() {{ x{}.{}{}.unwrap() {} x{}.{}{}.unwrap() }} else {{ false }}", 
                query_object.table_to_tuple_access.get(&left_table_name).unwrap(),
                left_col.column,
                query_object.table_to_tuple_access.get(&right_table_name).unwrap(),
                right_col.column,
                query_object.table_to_tuple_access.get(&left_table_name).unwrap(),
                left_col.column,
                if query_object.get_type(left_col).contains("String") { ".as_ref()" } else { "" },
                operator_str,
                query_object.table_to_tuple_access.get(&right_table_name).unwrap(),
                right_col.column,
                if query_object.get_type(right_col).contains("String") { ".as_ref()" } else { "" }
            );
        }

        // Case 2: Left is column, right is literal
        if is_left_column {
            let left_col = &condition.left_field.column_ref.as_ref().unwrap();
            let right_val = convert_literal(&condition.right_field.literal.as_ref().unwrap());
            
            let left_table_name = check_alias(&left_col.table.clone().unwrap(), query_object);
            
            //validate column
            query_object.check_column_validity(left_col, &left_table_name);

            return format!(
                "if x{}.{}.is_some() {{ x{}.{}{}.unwrap() {} {} }} else {{ false }}", 
                query_object.table_to_tuple_access.get(&left_table_name).unwrap(),
                left_col.column,
                query_object.table_to_tuple_access.get(&left_table_name).unwrap(),
                left_col.column,
                if query_object.get_type(left_col).contains("String") { ".as_ref()" } else { "" },
                operator_str,
                right_val
            );
        }

        // Case 3: Right is column, left is literal
        if is_right_column {
            let right_col = &condition.right_field.column_ref.as_ref().unwrap();
            let left_val = convert_literal(&condition.left_field.literal.as_ref().unwrap());
            
            let right_table_name = check_alias(&right_col.table.clone().unwrap(), query_object);
            
            //validate column
            query_object.check_column_validity(right_col, &right_table_name);

            return format!(
                "if x{}.{}.is_some() {{ {} {} x{}.{}{}.unwrap() }} else {{ false }}", 
                query_object.table_to_tuple_access.get(&right_table_name).unwrap(),
                right_col.column,
                left_val,
                operator_str,
                query_object.table_to_tuple_access.get(&right_table_name).unwrap(),
                right_col.column,
                if query_object.get_type(right_col).contains("String") { ".as_ref()" } else { "" }
            );
        }

        // Case 4: Both sides are literals
        let left_val = convert_literal(&condition.left_field.literal.as_ref().unwrap());
        let right_val = convert_literal(&condition.right_field.literal.as_ref().unwrap());
        return format!("{} {} {}", left_val, operator_str, right_val);
    }
}