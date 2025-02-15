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

    let table_names = query_object.get_all_table_names();

    let is_left_column = condition.left_field.column_ref.is_some();
    let is_right_column = condition.right_field.column_ref.is_some();

    let mut result_string = String::new();

    //case no join
    if !query_object.has_join {
        // For single table queries
        if is_left_column {
            //validate column
            query_object.check_column_validity(&condition.left_field.column_ref.as_ref().unwrap(), table_names.first().unwrap());

            result_string.push_str("if ");
            result_string.push_str(format!(
                "x.{}.is_some() {{ x.{}.unwrap()",
                if query_object.table_to_struct.get(table_names.first().unwrap()).unwrap().get(&condition.left_field.column_ref.clone().unwrap().to_string()).is_some() {
                    
                    condition.left_field.column_ref.clone().unwrap().to_string()
                } else {
                    "ERROR".to_string()
                },
                if query_object.table_to_struct.get(table_names.first().unwrap()).unwrap().get(&condition.left_field.column_ref.clone().unwrap().to_string()).is_some() {
                    if query_object.table_to_struct.get(table_names.first().unwrap()).unwrap().get(&condition.left_field.column_ref.clone().unwrap().to_string()).unwrap().contains("String"){
                        format!("{}.as_ref()", condition.left_field.column_ref.clone().unwrap().to_string())
                    }
                    else{
                    condition.left_field.column_ref.clone().unwrap().to_string()}
                } else {
                    "ERROR".to_string()
                }
            ).as_str());
        } else {
            result_string.push_str(format!(
                "{}",
                convert_literal(&condition.left_field.literal.as_ref().unwrap())
            ).as_str());
        }
    
        // Push operator
        result_string.push_str(format!(
            " {} ",
            operator_str
        ).as_str());
    
        if is_right_column {
            //validate column
            query_object.check_column_validity(&condition.right_field.column_ref.as_ref().unwrap(), table_names.first().unwrap());

            result_string.push_str(format!(
                "{}",
                if query_object.table_to_struct.get(table_names.first().unwrap()).unwrap().get(&condition.right_field.column_ref.clone().unwrap().to_string()).is_some() {
                    format!("if x.{}.is_some() {{ x.{}.unwrap() }} else {{ false }}", 
                        condition.right_field.column_ref.clone().unwrap().to_string(),
                        if query_object.table_to_struct.get(table_names.first().unwrap()).unwrap().get(&condition.right_field.column_ref.clone().unwrap().to_string()).unwrap().contains("String"){
                            format!("{}.as_ref()", condition.right_field.column_ref.clone().unwrap().to_string())
                        }
                        else{
                        condition.right_field.column_ref.clone().unwrap().to_string()}
                    )
                } else {
                    "ERROR".to_string()
                }
            ).as_str());
        } else {
            result_string.push_str(format!(
                "{}",
                convert_literal(&condition.right_field.literal.as_ref().unwrap())
            ).as_str());
        }
    
        if is_left_column {
            result_string.push_str(" } else { false }");
        }
    
        result_string
    } else {
        // For queries with joins
        if is_left_column {
            let table_name = check_alias(&condition.left_field.column_ref.clone().unwrap().table.clone().unwrap(), query_object);
            //validate column
            query_object.check_column_validity(&condition.left_field.column_ref.clone().unwrap(), &table_name);

            result_string.push_str("if ");
            result_string.push_str(format!(
                "x{}.{}.is_some() {{ x{}.{}{}.unwrap()",
                query_object.table_to_tuple_access.get(&table_name).unwrap(),
                if query_object.table_to_struct.get(&table_name).unwrap().get(&condition.left_field.column_ref.clone().unwrap().column).is_some() {
                    condition.left_field.column_ref.clone().unwrap().column
                } else {
                    "ERROR".to_string()
                },
                query_object.table_to_tuple_access.get(&table_name).unwrap(),
                if query_object.table_to_struct.get(&table_name).unwrap().get(&condition.left_field.column_ref.clone().unwrap().column).is_some() {
                    condition.left_field.column_ref.clone().unwrap().column
                } else {
                    "ERROR".to_string()
                },
                if query_object.get_type(&condition.left_field.column_ref.clone().unwrap()).contains("String") { ".as_ref()" } else { "" }
            ).as_str());
        } else {
            result_string.push_str(format!(
                "{}",
                convert_literal(&condition.left_field.literal.as_ref().unwrap())
            ).as_str());
        }
    
        // Push operator
        result_string.push_str(format!(
            " {} ",
            operator_str
        ).as_str());
    
        if is_right_column {
            let table_name = check_alias(&condition.right_field.column_ref.clone().unwrap().table.clone().unwrap(), query_object);
            //validate column
            query_object.check_column_validity(&condition.right_field.column_ref.clone().unwrap(), &table_name);

            result_string.push_str(format!(
                "{}",
                format!("if x{}.{}.is_some() {{ x{}.{}{}.unwrap() }} else {{ false }}",
                    query_object.table_to_tuple_access.get(&table_name).unwrap(),
                    if query_object.table_to_struct.get(&table_name).unwrap().get(&condition.right_field.column_ref.clone().unwrap().column).is_some() {
                        condition.right_field.column_ref.clone().unwrap().column
                    } else {
                        "ERROR".to_string()
                    },
                    query_object.table_to_tuple_access.get(&table_name).unwrap(),
                    if query_object.table_to_struct.get(&table_name).unwrap().get(&condition.right_field.column_ref.clone().unwrap().column).is_some() {
                        condition.right_field.column_ref.clone().unwrap().column
                    } else {
                        "ERROR".to_string()
                    },
                    if query_object.get_type(&condition.right_field.column_ref.clone().unwrap()).contains("String") { ".as_ref()" } else { "" }
                )
            ).as_str());
        } else {
            result_string.push_str(format!(
                "{}",
                convert_literal(&condition.right_field.literal.as_ref().unwrap())
            ).as_str());
        }
    
        if is_left_column {
            result_string.push_str(" } else { false }");
        }
    
        result_string
    }
}