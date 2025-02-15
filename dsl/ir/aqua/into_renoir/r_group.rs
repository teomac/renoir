use crate::dsl::ir::aqua::ir_ast_structure::{ComplexField, GroupByClause, GroupCondition};
use crate::dsl::ir::aqua::ir_ast_structure::{GroupConditionType, NullOp};
use crate::dsl::ir::aqua::r_utils::{check_alias, convert_column_ref};
use crate::dsl::ir::aqua::{AggregateType, AquaLiteral, BinaryOp, ComparisonOp};
use crate::dsl::ir::aqua::{ColumnRef, QueryObject};

/// Process the GroupByClause from Aqua AST and generate the corresponding Renoir operator string.
///
/// # Arguments
///
/// * `group_by` - The GroupByClause from the Aqua AST containing group by columns and having conditions
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
    let mut condition_string = String::new();

    match &condition.condition {
        GroupConditionType::Comparison(comp) => {
            // Get left and right values
            let left_field = convert_complex_field(&comp.left_field, query_object, group_by);
            let right_field = convert_complex_field(&comp.right_field, query_object, group_by);

            condition_string = if let Some(col) = &comp.left_field.column_ref {
                let col_access = if let Some(key_position) = group_by
                    .columns
                    .iter()
                    .position(|gc| gc.column == col.column && gc.table == col.table)
                {
                    if group_by.columns.len() == 1 {
                        "x.0".to_string()
                    } else {
                        format!("x.0.{}", key_position)
                    }
                } else if !query_object.has_join {
                    format!("x.1.{}", col.column)
                } else {
                    let table = col.table.as_ref().unwrap();
                    let table_name = check_alias(table, query_object);
                    format!(
                        "x.1{}",
                        query_object.table_to_tuple_access.get(&table_name).unwrap()
                    )
                };

                let column_type = query_object.get_type(col);
                format!(
                    "(if {}.is_some() {{ {}{}.unwrap() {} {} }} else {{ false }})",
                    col_access,
                    col_access,
                    if column_type.contains("String") { ".as_ref()" } else { "" },
                    match comp.operator {
                        ComparisonOp::GreaterThan => ">",
                        ComparisonOp::LessThan => "<",
                        ComparisonOp::Equal => "==",
                        ComparisonOp::NotEqual => "!=",
                        ComparisonOp::GreaterThanEquals => ">=",
                        ComparisonOp::LessThanEquals => "<=",
                    },
                    right_field
                )
            } else {
                format!(
                    "{} {} {}",
                    left_field,
                    match comp.operator {
                        ComparisonOp::GreaterThan => ">",
                        ComparisonOp::LessThan => "<",
                        ComparisonOp::Equal => "==",
                        ComparisonOp::NotEqual => "!=",
                        ComparisonOp::GreaterThanEquals => ">=",
                        ComparisonOp::LessThanEquals => "<=",
                    },
                    right_field
                )
            };
        }
        GroupConditionType::NullCheck(null_check) => {
            let field_str = convert_complex_field(&null_check.field, query_object, group_by);

            match null_check.operator {
                NullOp::IsNull => condition_string.push_str(&format!("{}.is_none()", field_str)),
                NullOp::IsNotNull => condition_string.push_str(&format!("{}.is_some()", field_str)),
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

/// Convert a ComplexField to its string representation in Renoir.
///
/// # Arguments
///
/// * `field` - The ComplexField to convert
/// * `query_object` - The QueryObject containing metadata about tables and columns
///
/// # Returns
///
/// A String containing the Renoir representation of the field
fn convert_complex_field(
    field: &ComplexField,
    query_object: &QueryObject,
    group_by: &GroupByClause,
) -> String {
    if let Some(col) = &field.column_ref {
        //validate column_ref
        query_object.check_column_validity(col, &String::new());
        // Check if this column is part of the GROUP BY key
        if let Some(key_position) = group_by
            .columns
            .iter()
            .position(|gc| gc.column == col.column && gc.table == col.table)
        {
            let column_type = query_object.get_type(col);
            if group_by.columns.len() == 1 {
                format!(
                    "(if x.0.is_some() {{ x.0{}.unwrap() }} else {{ false }})",
                    if column_type.contains("String") { ".as_ref()" } else { "" }
                )
            } else {
                format!(
                    "(if x.0.{}.is_some() {{ x.0.{}{}.unwrap() }} else {{ false }})",
                    key_position, 
                    key_position,
                    if column_type.contains("String") { ".as_ref()" } else { "" }
                )
            }
        } else {
            if !query_object.has_join {
                let column_type = query_object.get_type(col);
                format!(
                    "(if x.1.{}.is_some() {{ x.1.{}{}.unwrap() }} else {{ false }})",
                    col.column,
                    col.column,
                    if column_type.contains("String") { ".as_ref()" } else { "" }
                )
            } else {
                let table = col.table.as_ref().unwrap();
                let table_name = check_alias(table, query_object);
                let column_type = query_object.get_type(col);
                format!(
                    "(if x.1{}.is_some() {{ x.1{}{}.unwrap() }} else {{ false }})",
                    query_object.table_to_tuple_access.get(&table_name).unwrap(),
                    query_object.table_to_tuple_access.get(&table_name).unwrap(),
                    if column_type.contains("String") { ".as_ref()" } else { "" }
                )
            }
        }
    } else if let Some(lit) = &field.literal {
        match lit {
            AquaLiteral::Integer(i) => i.to_string(),
            AquaLiteral::Float(f) => format!("{:.2}", f),
            AquaLiteral::String(s) => format!("\"{}\"", s),
            AquaLiteral::Boolean(b) => b.to_string(),
            AquaLiteral::ColumnRef(col_ref) => convert_column_ref(col_ref, query_object),
        }
    } else if let Some(agg) = &field.aggregate {
        // validate column_ref
        query_object.check_column_validity(&agg.column, &String::new());
        
        let column_type = query_object.get_type(&agg.column);
        let inner_col = if !query_object.has_join {
            format!(
                "if x.1.{}.is_some() {{ x.1.{}{}.unwrap() }} else {{ false }}",
                agg.column.column, 
                agg.column.column,
                if column_type.contains("String") { ".as_ref()" } else { "" }
            )
        } else {
            let table = agg.column.table.as_ref().unwrap();
            let table_name = check_alias(table, query_object);
            format!(
                "if x.1{}.is_some() {{ x.1{}{}.unwrap() }} else {{ false }}",
                query_object.table_to_tuple_access.get(&table_name).unwrap(),
                query_object.table_to_tuple_access.get(&table_name).unwrap(),
                if column_type.contains("String") { ".as_ref()" } else { "" }
            )
        };

        match agg.function {
            AggregateType::Max => format!("max({})", inner_col),
            AggregateType::Min => format!("min({})", inner_col),
            AggregateType::Avg => format!("avg({})", inner_col),
            AggregateType::Sum => format!("sum({})", inner_col),
            AggregateType::Count => {
                if agg.column.column == "*" {
                    "count()".to_string()
                } else {
                    format!("count({})", inner_col)
                }
            }
        }
    } else if let Some(nested) = &field.nested_expr {
        let (left, op, right) = &**nested;
        format!(
            "({} {} {})",
            convert_complex_field(left, query_object, group_by),
            op,
            convert_complex_field(right, query_object, group_by)
        )
    } else {
        panic!("Invalid ComplexField: no valid field type found");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;

    #[test]
    fn test_process_group_by_no_joins() {
        let mut query_object = QueryObject::new();
        query_object.has_join = false;

        let group_by = GroupByClause {
            columns: vec![ColumnRef {
                table: None,
                column: "col1".to_string(),
            }],
            group_condition: None,
        };

        let result = process_group_by(&group_by, &query_object);
        assert_eq!(result, ".group_by(|x| (x.col1.clone())).drop_key()");
    }

    #[test]
    fn test_process_group_by_with_joins() {
        let mut query_object = QueryObject::new();
        query_object.has_join = true;

        let mut table_to_tuple_access = IndexMap::new();
        table_to_tuple_access.insert("table1".to_string(), ".0".to_string());

        query_object.update_tuple_access(&table_to_tuple_access);

        let group_by = GroupByClause {
            columns: vec![ColumnRef {
                table: Some("table1".to_string()),
                column: "col1".to_string(),
            }],
            group_condition: None,
        };

        let result = process_group_by(&group_by, &query_object);
        assert_eq!(result, ".group_by(|x| (x.0.col1.clone())).drop_key()");
    }
}
