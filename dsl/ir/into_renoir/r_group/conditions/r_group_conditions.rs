use crate::dsl::ir::ir_ast_structure::{
    AggregateType, ComplexField, GroupBaseCondition, GroupClause,
};
use crate::dsl::ir::r_group::r_group_keys::{GroupAccumulatorInfo, GroupAccumulatorValue};
use crate::dsl::ir::QueryObject;

// Function to parse group conditions and collect necessary information
pub fn parse_group_conditions(
    condition: &GroupClause,
    query_object: &QueryObject,
    acc_info: &mut GroupAccumulatorInfo,
    group_by: &Group,
) {
    // Collect and validate aggregates in the condition
    match condition {
        GroupClause::Base(base_cond) => {
            match base_cond {
                GroupBaseCondition::Comparison(comp) => {
                    // Process both sides of comparison
                    collect_field_aggregates(&comp.left_field, acc_info, query_object, group_by);
                    collect_field_aggregates(&comp.right_field, acc_info, query_object, group_by);

                    // Type check the comparison
                    let left_type = query_object.get_complex_field_type(&comp.left_field);
                    let right_type = query_object.get_complex_field_type(&comp.right_field);

                    // Validate types are compatible for comparison
                    if left_type != right_type {
                        if !((left_type == "f64" || left_type == "i64" || left_type == "usize")
                            && (right_type == "f64"
                                || right_type == "i64"
                                || right_type == "usize"))
                        {
                            panic!(
                                "Invalid comparison between incompatible types: {} and {}",
                                left_type, right_type
                            );
                        }
                    }
                }
                GroupBaseCondition::NullCheck(null_check) => {
                    collect_field_aggregates(&null_check.field, acc_info, query_object, group_by);
                }
            }
        }
        GroupClause::Expression { left, op: _, right } => {
            // Recursively process both sides of the expression
            parse_group_conditions(left, query_object, acc_info, group_by);
            parse_group_conditions(right, query_object, acc_info, group_by);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////

// Helper function to collect aggregates from a ComplexField
fn collect_field_aggregates(
    field: &ComplexField,
    acc_info: &mut GroupAccumulatorInfo,
    query_object: &QueryObject,
    group_by: &Group,
) {
    match field {
        ComplexField {
            column_ref: Some(col),
            ..
        } => {
            // Validate that the column is either in GROUP BY or used in aggregate
            if !group_by.columns.iter().any(|c| c.column == col.column) {
                panic!(
                    "Column {} must appear in GROUP BY or be used in aggregate function",
                    col.column
                );
            }
        }
        ComplexField {
            aggregate: Some(agg),
            ..
        } => {
            // Process aggregate function
            let col_type = query_object.get_type(&agg.column);
            match agg.function {
                AggregateType::Avg => {
                    acc_info.add_avg(agg.column.clone(), col_type);
                }
                AggregateType::Count => {
                    acc_info.add_aggregate(
                        GroupAccumulatorValue::Aggregate(agg.function.clone(), agg.column.clone()),
                        "usize".to_string(),
                    );
                }
                _ => {
                    acc_info.add_aggregate(
                        GroupAccumulatorValue::Aggregate(agg.function.clone(), agg.column.clone()),
                        col_type,
                    );
                }
            }
        }
        ComplexField {
            nested_expr: Some(nested),
            ..
        } => {
            // Process nested expressions recursively
            let (left, op, right) = &**nested;

            collect_field_aggregates(left, acc_info, query_object, group_by);
            collect_field_aggregates(right, acc_info, query_object, group_by);

            // Validate operation types
            let left_type = query_object.get_complex_field_type(left);
            let right_type = query_object.get_complex_field_type(right);

            // Check arithmetic operations are only performed on numeric types
            if op == "+" || op == "-" || op == "*" || op == "/" || op == "^" {
                if left_type != "f64" && left_type != "i64" {
                    panic!(
                        "Invalid arithmetic operation on non-numeric type: {}",
                        left_type
                    );
                }
                if right_type != "f64" && right_type != "i64" {
                    panic!(
                        "Invalid arithmetic operation on non-numeric type: {}",
                        right_type
                    );
                }
            }
        }
        ComplexField {
            literal: Some(_), ..
        } => {
            // Literals don't need special processing for aggregates
        }
        _ => panic!("Invalid ComplexField - no valid content"),
    }
}
