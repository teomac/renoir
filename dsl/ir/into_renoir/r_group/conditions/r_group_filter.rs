use crate::dsl::ir::ir_ast_structure::{
    AggregateType, ComplexField, GroupBaseCondition, GroupClause, NullOp,
};
use crate::dsl::ir::r_group::r_group_keys::GroupAccumulatorInfo;
use crate::dsl::ir::{AggregateFunction, BinaryOp, ComparisonOp, IrLiteral};
use crate::dsl::ir::{ColumnRef, QueryObject};

// Function to create the filter operation
pub fn create_filter_operation(
    condition: &GroupClause,
    keys: &Vec<ColumnRef>,
    query_object: &QueryObject,
    acc_info: &GroupAccumulatorInfo,
) -> String {
    let mut filter_str = String::new();
    filter_str.push_str(".filter(|x| ");

    // Process the conditions recursively
    filter_str.push_str(&process_filter_condition(
        condition,
        keys,
        query_object,
        acc_info,
    ));

    filter_str.push_str(")");

    filter_str
}

// Function to process filter conditions recursively
fn process_filter_condition(
    condition: &GroupClause,
    keys: &Vec<ColumnRef>,
    query_object: &QueryObject,
    acc_info: &GroupAccumulatorInfo,
) -> String {
    let mut check_list: Vec<String> = Vec::new();
    match condition {
        GroupClause::Base(base_condition) => {
            match base_condition {
                GroupBaseCondition::Comparison(comp) => {
                    let operator = match comp.operator {
                        ComparisonOp::GreaterThan => ">",
                        ComparisonOp::LessThan => "<",
                        ComparisonOp::Equal => "==",
                        ComparisonOp::GreaterThanEquals => ">=",
                        ComparisonOp::LessThanEquals => "<=",
                        ComparisonOp::NotEqual => "!=",
                    };

                    // Get types for both sides of comparison
                    let left_type = query_object.get_complex_field_type(&comp.left_field);
                    let right_type = query_object.get_complex_field_type(&comp.right_field);

                    // Process left and right expressions
                    let left_expr = process_filter_field(
                        &comp.left_field,
                        keys,
                        query_object,
                        acc_info,
                        &mut check_list,
                    );
                    let right_expr = process_filter_field(
                        &comp.right_field,
                        keys,
                        query_object,
                        acc_info,
                        &mut check_list,
                    );

                    let is_check_list_empty = check_list.is_empty(); // if true there is only one or more count
                                                                     // Deduplicate and the check list
                    check_list.sort();
                    check_list.dedup();

                    // Handle type conversions for comparison - improved handling for numeric types
                    if left_type != right_type {
                        if (left_type == "f64" || left_type == "i64" || left_type == "usize")
                            && (right_type == "f64" || right_type == "i64" || right_type == "usize")
                        {
                            if is_check_list_empty {
                                format!(
                                    "({} as f64) {} ({} as f64)",
                                    left_expr, operator, right_expr
                                )
                            } else {
                                // Cast both to f64
                                format!(
                                    "if {} {{({} as f64) {} ({} as f64)}} else {{ false }}",
                                    check_list.join(" && "),
                                    left_expr,
                                    operator,
                                    right_expr
                                )
                            }
                        } else {
                            if is_check_list_empty {
                                format!("{} {} {}", left_expr, operator, right_expr)
                            } else {
                                // Different non-numeric types - this should already be caught during validation
                                format!(
                                    "if {} {{({}) {} ({})}} else {{ false }}",
                                    check_list.join(" && "),
                                    left_expr,
                                    operator,
                                    right_expr
                                )
                            }
                        }
                    } else {
                        if is_check_list_empty {
                            format!("{} {} {}", left_expr, operator, right_expr)
                        } else {
                            // Same types
                            format!(
                                "if {} {{({}) {} ({})}} else {{ false }}",
                                check_list.join(" && "),
                                left_expr,
                                operator,
                                right_expr
                            )
                        }
                    }
                }
                GroupBaseCondition::NullCheck(null_check) => {
                    if null_check.field.column_ref.is_some(){
                    // Get the column reference that's being checked for null
                    let col_ref = if let Some(ref col) = null_check.field.column_ref {
                        col
                    } else {
                        panic!("NULL check must be on a column reference");
                    };

                    // Check if this column is part of the GROUP BY key
                    let is_key_field = keys.iter().any(|c| {
                        c.column == col_ref.column
                            && (c.table.is_none()
                                || col_ref.table.is_none()
                                || c.table == col_ref.table)
                    });

                    // Get column access based on whether it's a key field
                    let col_access = if is_key_field {
                        // Get the position in the group by key tuple
                        let key_position = keys
                            .iter()
                            .position(|c| {
                                c.column == col_ref.column
                                    && (c.table.is_none()
                                        || col_ref.table.is_none()
                                        || c.table == col_ref.table)
                            })
                            .unwrap();

                        if keys.len() == 1 {
                            // Single key column
                            "x.0".to_string()
                        } else {
                            // Multiple key columns - access by position
                            format!("x.0.{}", key_position)
                        }
                    } else {
                        // Not a key column - must be in the accumulated values or aggregates

                        let stream_name = if col_ref.table.is_some() {
                            query_object
                                .get_stream_from_alias(col_ref.table.as_ref().unwrap())
                                .unwrap()
                        } else {
                            if query_object.streams.len() == 1 {
                                query_object.streams.first().unwrap().0
                            } else {
                                panic!("Column reference must have a table reference")
                            }
                        };
                        let stream = query_object.get_stream(&stream_name);
                        stream.check_if_column_exists(&col_ref.column);

                        format!(
                            "x.1{}.{}",
                            stream.get_access().get_base_path(),
                            col_ref.column
                        )
                    };

                    // Generate the appropriate null check
                    match null_check.operator {
                        NullOp::IsNull => format!("{}.is_none()", col_access),
                        NullOp::IsNotNull => format!("{}.is_some()", col_access),
                    }
                }

                else if null_check.field.literal.is_some(){
                    let lit = null_check.field.literal.as_ref().unwrap();
                    match lit {
                        IrLiteral::Boolean(_) | IrLiteral::Integer(_) | IrLiteral::Float(_) => {
                            match null_check.operator {
                                NullOp::IsNull => format!("false"),
                                NullOp::IsNotNull => format!("true"),
                            }
                        }
                        IrLiteral::String(string) => {
                            match null_check.operator {
                                NullOp::IsNull => format!("{}", string.is_empty()),
                                NullOp::IsNotNull => format!("{}", !string.is_empty()),
                            }
                        }
                        IrLiteral::ColumnRef(_) => {
                            panic!("We should not be here.")
                        }
                    }
                }
                else if null_check.field.aggregate.is_some(){
                    match null_check.operator {
                        NullOp::IsNull => format!("false"),
                        NullOp::IsNotNull => format!("true"),
                    }
                }
                else {
                    panic!("Invalid NULL check - must be on a column reference or literal")
                }
            
            
            }
            }
        }
        GroupClause::Expression { left, op, right } => {
            let op_str = match op {
                BinaryOp::And => "&&",
                BinaryOp::Or => "||",
            };

            format!(
                "({} {} {})",
                process_filter_condition(left, keys, query_object, acc_info),
                op_str,
                process_filter_condition(right, keys, query_object, acc_info)
            )
        }
    }
}

// Helper function to process fields in filter conditions
fn process_filter_field(
    field: &ComplexField,
    keys: &Vec<ColumnRef>,
    query_object: &QueryObject,
    acc_info: &GroupAccumulatorInfo,
    mut check_list: &mut Vec<String>, // Added parameter
) -> String {
    if let Some(ref nested) = field.nested_expr {
        let (left, op, right) = &**nested;

        let left_type = query_object.get_complex_field_type(left);
        let right_type = query_object.get_complex_field_type(right);

        let left_expr = process_filter_field(left, keys, query_object, acc_info, &mut check_list);
        let right_expr = process_filter_field(right, keys, query_object, acc_info, &mut check_list);

        // Improved type handling for arithmetic operations
        if left_type != right_type {
            if (left_type == "f64" || left_type == "i64")
                && (right_type == "f64" || right_type == "i64")
            {
                // Division always results in f64
                if op == "/" {
                    return format!("({} as f64) {} ({} as f64)", left_expr, op, right_expr);
                }

                // Special handling for power operation (^)
                if op == "^" {
                    // If either operand is f64, use powf
                    if left_type == "f64" || right_type == "f64" {
                        return format!(
                            "({}).powf({} as f64)",
                            if left_type == "i64" {
                                format!("({} as f64)", left_expr)
                            } else {
                                left_expr
                            },
                            right_expr
                        );
                    } else {
                        // Both are integers, use pow
                        return format!("({}).pow({} as u32)", left_expr, right_expr);
                    }
                }

                // Add proper type conversion for other operations
                if left_type == "i64" && right_type == "f64" {
                    return format!("({} as f64) {} {}", left_expr, op, right_expr);
                } else if left_type == "f64" && right_type == "i64" {
                    return format!("{} {} ({} as f64)", left_expr, op, right_expr);
                }
            }
        } else {
            // Same types
            if op == "/" {
                return format!("({} as f64) {} ({} as f64)", left_expr, op, right_expr);
            } else if op == "^" {
                if left_type == "f64" {
                    return format!("({}).powf({})", left_expr, right_expr);
                } else {
                    return format!("({}).pow({} as u32)", left_expr, right_expr);
                }
            }
        }

        format!("({} {} {})", left_expr, op, right_expr)
    } else if let Some(ref col) = field.column_ref {
        //get type
        let as_ref = if query_object.get_type(col) == "String" {
            ".as_ref()"
        } else {
            ""
        };
        // Handle column reference - check if it's a key or not
        if let Some(key_position) = keys.iter().position(|c| c.column == col.column) {
            let col_type = query_object.get_type(col);
            // It's a key - use its position in the group by tuple
            if keys.len() == 1 {
                check_list.push(format!("x.0{}.is_some()", as_ref));
                format!(
                    "x.0{}.unwrap(){}",
                    as_ref,
                    if col_type == "f64" {
                        ".into_inner()"
                    } else {
                        ""
                    }
                )
            } else {
                check_list.push(format!("x.0.{}{}.is_some()", key_position, as_ref));
                format!(
                    "x.0.{}{}.unwrap(){}",
                    key_position,
                    as_ref,
                    if col_type == "f64" {
                        ".into_inner()"
                    } else {
                        ""
                    }
                )
            }
        } else {
            // Not a key - use x.1

            let stream_name = if col.table.is_some() {
                query_object
                    .get_stream_from_alias(col.table.as_ref().unwrap())
                    .unwrap()
            } else {
                if query_object.streams.len() == 1 {
                    query_object.streams.first().unwrap().0
                } else {
                    panic!("Column reference must have a table reference")
                }
            };

            let stream = query_object.get_stream(&stream_name);

            stream.check_if_column_exists(&col.column);

            check_list.push(format!(
                "x.1{}.{}{}.is_some()",
                stream.get_access().get_base_path(),
                col.column,
                as_ref
            ));

            format!(
                "x.1{}.{}{}.unwrap()",
                stream.get_access().get_base_path(),
                col.column,
                as_ref
            )
        }
    } else if let Some(ref lit) = field.literal {
        match lit {
            IrLiteral::Integer(i) => i.to_string(),
            IrLiteral::Float(f) => format!("{:.2}", f),
            IrLiteral::String(s) => format!("\"{}\"", s),
            IrLiteral::Boolean(b) => b.to_string(),
            IrLiteral::ColumnRef(_) => {
                panic!("ColumnRef should have been handled earlier")
            }
        }
    } else if let Some(ref agg) = field.aggregate {
        //retrive aggregate position from the accumulator
        let agg_pos = acc_info.get_agg_position(agg);
        // Aggregates are always in x.1
        let col = &agg.column;
        let col_access = if acc_info.agg_positions.len() == 1 {
            format!("x.1")
        } else {
            format!("x.1.{}", agg_pos)
        };

        if agg.function != AggregateType::Count {
            check_list.push(format!("{}.is_some()", col_access));
        }

        match agg.function {
            AggregateType::Count => {
                format!("{}", col_access)
            }
            AggregateType::Max | AggregateType::Min | AggregateType::Sum => {
                format!("{}.unwrap()", col_access)
            }
            AggregateType::Avg => {
                //get the sum and count positions. Sum position corresponds to the position of the aggregate in the accumulator
                let count_pos = acc_info.get_agg_position(&AggregateFunction {
                    function: AggregateType::Count,
                    column: col.clone(),
                });
                format!(
                    "{}.unwrap() / {} as f64",
                    col_access,
                    format!("x.1.{}", count_pos)
                )
            }
        }
    } else {
        panic!("Invalid ComplexField - no valid content")
    }
}
