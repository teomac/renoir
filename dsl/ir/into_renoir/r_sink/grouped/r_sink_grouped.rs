use core::panic;
use crate::dsl::ir::ir_ast_structure::{ComplexField, SelectColumn};
use crate::dsl::ir::r_group::r_group_keys::{GroupAccumulatorInfo, 
    GroupAccumulatorValue};
use crate::dsl::ir::{AggregateType, IrLiteral};
use crate::dsl::struct_object::object::QueryObject;

/// /// Processes projections in the context of a GROUP BY operation.
///
/// # Arguments
///
/// * `query_object` - A reference to the `QueryObject` which contains the query's abstract syntax tree (AST) and other metadata.
/// * `acc_info` - A reference to the `GroupAccumulatorInfo` which contains information about the positions of aggregate functions in the accumulator.
///
/// # Returns
///
/// A `String` representing the Rust code for the map operation that processes the projections.
///
/// # Panics
///
/// This function will panic if:
/// - A column in the projections is not part of the GROUP BY key.
/// - The GROUP BY clause is missing.
/// - An aggregate function is not found in the accumulator.
/// - An invalid arithmetic expression is encountered.
/// - An invalid `ComplexField` is encountered.
///
pub fn process_grouping_projections(
    query_object: &QueryObject,
    acc_info: &GroupAccumulatorInfo,
) -> String {
    let mut result = String::new();
    let is_single_agg: bool = acc_info.agg_positions.len() == 1;
    let mut check_list = Vec::new();

    // Start the map operation
    result.push_str(".map(|x| OutputStruct {\n");

    // Process each select clause
    for (i, clause) in query_object
        .ir_ast
        .as_ref()
        .unwrap()
        .select
        .select
        .iter()
        .enumerate()
    {
        check_list.clear();
        let field_name = query_object.result_column_types.get_index(i).unwrap().0;

        match clause {
            SelectColumn::Column(col_ref, _) => {
                //case select *, we call the create_select_star_group function
                if col_ref.column == "*" {
                    return create_select_star_group(query_object);
                }

                // For columns, check if they are part of the GROUP BY key
                if let Some(group_by) = &query_object.ir_ast.as_ref().unwrap().group_by {
                    let key_position = group_by
                        .columns
                        .iter()
                        .position(|c| c.column == col_ref.column && c.table == col_ref.table);

                    if let Some(pos) = key_position {
                        // Column is in the GROUP BY key
                        let value = if group_by.columns.len() == 1 {
                            format!("x.0.clone()")
                        } else {
                            format!("x.0.{}.clone()", pos)
                        };
                        result.push_str(&format!("    {}: {},\n", field_name, value));
                    } else {
                        // Column not in GROUP BY - this is an error
                        panic!("Column {} not in GROUP BY clause", col_ref.column);
                    }
                } else {
                    panic!("GROUP BY clause missing but process_grouping_projections was called");
                }
            }
            SelectColumn::Aggregate(agg, _) => {
                // For aggregates, access them from the accumulator
                let value = match agg.function {
                    AggregateType::Avg => {
                        // For AVG, we need both sum and count positions
                        let sum_key = GroupAccumulatorValue::Aggregate(
                            AggregateType::Sum,
                            agg.column.clone(),
                        );
                        let count_key = GroupAccumulatorValue::Aggregate(
                            AggregateType::Count,
                            agg.column.clone(),
                        );

                        let sum_pos = acc_info
                            .agg_positions
                            .get(&sum_key)
                            .expect("SUM for AVG not found in accumulator")
                            .0;
                        let count_pos = acc_info
                            .agg_positions
                            .get(&count_key)
                            .expect("COUNT for AVG not found in accumulator")
                            .0;

                        check_list.push(format!("x.1.{}.is_some()", sum_pos));

                        format!("if {} {{ Some(x.1.{}.unwrap() as f64 / x.1.{} as f64) }} else {{ None }}", check_list.join(" && "), sum_pos, count_pos)
                    }
                    AggregateType::Max | AggregateType::Min | AggregateType::Sum => {
                        let agg_key = GroupAccumulatorValue::Aggregate(
                            agg.function.clone(),
                            agg.column.clone(),
                        );
                        let original_type = query_object.get_type(&agg.column);

                        if let Some((pos, _)) = acc_info.agg_positions.get(&agg_key) {
                            check_list.push(format!("x.1.{}.is_some()", pos));

                            if original_type == "i64" {
                                // Cast back to i64 if that was the original type

                                format!(
                                    "if {} {{ Some((x.1{}.unwrap() as i64)) }} else {{ None }}",
                                    check_list.join(" && "),
                                    if !is_single_agg {
                                        format!(".{}", pos)
                                    } else {
                                        String::new()
                                    }
                                )
                            } else {
                                format!(
                                    "if {} {{ Some(x.1{}.unwrap())  }} else {{ None }}",
                                    check_list.join(" &&"),
                                    if !is_single_agg {
                                        format!(".{}", pos)
                                    } else {
                                        String::new()
                                    }
                                )
                            }
                        } else {
                            panic!("Aggregate {:?} not found in accumulator", agg);
                        }
                    }
                    AggregateType::Count => {
                        let agg_key = GroupAccumulatorValue::Aggregate(
                            agg.function.clone(),
                            agg.column.clone(),
                        );
                        if let Some((pos, _)) = acc_info.agg_positions.get(&agg_key) {
                            format!(
                                "Some(x.1{})",
                                if !is_single_agg {
                                    format!(".{}", pos)
                                } else {
                                    String::new()
                                }
                            )
                        } else {
                            panic!("Aggregate {:?} not found in accumulator", agg);
                        }
                    }
                };
                result.push_str(&format!("    {}: {},\n", field_name, value));
            }
            SelectColumn::ComplexValue(field, _) => {
                let temp = process_complex_field_for_group(
                    &field,
                    query_object,
                    acc_info,
                    &mut check_list,
                );

                let is_check_list_empty = check_list.is_empty();

                if !is_check_list_empty{
                // Deduplicate check list
                check_list.sort();
                check_list.dedup();}
                
                // For complex expressions, recursively process them
                let value;
                if is_check_list_empty {
                    value = format!("Some({})", temp);
                } else {
                    value = format!(
                        "if {} {{ Some({}) }} else {{ None }}",
                        check_list.join(" && "),
                        temp
                    );
                }

                result.push_str(&format!("    {}: {},\n", field_name, value));
            }
            SelectColumn::StringLiteral(value) => {
                result.push_str(&format!("    {}: Some(\"{}\".to_string()),\n", field_name, value));
            },
        }
    }

    // Close the map operation
    result.push_str("})");

    result
}




// Helper function to process complex fields in the context of GROUP BY
fn process_complex_field_for_group(
    field: &ComplexField,
    query_object: &QueryObject,
    acc_info: &GroupAccumulatorInfo,
    check_list: &mut Vec<String>,
) -> String {
    let is_single_agg: bool = acc_info.agg_positions.len() == 1;
    if let Some(ref nested) = field.nested_expr {
        // Handle nested expression (left_field OP right_field)
        let (left, op, right) = &**nested;

        let left_type = query_object.get_complex_field_type(left);
        let right_type = query_object.get_complex_field_type(right);

        // Different types case
        if left_type != right_type {
            if (left_type == "f64" || left_type == "i64" || left_type == "usize")
                && (right_type == "f64" || right_type == "i64" || right_type == "usize")
            {
                // Division always results in f64
                if op == "/" {
                    return format!(
                        "({} as f64) {} ({} as f64)",
                        process_complex_field_for_group(left, query_object, acc_info, check_list),
                        op,
                        process_complex_field_for_group(right, query_object, acc_info, check_list)
                    );
                }

                // Special handling for power operation (^)
                if op == "^" {
                    let left_expr =
                        process_complex_field_for_group(left, query_object, acc_info, check_list);
                    let right_expr =
                        process_complex_field_for_group(right, query_object, acc_info, check_list);

                    // If either operand is f64, use powf
                    if left_type == "f64" || right_type == "f64" {
                        return format!(
                            "({}).powf({} as f64)",
                            if left_type == "i64" || left_type == "usize" {
                                format!("({} as f64)", left_expr)
                            } else {
                                left_expr
                            },
                            right_expr
                        );
                    } else {
                        // Both are integers, use pow
                        return format!("({} as u32).pow({} as u32)", left_expr, right_expr);
                    }
                }

                let left_expr =
                    process_complex_field_for_group(left, query_object, acc_info, check_list);
                let right_expr =
                    process_complex_field_for_group(right, query_object, acc_info, check_list);

                // Add as f64 to integer literals when needed
                let processed_left = if let Some(ref lit) = left.literal {
                    if let IrLiteral::Integer(_) = lit {
                        format!("{} as f64", left_expr)
                    } else {
                        left_expr
                    }
                } else {
                    left_expr
                };

                let processed_right = if let Some(ref lit) = right.literal {
                    if let IrLiteral::Integer(_) = lit {
                        format!("{} as f64", right_expr)
                    } else {
                        right_expr
                    }
                } else {
                    right_expr
                };

                //for any operation, convert all to f64
                return format!("(({} as f64) {} ({} as f64))", processed_left, op, processed_right);
            } else {
                panic!(
                    "Invalid arithmetic expression - incompatible types: {} and {}",
                    left_type, right_type
                );
            }
        } else {
            //case same type
            //if operation is plus, minus, multiply, division, or power and types are not numeric, panic
            if op == "+" || op == "-" || op == "*" || op == "/" || op == "^" {
                if left_type != "f64" && left_type != "i64" && left_type != "usize" {
                    panic!(
                        "Invalid arithmetic expression - non-numeric types: {} and {}",
                        left_type, right_type
                    );
                }
            }

            // Division always results in f64
            if op == "/" {
                return format!(
                    "({} as f64) {} ({} as f64)",
                    process_complex_field_for_group(left, query_object, acc_info, check_list),
                    op,
                    process_complex_field_for_group(right, query_object, acc_info, check_list)
                );
            }

            // Special handling for power operation (^)
            if op == "^" {
                let left_expr =
                    process_complex_field_for_group(left, query_object, acc_info, check_list);
                let right_expr =
                    process_complex_field_for_group(right, query_object, acc_info, check_list);

                // If both are f64, use powf
                if left_type == "f64" {
                    return format!("({}).powf({})", left_expr, right_expr);
                } else {
                    // Both are integers, use pow
                    return format!("({} as u32).pow({} as u32)", left_expr, right_expr);
                }
            }

            // Regular arithmetic with same types
            format!(
                "({} {} {})",
                process_complex_field_for_group(left, query_object, acc_info, check_list),
                op,
                process_complex_field_for_group(right, query_object, acc_info, check_list)
            )
        }
    } else if let Some(ref col) = field.column_ref {
        // Handle column reference - must be in GROUP BY key
        if let Some(group_by) = &query_object.ir_ast.as_ref().unwrap().group_by {
            let key_position = group_by
                .columns
                .iter()
                .position(|c| c.column == col.column && c.table == col.table);

            check_list.push(format!(
                "x.0.{}.is_some()",
                key_position.expect("Column not in GROUP BY key")
            ));

            if let Some(pos) = key_position {
                // Column is in the GROUP BY key
                if group_by.columns.len() == 1 {
                    format!("x.0.unwrap()")
                } else {
                    format!("x.0.{}.unwrap()", pos)
                }
            } else {
                panic!("Column {} not in GROUP BY clause", col.column);
            }
        } else {
            panic!("GROUP BY clause missing but process_complex_field_for_group was called");
        }
    } else if let Some(ref lit) = field.literal {
        // Handle literal values
        match lit {
            IrLiteral::Integer(i) => i.to_string(),
            IrLiteral::Float(f) => format!("{:.2}", f),
            IrLiteral::String(s) => format!("\"{}\"", s),
            IrLiteral::Boolean(b) => b.to_string(),
            _ => {
                panic!("Invalid ComplexField - no valid content");
            }
        }
    } else if let Some(ref agg) = field.aggregate {
        // Handle aggregate functions
        match agg.function {
            AggregateType::Avg => {
                // For AVG, we need both sum and count positions
                let sum_key =
                    GroupAccumulatorValue::Aggregate(AggregateType::Sum, agg.column.clone());
                let count_key =
                    GroupAccumulatorValue::Aggregate(AggregateType::Count, agg.column.clone());

                let sum_pos = acc_info
                    .agg_positions
                    .get(&sum_key)
                    .expect("SUM for AVG not found in accumulator")
                    .0;
                let count_pos = acc_info
                    .agg_positions
                    .get(&count_key)
                    .expect("COUNT for AVG not found in accumulator")
                    .0;

                check_list.push(format!("x.1{}.is_some()", sum_pos));
                // Only compute average if sum is Some
                format!("(x.1{}.unwrap() as f64) / x.1{} as f64",
                    format!(".{}", sum_pos),
                    format!(".{}", count_pos)
                )
            }
            AggregateType::Max | AggregateType::Min | AggregateType::Sum => {
                let agg_key =
                    GroupAccumulatorValue::Aggregate(agg.function.clone(), agg.column.clone());

                if let Some((pos, _)) = acc_info.agg_positions.get(&agg_key) {
                    check_list.push(format!("x.1.{}.is_some()", pos));
                    // These are already Option types, so we return them directly
                    format!(
                        "x.1{}.unwrap()",
                        if !is_single_agg {
                            format!(".{}", pos)
                        } else {
                            String::new()
                        }
                    )
                } else {
                    panic!("Aggregate {:?} not found in accumulator", agg);
                }
            }
            AggregateType::Count => {
                let agg_key =
                    GroupAccumulatorValue::Aggregate(agg.function.clone(), agg.column.clone());
                if let Some((pos, _)) = acc_info.agg_positions.get(&agg_key) {
                    format!(
                        "x.1{}",
                        if !is_single_agg {
                            format!(".{}", pos)
                        } else {
                            "".to_string()
                        }
                    )
                } else {
                    panic!("Aggregate {:?} not found in accumulator", agg);
                }
            }
        }
    } else {
        panic!("Invalid ComplexField - no valid content");
    }
}


//Function to handle select * case
fn create_select_star_group(query_object: &QueryObject) -> String {
    let mut result = String::new();
    let group_by = query_object
        .ir_ast
        .as_ref()
        .unwrap()
        .group_by
        .as_ref()
        .unwrap();

    if !group_by.columns.is_empty() {
        // Handle different cases based on number of group keys
        if group_by.columns.len() == 1 {
            result.push_str(".map(|x| OutputStruct { ");

            // For single column, x.0 directly contains the key value
            let key_col = &group_by.columns[0];

            // Find the corresponding result column name
            for (key, _) in &query_object.result_column_types {
                if key.contains(&key_col.column) {
                    result.push_str(&format!("{}: x.0.clone()", key));
                    break;
                }
            }

            result.push_str(" })");
        } else {
            // For multiple columns, x.0 is a tuple of key values
            result.push_str(".map(|x| OutputStruct { ");

            let mut field_assignments = Vec::new();

            // Process each key column and find matching result column names
            for (i, key_col) in group_by.columns.iter().enumerate() {
                for (key, _) in &query_object.result_column_types {
                    if key.contains(&key_col.column) {
                        field_assignments.push(format!("{}: x.0.{}.clone()", key, i));
                        break;
                    }
                }
            }

            result.push_str(&field_assignments.join(", "));
            result.push_str(" })");
        }
    } else {
        // Fallback for empty group by (should not happen, but just in case)
        result.push_str(".map(|_| OutputStruct { })");
    }

    result
}
