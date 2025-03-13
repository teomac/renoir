use core::panic;
use crate::dsl::ir::ir_ast_structure::{ComplexField, ProjectionColumn};
use crate::dsl::ir::{AggregateType, IrLiteral};
use crate::dsl::struct_object::object::QueryObject;
use crate::dsl::ir::r_sink::r_sink_utils::{AccumulatorInfo, AccumulatorValue};

// function to create aggregate fold and map
pub fn create_aggregate_map(projection_clauses: &Vec<ProjectionColumn>, stream_name: &String, query_object: &QueryObject) -> String {
    let mut acc_info = AccumulatorInfo::new();
    let mut result = String::new();

    let stream = query_object.get_stream(stream_name);
    let is_grouped = stream.is_keyed;
    let keys = stream.key_columns.clone();

    let mut check_list = Vec::new();

    //SELECT MAX(power), MIN(power)/MAX(POWER), name, total_km ^2 FROM table1 GROUP BY name, total_km

    // First analyze all clauses to build accumulator info
    for (i, clause) in projection_clauses.iter().enumerate() {
        let result_type = query_object.result_column_types.get_index(i).unwrap().1;
        match clause {
            ProjectionColumn::Aggregate(agg, _) => match agg.function {
                AggregateType::Avg => {
                    acc_info.add_avg(agg.column.clone(), result_type.clone());
                }
                AggregateType::Count => {
                    acc_info.add_value(
                        AccumulatorValue::Aggregate(agg.function.clone(), agg.column.clone()),
                        "usize".to_string(),
                    );
                }
                _ => {
                    acc_info.add_value(
                        AccumulatorValue::Aggregate(agg.function.clone(), agg.column.clone()),
                        result_type.clone(),
                    );
                }
            },
            ProjectionColumn::ComplexValue(field, _) => {
                process_complex_field_for_accumulator(
                    field,
                    stream_name,
                    &mut acc_info,
                    query_object,
                    &mut check_list,
                );
            }
            ProjectionColumn::Column(col, _) => {
                //check if the stream is grouped and if the column is a key column
                if is_grouped && !keys.contains(col) {
                    panic!("Cannot use key column in projection clause in grouped query");
                }
                //no need to add the column because it is already a key in the keyed stream
            }
            ProjectionColumn::StringLiteral(value) => {
                // String literals are constant, no update needed
            }
        }
    }

    // Initialize the fold accumulator with correct types and initial values
    let mut tuple_types = Vec::new();
    let mut tuple_inits = Vec::new();

    // Set appropriate initial values based on type and aggregation
    for (value, (_pos, val_type)) in acc_info.value_positions.iter() {
        match value {
            AccumulatorValue::Aggregate(agg_type, _) => {
                println!("val_type: {}", val_type);
                match agg_type {
                    AggregateType::Max | AggregateType::Min | AggregateType::Sum => {
                        // Initialize these as None
                        tuple_inits.push("None".to_string());
                        // Update the type to be Option<type>
                        tuple_types.push(format!("Option<{}>", val_type));
                    }
                    AggregateType::Count => {
                        // Count stays the same - initialized to 0
                        tuple_inits.push("0".to_string());
                        tuple_types.push("usize".to_string());
                    }
                    AggregateType::Avg => {
                        // Avg is handled through Sum and Count
                        tuple_inits.push("None".to_string());
                        tuple_types.push("Option<f64>".to_string());
                    }
                }
            }
            AccumulatorValue::Column(_) => {
                // No need to add columns to the accumulator
            }
            AccumulatorValue::Literal(string_val) => {
                // String literals are constant, no update needed
            }
        }
    }

    let tuple_type = format!("({})", tuple_types.join(", "));
    let tuple_init = format!("({})", tuple_inits.join(", "));

    // Start fold operation
    result.push_str(&format!(
        ".fold({}, |acc: &mut {}, x| {{\n",
        tuple_init, tuple_type
    ));

    // Generate fold accumulator updates
    let mut update_code = String::new();

    let is_single_acc = acc_info.value_positions.len() == 1;
    let mut asterisk: String = String::new();

    for (value, (pos, _)) in acc_info.value_positions.iter() {
        let mut index_acc = format!(".{}", pos);

        if is_single_acc {
            index_acc = String::new();
            asterisk = "*".to_string();
        }
        match value {
            AccumulatorValue::Aggregate(agg_type, col) => {
                let col_stream_name = if col.table.is_some(){
                    query_object.get_stream_from_alias(col.table.as_ref().unwrap()).unwrap()
                }
                else{
                    stream_name
                };

                let col_stream = query_object.get_stream(col_stream_name);

                col_stream.check_if_column_exists(&col.column);

                //check if is grouped and the column is a key column
                let mut is_key_col = false;
                if is_grouped {
                    is_key_col = keys.contains(&col);
                }

                let col_access = {
                    format!(
                        "x{}{}.{}",
                        if is_grouped {
                            if is_key_col {
                                ".0"
                            } else {
                                ".1"
                            }
                        }
                        else{
                            ""
                        },
                        col_stream.get_access().get_base_path(),
                        col.column
                    )
                };

                match agg_type {
                    AggregateType::Count => {
                        if col.column == "*" {
                            update_code.push_str(&format!("    acc{} += 1;\n", index_acc));
                        } else {
                            update_code.push_str(&format!(
                                "    if {}.is_some() {{ {}acc{} += 1; }}\n",
                                col_access, asterisk, index_acc
                            ));
                        }
                    }
                    AggregateType::Sum => {
                        update_code.push_str(&format!(
                            "    if let Some(val) = {} {{ 
                                {}acc{} = Some(acc{}.unwrap_or(0.0) + val);
                            }}\n",
                            col_access, asterisk, index_acc, index_acc
                        ));
                    }
                    AggregateType::Max => {
                        update_code.push_str(&format!(
                            "    if let Some(val) = {} {{
                                {}acc{} = Some(match acc{} {{
                                    Some(current_max) => current_max.max(val),
                                    None => val
                                }});
                            }}\n",
                            col_access, asterisk, index_acc, index_acc
                        ));
                    }
                    AggregateType::Min => {
                        update_code.push_str(&format!(
                            "    if let Some(val) = {} {{
                                {}acc{} = Some(match acc{} {{
                                    Some(current_min) => current_min.min(val),
                                    None => val
                                }});
                            }}\n",
                            col_access, asterisk, index_acc, index_acc
                        ));
                    }
                    AggregateType::Avg => {} // Handled through Sum and Count
                }
            }
            AccumulatorValue::Column(col) => {
                //check if the column is a key column
                if !is_grouped{
                    panic!("Cannot use column in projection clause in non-grouped query");
                }
                else {
                    if !keys.contains(col) {
                        panic!("Cannot use column in projection clause in grouped query that is not a key column");
                    }
                }
            }
            AccumulatorValue::Literal(_) => {
                // String literals are constant, no update needed
            }
        }
    }

    result.push_str(&update_code);
    result.push_str("})\n");

    // Generate final map to OutputStruct
    result.push_str(".map(|x| OutputStruct {\n");

    for (i, clause) in projection_clauses.iter().enumerate() {
        check_list.clear();
        let field_name = query_object.result_column_types.get_index(i).unwrap().0;
        let value = match clause {
            ProjectionColumn::Aggregate(agg, _) => {
                match agg.function {
                    AggregateType::Avg => {
                        let (sum_pos, count_pos) = (
                            acc_info
                                .value_positions
                                .get(&AccumulatorValue::Aggregate(
                                    AggregateType::Sum,
                                    agg.column.clone(),
                                ))
                                .unwrap()
                                .0,
                            acc_info
                                .value_positions
                                .get(&AccumulatorValue::Aggregate(
                                    AggregateType::Count,
                                    agg.column.clone(),
                                ))
                                .unwrap()
                                .0,
                        );
                        // Only compute average if sum is Some
                        format!("if let Some(sum) = x.{} {{ Some(sum as f64 / x.{} as f64) }} else {{ None }}", 
                            sum_pos, count_pos)
                    }
                    AggregateType::Max | AggregateType::Min | AggregateType::Sum => {
                        let pos = acc_info
                            .value_positions
                            .get(&AccumulatorValue::Aggregate(
                                agg.function.clone(),
                                agg.column.clone(),
                            ))
                            .unwrap()
                            .0;
                        // These are already Option types, so we just return them directly
                        format!(
                            "acc{}",
                            if !is_single_acc {
                                format!(".{}", pos)
                            } else {
                                String::new()
                            }
                        )
                    }
                    _ => {
                        let pos = acc_info
                            .value_positions
                            .get(&AccumulatorValue::Aggregate(
                                agg.function.clone(),
                                agg.column.clone(),
                            ))
                            .unwrap()
                            .0;
                        //if there is only one acc, do not use .0
                        if is_single_acc {
                            format!("Some(x)")
                        } else {
                            format!("Some(x.{})", pos)
                        }
                    }
                }
            }
            ProjectionColumn::ComplexValue(field, _) => {
                let temp = process_complex_field_for_accumulator(
                    field,
                    stream_name,
                    &mut acc_info,
                    query_object,
                    &mut check_list,
                );
                check_list.sort();
                check_list.dedup();
                let is_check_list_empty = check_list.is_empty();
                if is_check_list_empty {
                    format!("Some({})", temp)
                } else {
                    format!(
                        "if {} {{ Some({}) }} else {{ None }}",
                        check_list.join(" && "),
                        temp
                    )
                }
            }
            ProjectionColumn::Column(col, _) => {
                let pos = acc_info
                    .value_positions
                    .get(&AccumulatorValue::Column(col.clone()))
                    .unwrap()
                    .0;
                format!("Some(x.{})", pos)
            }
            ProjectionColumn::StringLiteral(value) => {
                format!("Some(\"{}\".to_string())", value)
            }
        };
        result.push_str(&format!("    {}: {},\n", field_name, value));
    }

    result.push_str("})");
    result
}

fn process_complex_field_for_accumulator(
    field: &ComplexField,
    stream_name: &String,
    acc_info: &mut AccumulatorInfo,
    query_object: &QueryObject,
    check_list: &mut Vec<String>,
) -> String {
    let stream = query_object.get_stream(stream_name);
    let is_grouped = stream.is_keyed;
    let keys = stream.key_columns.clone();

    let is_single_acc = acc_info.value_positions.len() == 1;

    if let Some(ref nested) = field.nested_expr {
        // Handle nested expression (left_field OP right_field)
        let (left, op, right) = &**nested;

        let left_type = query_object.get_complex_field_type(left);
        let right_type = query_object.get_complex_field_type(right);

        // Different types case
        if left_type != right_type {
            if (left_type == "f64" || left_type == "i64")
                && (right_type == "f64" || right_type == "i64")
            {
                // Division always results in f64
                if op == "/" {
                    return format!(
                        "({} as f64) {} ({} as f64)",
                        process_complex_field_for_accumulator(
                            left,
                            stream_name,
                            acc_info,
                            query_object,
                            check_list
                        ),
                        op,
                        process_complex_field_for_accumulator(
                            right,
                            stream_name,
                            acc_info,
                            query_object,
                            check_list
                        )
                    );
                }

                // Special handling for power operation (^)
                if op == "^" {
                    let left_expr = process_complex_field_for_accumulator(
                        left,
                        stream_name,
                        acc_info,
                        query_object,
                        check_list,
                    );
                    let right_expr = process_complex_field_for_accumulator(
                        right,
                        stream_name,
                        acc_info,
                        query_object,
                        check_list,
                    );

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

                let left_expr =
                    process_complex_field_for_accumulator(left, stream_name,acc_info, query_object, check_list);
                let right_expr = process_complex_field_for_accumulator(
                    right,
                    stream_name,
                    acc_info,
                    query_object,
                    check_list,
                );

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

                //if left is i64 and right is float or vice versa, convert the i64 to f64
                if left_type == "i64" && right_type == "f64" {
                    return format!("({} as f64 {} {})", processed_left, op, processed_right);
                } else if left_type == "f64" && right_type == "i64" {
                    return format!("({} {} {} as f64)", processed_left, op, processed_right);
                }

                return format!("({} {} {})", processed_left, op, processed_right);
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
                    process_complex_field_for_accumulator(left, stream_name, acc_info, query_object, check_list),
                    op,
                    process_complex_field_for_accumulator(
                        right,
                        stream_name,
                        acc_info,
                        query_object,
                        check_list
                    )
                );
            }

            // Special handling for power operation (^)
            if op == "^" {
                let left_expr =
                    process_complex_field_for_accumulator(left, stream_name, acc_info, query_object, check_list);
                let right_expr = process_complex_field_for_accumulator(
                    right,
                    stream_name,
                    acc_info,
                    query_object,
                    check_list,
                );

                // If both are f64, use powf
                if left_type == "f64" {
                    return format!("({}).powf({})", left_expr, right_expr);
                } else {
                    // Both are integers, use pow
                    return format!("({}).pow({} as u32)", left_expr, right_expr);
                }
            }

            // Regular arithmetic with same types
            format!(
                "({} {} {})",
                process_complex_field_for_accumulator(left, stream_name, acc_info, query_object, check_list),
                op,
                process_complex_field_for_accumulator(right, stream_name, acc_info, query_object, check_list)
            )
        }
    } else if let Some(ref col) = field.column_ref {
        //check if the stream is grouped and if the column is a key column
        if is_grouped && !keys.contains(col) {
            panic!("Cannot use key column in projection clause in grouped query");
        }

        // Handle regular column reference
        let pos = acc_info.add_value(
            AccumulatorValue::Column(col.clone()),
            query_object.get_type(col),
        );
        format!("acc.{}", pos)
    } else if let Some(ref lit) = field.literal {
        // Handle literal values
        match lit {
            IrLiteral::Integer(i) => i.to_string(),
            IrLiteral::Float(f) => format!("{:.2}", f),
            IrLiteral::String(s) => format!("\"{}\"", s),
            IrLiteral::Boolean(b) => b.to_string(),
            IrLiteral::ColumnRef(col) => {
                let pos = acc_info.add_value(
                    AccumulatorValue::Column(col.clone()),
                    query_object.get_type(col),
                );
                format!("acc.{}", pos)
            }
        }
    } else if let Some(ref agg) = field.aggregate {
        // Handle aggregate functions
        match agg.function {
            AggregateType::Avg => {
                let (sum_pos, count_pos) =
                    acc_info.add_avg(agg.column.clone(), query_object.get_type(&agg.column));
                check_list.push(format!("acc.{}.is_some()", sum_pos));
                format!(
                    "(acc.{}.unwrap() as f64 / acc.{} as f64)",
                    sum_pos, count_pos
                )
            }
            AggregateType::Max | AggregateType::Min | AggregateType::Sum => {
                let pos = acc_info.add_value(
                    AccumulatorValue::Aggregate(agg.function.clone(), agg.column.clone()),
                    query_object.get_type(&agg.column),
                );
                if is_single_acc {
                    check_list.push(format!("acc.is_some()"));
                    format!("acc.unwrap()")
                } else {
                    check_list.push(format!("acc.{}.is_some()", pos));
                    format!("acc.{}.unwrap()", pos)
                }
            }
            AggregateType::Count => {
                let pos = acc_info.add_value(
                    AccumulatorValue::Aggregate(agg.function.clone(), agg.column.clone()),
                    "usize".to_string(),
                );
                if is_single_acc {
                    format!("acc")
                } else {
                    format!("acc.{}", pos)
                }
            }
        }
    } else {
        panic!("Invalid ComplexField - no valid content");
    }
}


