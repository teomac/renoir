use crate::dsl::ir::ir_ast_structure::{ComplexField, ProjectionColumn};
use crate::dsl::ir::r_sink::r_sink_utils::{AccumulatorInfo, AccumulatorValue};
use crate::dsl::ir::{AggregateType, ColumnRef, IrLiteral};
use crate::dsl::struct_object::object::QueryObject;
///
/// initial function
///
/// function that performs the .fold() operation
/// function to parse complex expr for the .fold()
///
/// function that performs the .map() operation
/// function to parse complex expr for the .map()
///
use core::panic;

//initial function
pub(crate) fn create_aggregate_map(
    projection_clauses: &[ProjectionColumn],
    stream_name: &String,
    final_struct_name: &String,
    query_object: &QueryObject,
) -> String {
    let mut acc_info = AccumulatorInfo::new();
    let mut result = String::new();

    let stream = query_object.get_stream(stream_name);
    let mut all_streams = Vec::new();
    if stream.join_tree.is_some() {
        all_streams.extend(stream.join_tree.as_ref().unwrap().get_involved_streams());
    } else {
        all_streams.push(stream_name.clone());
    }
    let is_grouped = stream.is_keyed;
    let mut keys = Vec::new();
    for stream in all_streams.iter() {
        keys.extend(query_object.get_stream(stream).key_columns.clone());
    }
    let col_keys = keys.iter()
        .map(|key| key.0.clone())
        .collect::<Vec<ColumnRef>>();

    // First analyze all clauses to build accumulator info
    for (i, clause) in projection_clauses.iter().enumerate() {
        let result_type = query_object.result_column_types.get_index(i).unwrap().1;
        match clause {
            ProjectionColumn::Aggregate(agg, _) => match agg.function {
                AggregateType::Avg => {
                    let col_type = query_object.get_type(&agg.column);

                    acc_info.add_avg(agg.column.clone(), col_type);
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
                collect_aggregates_from_complex_field(field, &mut acc_info, query_object);
            }
            ProjectionColumn::Column(col, _) => {
                //check if the stream is grouped and if the column is a key column
                if is_grouped && !col_keys.contains(col) {
                    panic!("Cannot use key column in projection clause in grouped query");
                }
                //no need to add the column because it is already a key in the keyed stream
            }
            ProjectionColumn::StringLiteral(_, _) => {
                // String literals are constant, no update needed
            }
            _ => panic!("Invalid projection clause"),
        }
    }

    //call function to perform the .fold()
    result.push_str(&create_fold(&mut acc_info, stream_name, query_object));

    //call function to perform the .map()
    result.push_str(&create_map(
        projection_clauses,
        &acc_info,
        stream_name,
        final_struct_name,
        query_object,
    ));

    result
}

// Helper function to collect aggregates from complex fields
fn collect_aggregates_from_complex_field(
    field: &ComplexField,
    acc_info: &mut AccumulatorInfo,
    query_object: &QueryObject,
) {
    if let Some(ref agg) = field.aggregate {
        // Found an aggregate, add it
        match agg.function {
            AggregateType::Avg => {
                acc_info.add_avg(agg.column.clone(), query_object.get_type(&agg.column));
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
                    query_object.get_type(&agg.column),
                );
            }
        }
    }

    // Check nested expressions recursively
    if let Some(ref nested) = field.nested_expr {
        let (left, _, right, _) = &**nested;
        collect_aggregates_from_complex_field(left, acc_info, query_object);
        collect_aggregates_from_complex_field(right, acc_info, query_object);
    }
}

//function used to create the .fold() operation
fn create_fold(
    acc_info: &mut AccumulatorInfo,
    stream_name: &String,
    query_object: &QueryObject,
) -> String {
    let mut result = String::new();
    let stream = query_object.get_stream(stream_name);
    let mut keys = Vec::new();
    let mut all_streams = Vec::new();
    if stream.join_tree.is_some() {
        all_streams.extend(stream.join_tree.as_ref().unwrap().get_involved_streams());
    } else {
        all_streams.push(stream_name.clone());
    }
    for stream in all_streams.iter() {
        keys.extend(query_object.get_stream(stream).key_columns.clone());
    }

    // Initialize the fold accumulator with correct types and initial values
    let mut tuple_types = Vec::new();
    let mut tuple_inits = Vec::new();

    // Set appropriate initial values based on type and aggregation
    for (value, (_pos, val_type)) in acc_info.value_positions.iter() {
        match value {
            AccumulatorValue::Aggregate(agg_type, _) => {
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
                let col_type = query_object.get_type(col);
                let col_stream_name = if col.table.is_some() {
                    query_object
                        .get_stream_from_alias(col.table.as_ref().unwrap())
                        .unwrap()
                } else {
                    stream_name
                };

                let col_stream = query_object.get_stream(col_stream_name);

                if col.column != "*" {
                    col_stream.check_if_column_exists(&col.column);
                }

                let col_access = {
                    format!(
                        "x{}.{}",
                        col_stream.get_access().get_base_path(),
                        col.column
                    )
                };

                match agg_type {
                    AggregateType::Count => {
                        if col.column == "*" {
                            update_code
                                .push_str(&format!("    {}acc{} += 1;\n", asterisk, index_acc));
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
                                {}acc{} = Some(acc{}.unwrap_or(0{}) + val);
                            }}\n",
                            col_access,
                            asterisk,
                            index_acc,
                            index_acc,
                            if col_type == "f64" { ".0" } else { "" }
                        ));
                    }
                    AggregateType::Max => {
                        update_code.push_str(&format!(
                            "    if let Some({}val) = {} {{
                                {}acc{} = Some(match acc{} {{
                                    Some(current_max) => {}current_max.max({}val),
                                    None => val
                                }});
                            }}\n",
                            if asterisk.is_empty() || col_type != "i64" {
                                ""
                            } else {
                                "mut "
                            },
                            col_access,
                            asterisk,
                            index_acc,
                            index_acc,
                            if col_type == "i64" {
                                asterisk.clone()
                            } else {
                                "".to_string()
                            },
                            if asterisk.is_empty() || col_type != "i64" {
                                ""
                            } else {
                                "&mut "
                            }
                        ));
                    }
                    AggregateType::Min => {
                        update_code.push_str(&format!(
                            "    if let Some({}val) = {} {{
                                {}acc{} = Some(match acc{} {{
                                    Some(current_min) => {}current_min.min({}val),
                                    None => val
                                }});
                            }}\n",
                            if asterisk.is_empty() || col_type != "i64" {
                                ""
                            } else {
                                "mut "
                            },
                            col_access,
                            asterisk,
                            index_acc,
                            index_acc,
                            if col_type == "i64" {
                                asterisk.clone()
                            } else {
                                "".to_string()
                            },
                            if asterisk.is_empty() || col_type != "i64" {
                                ""
                            } else {
                                "&mut "
                            }
                        ));
                    }
                    AggregateType::Avg => {} // Handled through Sum and Count
                }
            }
        }
    }

    result.push_str(&update_code);
    result.push_str("})\n");

    result
}

//function used to create the .map() operation
pub(crate) fn create_map(
    projection_clauses: &[ProjectionColumn],
    acc_info: &AccumulatorInfo,
    stream_name: &String,
    struct_name: &String,
    query_object: &QueryObject,
) -> String {
    let mut result = String::new();
    let stream = query_object.get_stream(stream_name);
    let is_grouped = stream.is_keyed;
    let mut all_streams = Vec::new();
    let mut keys = Vec::new();
    if stream.join_tree.is_some() {
        all_streams.extend(stream.join_tree.as_ref().unwrap().get_involved_streams());
    } else {
        all_streams.push(stream_name.clone());
    }

    for stream in all_streams.iter() {
        keys.extend(query_object.get_stream(stream).key_columns.clone());
    }
    let col_keys = keys.iter()
        .map(|key| key.0.clone())
        .collect::<Vec<ColumnRef>>();

    let mut check_list = Vec::new();

    result.push_str(&format!(
        ".map(move |x| {} {{\n",
        struct_name
    ));

    let is_single_acc = acc_info.value_positions.len() == 1;

    for (i, clause) in projection_clauses.iter().enumerate() {
        let field_name = query_object.result_column_types.get_index(i).unwrap().0;
        let field_type = query_object.result_column_types.get_index(i).unwrap().1;
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
                        format!("if let Some(sum) = x{}.{} {{ Some(sum as f64 / x{}.{} as f64) }} else {{ None }}", 
                            if is_grouped { ".1" } else { "" },
                            sum_pos,
                            if is_grouped { ".1" } else { "" },
                            count_pos)
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
                            "x{}{}",
                            if is_grouped { ".1" } else { "" },
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
                            format!("Some(x{})", if is_grouped { ".1" } else { "" })
                        } else {
                            format!("Some(x{}.{})", if is_grouped { ".1" } else { "" }, pos)
                        }
                    }
                }
            }
            ProjectionColumn::ComplexValue(field, _) => {
                let mut cast = String::new();
                let temp = process_complex_field_for_map(
                    field,
                    stream_name,
                    acc_info,
                    query_object,
                    &mut check_list,
                    &mut cast,
                );
                check_list.sort();
                check_list.dedup();
                if check_list.is_empty() {
                    format!("Some(({}) as {})", temp, field_type)
                } else {
                    format!(
                        "if {} {{ Some(({}) as {}) }} else {{ None }}",
                        check_list.join(" && "),
                        temp,
                        field_type
                    )
                }
            }
            ProjectionColumn::Column(col, _) => {
                let col_stream_name = if col.table.is_some() {
                    query_object
                        .get_stream_from_alias(col.table.as_ref().unwrap())
                        .unwrap()
                } else {
                    stream_name
                };
                //we need to check if the stream is grouped and if the column is a key column
                if is_grouped {
                    if !col_keys.contains(col) {
                        panic!("Cannot use key column in projection clause in grouped query");
                    }
                } else {
                    //no grouping, there cannot be a column in the final projection that is not a key column. Because we have aggregates
                    panic!("Cannot use column in projection clause in non-grouped query");
                };

                let col_type = query_object.get_type(col);

                let col_stream = query_object.get_stream(col_stream_name);
                col_stream.check_if_column_exists(&col.column);
                let key_position = keys.iter().find(|key| key.0.column == col.column).map_or_else(
                    || panic!("Key column {} not found in keys", col.column),
                    |key| key.1.to_string(),
                );
                let is_single_key = keys.len() == 1;
                if is_single_key {
                    if col_type == "f64" {
                        "if x.0.is_some() { Some(x.0.unwrap().into_inner() as f64) } else { None }".to_string()
                    } else {
                        "x.0.clone()".to_string()
                    }
                } else if col_type == "f64" {
                    format!(
                        "if x.0.{}.is_some() {{ Some(x.0.{}.unwrap().into_inner() as f64) }} else {{ None }}",
                        key_position, key_position
                    )
                } else {
                    format!(
                        "x.0.{}{}",
                        key_position,
                        if col_type == "String" { ".clone()" } else { "" }
                    )
                }
            }
            ProjectionColumn::StringLiteral(value, _) => {
                format!("Some(\"{}\".to_string())", value)
            }
            ProjectionColumn::SubqueryVec(result, _) => {
                format!(
                    "Some({}.first().unwrap().unwrap().to_string().clone())",
                    result
                )
            }
            _ => panic!("Invalid projection clause"),
        };
        check_list.clear();
        result.push_str(&format!("    {}: {},\n", field_name, value));
    }

    result.push_str("})");

    result
}

fn process_complex_field_for_map(
    field: &ComplexField,
    stream_name: &String,
    acc_info: &AccumulatorInfo,
    query_object: &QueryObject,
    check_list: &mut Vec<String>,
    cast: &mut String,
) -> String {
    let stream = query_object.get_stream(stream_name);
    let mut all_streams = Vec::new();
    let mut keys = Vec::new();
    let is_keyed = stream.is_keyed;
    if stream.join_tree.is_some() {
        all_streams.extend(stream.join_tree.as_ref().unwrap().get_involved_streams());
    } else {
        all_streams.push(stream_name.clone());
    }

    for stream in all_streams.iter() {
        keys.extend(query_object.get_stream(stream).key_columns.clone());
    }

    if let Some(ref nested) = field.nested_expr {
        // Handle nested expression (left_field OP right_field)
        let (left, op, right, is_par) = &**nested;

        let left_type = query_object.get_complex_field_type(left);
        let right_type = query_object.get_complex_field_type(right);

        // Different types case
        if left_type != right_type {
            if left_type != "f64"
                && left_type != "i64"
                && left_type != "usize"
                && right_type != "f64"
                && right_type != "i64"
                && right_type != "usize"
            {
                panic!(
                    "Invalid arithmetic expression - non-numeric types: {} and {}",
                    left_type, right_type
                );
            } else {
                //they are both numbers
                if left_type == "f64" || right_type == "f64" {
                    *cast = "f64".to_string();
                }
                else if left_type == "i64" || right_type == "i64" {
                    *cast = "i64".to_string();
                }
            }

            let left_expr = process_complex_field_for_map(
                left,
                stream_name,
                acc_info,
                query_object,
                check_list,
                cast,
            );
            let right_expr = process_complex_field_for_map(
                right,
                stream_name,
                acc_info,
                query_object,
                check_list,
                cast,
            );

            // Special handling for power operation (^)
            if op == "^" {
                if left_type == "f64" || right_type == "f64" || cast == "f64" {
                    return format!("({}).powf({})", left_expr, right_expr);
                } else {
                    return format!("({}).pow({})", left_expr, right_expr);
                }
            }

            format!(
                "{}{} {} {}{}",
                if *is_par { "(" } else { "" },
                left_expr,
                op,
                right_expr,
                if *is_par { ")" } else { "" }
            )
        } else {
            // Same type case
            if (op == "+" || op == "-" || op == "*" || op == "/" || op == "^")
                && left_type != "f64"
                && left_type != "i64"
                && left_type != "usize"
                && right_type != "f64"
                && right_type != "i64"
                && right_type != "usize"
            {
                panic!(
                    "Invalid arithmetic expression - non-numeric types: {} and {}",
                    left_type, right_type
                );
            }

            let left_expr = process_complex_field_for_map(
                left,
                stream_name,
                acc_info,
                query_object,
                check_list,
                cast,
            );
            let right_expr = process_complex_field_for_map(
                right,
                stream_name,
                acc_info,
                query_object,
                check_list,
                cast,
            );

            // Special handling for power operation (^)
            if op == "^" {
                if left_type == "f64" || cast == "f64" {
                    return format!("({}).powf({})", left_expr, right_expr);
                } else {
                    return format!("({}).pow({})", left_expr, right_expr);
                }
            }

            format!(
                "{}{} {} {}{}",
                if *is_par { "(" } else { "" },
                left_expr,
                op,
                right_expr,
                if *is_par { ")" } else { "" }
            )
        }
    } else if let Some(ref col) = field.column_ref {
        let needs_casting = !cast.is_empty();
        // Handle column reference - must be a key column in grouped context
        // Verify this is a key column
        if !keys.iter().any(|key| key.0.column == col.column) {
            panic!(
                "Column {} must be a key column when used with aggregates",
                col.column
            );
        }

        let is_single_key = keys.len() == 1;
        let col_type = query_object.get_type(col);

        // Get position in key tuple
        let key_position = keys
            .iter()
            .position(|key| key.0.column == col.column)
            .expect("Key column not found");

        // Key columns are accessed via x.0 and don't need safety checks
        if is_single_key {
            if col_type == "f64" {
                check_list.push("x.0.is_some()".to_string());
                return "x.0.unwrap().into_inner()".to_string();
            } else if needs_casting {
                return format!("(x.0 as {})", cast);
            } else {
                return format!("x.0{}", if col_type == "String" { ".clone()" } else { "" });
            }
        } else if col_type == "f64" {
            check_list.push(format!("x.0.{}.is_some()", key_position));
            return format!("x.0.{}.unwrap().into_inner()", key_position);
        } else if needs_casting {
            return format!("(x.0.{} as {})", key_position, cast);
        } else {
            return format!(
                "x.0.{}{}",
                key_position,
                if col_type == "String" { ".clone()" } else { "" }
            );
        }
    } else if let Some(ref lit) = field.literal {
        // Handle literal values
        match lit {
            IrLiteral::Integer(i) => {
                if !cast.is_empty() {
                    format!("({}.0)", i)
                } else {
                    i.to_string()
                }
            }
            IrLiteral::Float(f) => format!("{:.2}", f),
            IrLiteral::String(s) => format!("\"{}\"", s),
            IrLiteral::Boolean(b) => b.to_string()
        }
    } else if let Some(ref agg) = field.aggregate {
        // Handle aggregate access
        let is_single_acc = acc_info.value_positions.len() == 1;

        match agg.function {
            AggregateType::Avg => {
                // Get sum and count positions
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

                check_list.push(format!(
                    "x{}.{}.is_some()",
                    if is_keyed { ".1" } else { "" },
                    sum_pos
                ));
                format!(
                    "((x{}{}.unwrap() as f64) / (x{}{} as f64))",
                    if is_keyed { ".1" } else { "" },
                    if is_single_acc {
                        "".to_string()
                    } else {
                        format!(".{}", sum_pos)
                    },
                    if is_keyed { ".1" } else { "" },
                    if is_single_acc {
                        "".to_string()
                    } else {
                        format!(".{}", count_pos)
                    }
                )
            }
            AggregateType::Count => {
                let pos = acc_info
                    .value_positions
                    .get(&AccumulatorValue::Aggregate(
                        agg.function.clone(),
                        agg.column.clone(),
                    ))
                    .unwrap()
                    .0;

                if !cast.is_empty() {
                    format!(
                        "(x{}{} as {})",
                        if is_keyed { ".1" } else { "" },
                        if is_single_acc {
                            "".to_string()
                        } else {
                            format!(".{}", pos)
                        },
                        cast
                    )
                }
                // Count doesn't need a safety check as it's always available
                else {
                    format!(
                        "x{}{}",
                        if is_keyed { ".1" } else { "" },
                        if is_single_acc {
                            "".to_string()
                        } else {
                            format!(".{}", pos)
                        }
                    )
                }
            }
            _ => {
                // MAX, MIN, SUM
                let pos = acc_info
                    .value_positions
                    .get(&AccumulatorValue::Aggregate(
                        agg.function.clone(),
                        agg.column.clone(),
                    ))
                    .unwrap()
                    .0;

                let acc_type = query_object.get_type(&agg.column);

                check_list.push(format!(
                    "x{}{}.is_some()",
                    if is_keyed { ".1" } else { "" },
                    if is_single_acc {
                        "".to_string()
                    } else {
                        format!(".{}", pos)
                    }
                ));

                if !cast.is_empty() && *cast != acc_type {
                    format!(
                        "(x{}{}.unwrap() as {})",
                        if is_keyed { ".1" } else { "" },
                        if is_single_acc {
                            "".to_string()
                        } else {
                            format!(".{}", pos)
                        },
                        cast
                    )
                } else {
                    format!(
                        "x{}{}.unwrap()",
                        if is_keyed { ".1" } else { "" },
                        if is_single_acc {
                            "".to_string()
                        } else {
                            format!(".{}", pos)
                        }
                    )
                }
            }
        }
    } else if let Some((ref result, ref result_type)) = field.subquery_vec {
        if result_type == "String" {
            format!("{}.first().unwrap().unwrap().to_string().clone()", result)
        } else if result_type == "f64" {
            format!("{}.first().unwrap().unwrap().into_inner()", result)
        } else if !cast.is_empty() {
            format!("({}.first().unwrap().unwrap().clone() as {})", result, cast)
        } else {
            format!("{}.first().unwrap().unwrap().clone()", result)
        }
    } else {
        panic!("Invalid ComplexField - no valid content");
    }
}
