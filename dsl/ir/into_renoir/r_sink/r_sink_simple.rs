use crate::dsl::ir::ir_ast_structure::{ComplexField, ProjectionColumn};
use crate::dsl::ir::IrLiteral;
use crate::dsl::struct_object::object::QueryObject;
use core::panic;

pub fn create_simple_map(
    projection_clauses: &[ProjectionColumn],
    stream_name: &String,
    query_object: &QueryObject,
) -> String {
    let empty_string = "".to_string();
    let mut all_streams = Vec::new();

    let main_stream = query_object.get_stream(stream_name);
    let mut map_string = format!(".map(move |x| {} {{ ", main_stream.final_struct_name.last().unwrap());
    //if it has a join tree, get all the streams involved in the join
    if main_stream.join_tree.is_some() {
        all_streams.extend(
            main_stream
                .join_tree
                .clone()
                .unwrap()
                .get_involved_streams(),
        );
    } else {
        all_streams.push(stream_name.clone());
    }

    let is_grouped = main_stream.is_keyed && !main_stream.key_columns.is_empty();
    let mut keys = Vec::new();
    for stream in all_streams.iter() {
        keys.extend(query_object.get_stream(stream).key_columns.clone());
    }

    let mut check_list = Vec::new();

    let fields: Vec<String> = projection_clauses
        .iter()
        .enumerate() // Add enumerate to track position
        .map(|(i, clause)| {
            match clause {
                ProjectionColumn::Column(col_ref, _) => {
                    let field_name = query_object
                        .result_column_types
                        .get_index(i)
                        .unwrap_or((&empty_string, &empty_string))
                        .0;

                    let col_stream_name = if col_ref.table.is_some() {
                        query_object
                            .get_stream_from_alias(col_ref.table.as_ref().unwrap())
                            .unwrap()
                    } else {
                        stream_name
                    };

                    let col_type = query_object.get_type(col_ref);

                    let stream = query_object.get_stream(col_stream_name);
                    stream.check_if_column_exists(&col_ref.column);

                    //if the stream is grouped, check if the column is a key column
                    let mut is_key: bool = false;
                    if is_grouped {
                        if !keys.iter().any(|key| key == col_ref) {
                            panic!(
                                "Column {} is not a key column in the grouped stream",
                                col_ref.column
                            );
                        } else {
                            is_key = true;
                        }
                    }

                    if is_key {
                        let key_pos = keys.iter().position(|key| key == col_ref).unwrap();
                        let value: String;
                        if keys.len() == 1 {
                            if col_type == "f64" {
                                value = "if x.0.is_some() { Some(x.0.unwrap().into_inner() as f64) } else { None }".to_string();

                            } else {
                                value = format!(
                                    "{}x.0{}",
                                    if col_type == "bool" {"*"} else {""}
                                    ,if col_type == "String" { ".clone()" } else { "" }
                                );

                            }
                            format!("{}: {}", field_name, value)
                        } else {
                            if col_type == "f64" {
                                value = format!(
                                    "if x.0.{}.is_some() {{ Some(x.0.{}.unwrap().into_inner() as f64) }} else {{ None }}",

                                    key_pos,
                                    key_pos
                                );
                            } else {
                                value = format!("{}x.0.{}{}",
                                if col_type == "bool" {"*"} else {""},
                                key_pos, if col_type == "String" { ".clone()" } else { "" });

                            }
                            format!("{}: {}", field_name, value)
                        }
                    } else {
                        let value = format!(
                            "x{}.{}",
                            stream.get_access().get_base_path(),
                            col_ref.column
                        );
                        format!("{}: {}", field_name, value)
                    }
                }
                ProjectionColumn::ComplexValue(complex_field, alias) => {
                    let field_name = alias.as_ref().unwrap_or_else(|| {
                        query_object
                            .result_column_types
                            .iter()
                            .nth(i) // Use i from enumerate instead
                            .map(|(name, _)| name)
                            .unwrap()
                    });
                    let value = process_complex_field(
                        complex_field,
                        stream_name,
                        query_object,
                        &mut check_list,
                        &all_streams,
                    );
                    // Deduplicate and the check list
                    check_list.sort();
                    check_list.dedup();
                    let is_check_list_empty = check_list.is_empty();
                    if is_check_list_empty {
                        format!("{}: Some({})", field_name, value)
                    } else {
                        format!(
                            "{}: if {} {{Some({})}} else {{ None }}",
                            field_name,
                            check_list.join(" && "),
                            value
                        )
                    }
                }
                ProjectionColumn::StringLiteral(value, alias) => {
                    let field_name = alias.as_ref().unwrap_or_else(|| {
                        query_object
                            .result_column_types
                            .iter()
                            .nth(i) // Use i from enumerate instead
                            .map(|(name, _)| name)
                            .unwrap()
                    });
                    format!("{}: Some(\"{}\".to_string())", field_name, value)
                }
                ProjectionColumn::SubqueryVec(result , alias) => {
                    let field_name = alias.as_ref().unwrap_or_else(|| {
                        query_object
                            .result_column_types
                            .iter()
                            .nth(i) // Use i from enumerate instead
                            .map(|(name, _)| name)
                            .unwrap()
                    });
                    format!("{}: Some({}.first().unwrap().unwrap().to_string().clone())", field_name, result)
                }
                _ => unreachable!("Should not have aggregates in simple map"),
            }
        })
        .collect();

    map_string.push_str(&fields.join(", "));
    map_string.push_str(" })");
    map_string
}

pub fn process_complex_field(
    field: &ComplexField,
    stream_name: &String,
    query_object: &QueryObject,
    check_list: &mut Vec<String>,
    all_streams: &Vec<String>,
) -> String {
    let mut keys = Vec::new();
    for stream in all_streams.iter() {
        keys.extend(query_object.get_stream(stream).key_columns.clone());
    }

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
                        process_complex_field(
                            left,
                            stream_name,
                            query_object,
                            check_list,
                            all_streams
                        ),
                        op,
                        process_complex_field(
                            right,
                            stream_name,
                            query_object,
                            check_list,
                            all_streams
                        )
                    );
                }

                // Special handling for power operation (^)
                if op == "^" {
                    let left_expr = process_complex_field(
                        left,
                        stream_name,
                        query_object,
                        check_list,
                        all_streams,
                    );
                    let right_expr = process_complex_field(
                        right,
                        stream_name,
                        query_object,
                        check_list,
                        all_streams,
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
                    process_complex_field(left, stream_name, query_object, check_list, all_streams);
                let right_expr = process_complex_field(
                    right,
                    stream_name,
                    query_object,
                    check_list,
                    all_streams,
                );

                // Add as f64 to integer literals when needed
                let processed_left = if let Some(IrLiteral::Integer(_)) = left.literal {
                    format!("{} as f64", left_expr)
                } else {
                    left_expr
                };

                let processed_right = if let Some(IrLiteral::Integer(_)) = right.literal {
                    format!("{} as f64", right_expr)
                } else {
                    right_expr
                };

                //if left is i64 and right is float or vice versa, convert the i64 to f64
                if left_type == "i64" && right_type == "f64" {
                    return format!("({} as f64 {} {})", processed_left, op, processed_right);
                } else if left_type == "f64" && right_type == "i64" {
                    return format!("({} {} {} as f64)", processed_left, op, processed_right);
                }

                format!("({} {} {})", processed_left, op, processed_right)
            } else {
                panic!(
                    "Invalid arithmetic expression - incompatible types: {} and {}",
                    left_type, right_type
                );
            }
        } else {
            //case same type
            //if operation is plus, minus, multiply, division, or power and types are not numeric, panic
            if (op == "+" || op == "-" || op == "*" || op == "/" || op == "^")
                && left_type != "f64"
                && left_type != "i64"
            {
                panic!(
                    "Invalid arithmetic expression - non-numeric types: {} and {}",
                    left_type, right_type
                );
            }

            // Division always results in f64
            if op == "/" {
                return format!(
                    "({} as f64) {} ({} as f64)",
                    process_complex_field(left, stream_name, query_object, check_list, all_streams),
                    op,
                    process_complex_field(
                        right,
                        stream_name,
                        query_object,
                        check_list,
                        all_streams
                    )
                );
            }

            // Special handling for power operation (^)
            if op == "^" {
                let left_expr =
                    process_complex_field(left, stream_name, query_object, check_list, all_streams);
                let right_expr = process_complex_field(
                    right,
                    stream_name,
                    query_object,
                    check_list,
                    all_streams,
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
                process_complex_field(left, stream_name, query_object, check_list, all_streams),
                op,
                process_complex_field(right, stream_name, query_object, check_list, all_streams)
            )
        }
    } else if let Some(ref col) = field.column_ref {
        // Handle column reference
        let col_stream_name = if col.table.is_some() {
            query_object
                .get_stream_from_alias(col.table.as_ref().unwrap())
                .unwrap()
        } else {
            stream_name
        };

        let col_stream = query_object.get_stream(col_stream_name);
        col_stream.check_if_column_exists(&col.column);

        let col_type = query_object.get_type(col);

        //if the stream is grouped, check if the column is a key column
        let mut is_key = false;

        if query_object.get_stream(stream_name).is_keyed {
            if !keys.iter().any(|key| key == col) {
                panic!(
                    "Column {} is not a key column in the grouped stream",
                    col.column
                );
            } else {
                is_key = true;
            }
        }

        if is_key {
            let key_pos = keys.iter().position(|key| key == col).unwrap();
            if keys.len() == 1 {
                if col_type == "f64" {
                    check_list.push("x.0.is_some()".to_string());
                    return "x.0.unwrap().into_inner()".to_string();
                } else {
                    return format!("x.0{}", if col_type == "String" { ".clone()" } else { "" });
                }
            } else if col_type == "f64" {
                check_list.push(format!("x.0.{}.is_some()", key_pos));
                return format!("x.0.{}.unwrap().into_inner()", key_pos,);
            } else {
                return format!(
                    "x.0.{}{}",
                    key_pos,
                    if col_type == "String" { ".clone()" } else { "" }
                );
            }
        } else {
            check_list.push(format!(
                "x{}.{}.is_some()",
                col_stream.access.base_path, col.column
            ));
            format!("x{}.{}.unwrap()", col_stream.access.base_path, col.column)
        }
    } else if let Some(ref lit) = field.literal {
        // Handle literal value
        match lit {
            IrLiteral::Integer(i) => i.to_string(),
            IrLiteral::Float(f) => format!("{:.2}", f),
            IrLiteral::String(s) => format!("\"{}\"", s),
            IrLiteral::Boolean(b) => b.to_string(),
            IrLiteral::ColumnRef(_) => {
                panic!("Column ref should have been handled earlier");
            }
        }
    } else if let Some((ref result, ref result_type)) = field.subquery_vec {
        if result_type == "String" {
            format!("{}.first().unwrap().unwrap().to_string().clone()", result)
        } else {
            format!("{}.first().unwrap().unwrap().clone()", result)
        }
    } else {
        panic!("Invalid ComplexField - no valid content");
    }
}
