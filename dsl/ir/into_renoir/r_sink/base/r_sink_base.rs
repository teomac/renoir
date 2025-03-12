use core::panic;
use crate::dsl::ir::ir_ast_structure::{ComplexField, ProjectionColumn};
use crate::dsl::ir::IrLiteral;
use crate::dsl::struct_object::object::QueryObject;
use crate::dsl::ir::r_sink::base::r_sink_utils::*;
use crate::dsl::ir::r_sink::base::r_sink_base_agg::create_aggregate_map;



/// Processes a `SelectColumn` and generates a corresponding string representation
/// of the query operation.
///
/// # Arguments
///
/// * `select_clauses` - A reference to a/// * `query_object` - A reference to the `QueryObject` which contains metadata and type information for the query.
///
/// # Returns
///
/// A `String` that represents the query operation based on the provided `SelectColumn`.
///
/// # Panics
///
/// This function will panic if:
/// - The data type for aggregation is not `f64` or `i64`.
/// - The data type for power operation is not `f64` or `i64`.
///
///
///
///
///

pub fn process_projections(
    projections: &Vec<ProjectionColumn>,
    stream_name: &String,
    query_object: &mut QueryObject,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut final_string = String::new();
    // Check for SELECT * case
    if projections.len() == 1 {
        match &projections[0] {
            ProjectionColumn::Column(col_ref, _) if col_ref.column == "*" => {
                final_string = create_select_star_map(stream_name, query_object);
            }
            _ => {}
        }

        let stream_name = query_object.streams.keys().cloned().collect::<Vec<String>>()[0].clone();
        let stream = query_object.get_mut_stream(&stream_name);

        stream.insert_op(final_string.clone());

        return Ok(());
    }
    // Check if any aggregations are present using recursive traversal
    let has_aggregates: bool = projections.iter().any(|clause| match clause {
        ProjectionColumn::Aggregate(_, _) => true,
        ProjectionColumn::ComplexValue(field, _) => has_aggregate_in_complex_field(field),
        _ => false,
    });

    if has_aggregates {
        final_string = create_aggregate_map(projections, query_object);
    } else {
        final_string = create_simple_map(projections, query_object);
    }

    let stream_name = query_object.streams.keys().cloned().collect::<Vec<String>>()[0].clone();
    let stream = query_object.get_mut_stream(&stream_name);

    stream.insert_op(final_string.clone());

    Ok(())
}



fn create_select_star_map(stream_name: &String, query_object: &QueryObject) -> String {
    let mut result = String::from(".map(|x| OutputStruct { ");
    let stream = query_object.get_stream(stream_name);
    
    if query_object.has_join {
        // Handle joined case - need to use tuple access
        //for stream in all_streams, build all the columns mapping in the .map
        let mut offset: usize = 0;
        let all_streams = stream.join_tree.clone().unwrap().get_involved_streams();

        for stream in all_streams.iter() {

            let stream = query_object.get_stream(stream);
            let tuple_access = stream.get_access().get_base_path();
            let table_struct = query_object.get_struct(&stream.source_table).unwrap();

            for (column_index, field_name) in table_struct.iter().enumerate() {
                result.push_str(&format!(
                    "{}: x{}.{}, ",
                    query_object
                        .result_column_types
                        .get_index(offset + column_index)
                        .unwrap()
                        .0,
                    tuple_access,
                    field_name.0
                ));
            }

            offset += table_struct.len();
        }
    } else {
        // Simple case - direct access
        // retrieve the column list of the first table
        let columns = query_object
            .tables_info
            .get(&query_object.get_all_table_names()[0])
            .unwrap();

        //zip the column list with the result_column_types
        let zip = columns.iter().zip(query_object.result_column_types.iter());

        //iterate over the zip and build the mapping
        let fields: Vec<String> = zip
            .collect::<Vec<_>>()
            .iter()
            .map(|(column, result_column)| format!("{}: x.{}", result_column.0, column.0))
            .collect();

        result.push_str(&fields.join(", "));
    }

    result.push_str(" })");
    result
}

fn create_simple_map(projection_clauses: &Vec<ProjectionColumn>, query_object: &QueryObject) -> String {
    let mut map_string = String::from(".map(|x| OutputStruct { ");
    let empty_string = "".to_string();
    let all_streams = query_object.streams.keys().collect::<Vec<&String>>();


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
                        .unwrap_or_else(|| (&empty_string, &empty_string))
                        .0;

                    let stream_name = if col_ref.table.is_some(){
                        query_object.get_stream_from_alias(&col_ref.table.as_ref().unwrap()).unwrap()
                    }else{
                        if all_streams.len() == 1{
                            &all_streams[0].clone()}
                        else{
                            panic!("Invalid column reference - no table specified and multiple streams present in query");
                        }
                    };

                    let stream = query_object.get_stream(stream_name);
                    stream.check_if_column_exists(&col_ref.column);

                    let value = 
                        format!("x{}.{}", stream.get_access().get_base_path(), col_ref.column)
                    ;
                    format!("{}: {}", field_name, value)
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
                    let value = process_complex_field(complex_field, query_object, &mut check_list);
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
                ProjectionColumn::StringLiteral(value) => format!("{}: Some(\"{}\".to_string())", value, value),
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
    query_object: &QueryObject,
    check_list: &mut Vec<String>,
) -> String {
    let all_streams = query_object.streams.keys().collect::<Vec<&String>>();

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
                        process_complex_field(left, query_object, check_list),
                        op,
                        process_complex_field(right, query_object, check_list)
                    );
                }

                // Special handling for power operation (^)
                if op == "^" {
                    let left_expr = process_complex_field(left, query_object, check_list);
                    let right_expr = process_complex_field(right, query_object, check_list);

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

                let left_expr = process_complex_field(left, query_object, check_list);
                let right_expr = process_complex_field(right, query_object, check_list);

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
                if left_type != "f64" && left_type != "i64" {
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
                    process_complex_field(left, query_object, check_list),
                    op,
                    process_complex_field(right, query_object, check_list)
                );
            }

            // Special handling for power operation (^)
            if op == "^" {
                let left_expr = process_complex_field(left, query_object, check_list);
                let right_expr = process_complex_field(right, query_object, check_list);

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
                process_complex_field(left, query_object, check_list),
                op,
                process_complex_field(right, query_object, check_list)
            )
        }
    } else if let Some(ref col) = field.column_ref {
        // Handle column reference
        let stream_name = if col.table.is_some(){
            query_object.get_stream_from_alias(&col.table.as_ref().unwrap()).unwrap()
        }else{
            if all_streams.len() == 1{
                &all_streams[0].clone()}
            else{
                panic!("Invalid column reference - no table specified and multiple streams present in query");
            }
        };

        let stream = query_object.get_stream(stream_name);
        stream.check_if_column_exists(&col.column);

           

            check_list.push(format!("x{}.{}.is_some()", stream.access.base_path, col.column));
            format!("x{}.{}.unwrap()", stream.access.base_path, col.column)
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
    } else {
        panic!("Invalid ComplexField - no valid content");
    }
}



