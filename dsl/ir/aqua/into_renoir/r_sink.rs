use crate::dsl::ir::aqua::r_utils::convert_column_ref;
use crate::dsl::ir::aqua::{AggregateType, AquaLiteral, ColumnRef, QueryObject, SelectClause};

/// Processes a `SelectClause` and generates a corresponding string representation
/// of the query operation.
///
/// # Arguments
///
/// * 'select_clauses` - A reference to the Vec<SelectClause> which represents all selections in the query.
/// * `query_object` - A reference to the `QueryObject` which contains metadata and type information for the query.
///
/// # Returns
///
/// A `String` that represents the query operation based on the provided `SelectClause`.
///
/// # Panics
///
/// This function will panic if:
/// - The data type for aggregation is not `f64` or `i64`.
/// - The data type for power operation is not `f64` or `i64`.
///

pub fn process_select_clauses(
    select_clauses: &Vec<SelectClause>,
    query_object: &mut QueryObject,
) -> String {
    // If there's only one column and it's an asterisk, return the identity map

    //case of SELECT *
    if select_clauses.len() == 1 {
        if let SelectClause::Column(col, _alias) = &select_clauses[0] {
            if col.column == "*" {
                let mut result = String::from(".map(|x| OutputStruct { ");

                let fields: Vec<String> = query_object
                    .result_column_to_input
                    .iter()
                    .map(|(field_name, _)| format!("{}: x.{}.clone()", field_name, field_name))
                    .collect();

                result.push_str(&fields.join(", "));
                result.push_str(" })");

                return result;
            }
        }
    }
    // Start building the map expression
    let _map_internals = String::new();

    // Process each select clause
    for (_i, clause) in select_clauses.iter().enumerate() {
        match clause {
            SelectClause::Column(col_ref, _alias) => {
                query_object.insert_projection(&col_ref, "");
            }
            SelectClause::Aggregate(agg, _alias) => {
                if agg.column.column == "*" && agg.function != AggregateType::Count {
                    panic!("The only aggregate function that can be applied to '*' is COUNT");
                }
                if agg.column.column != "*" {
                    let data_type = query_object.get_type(&agg.column);
                    if data_type != "f64" && data_type != "i64" {
                        panic!("Invalid type for aggregation");
                    }
                }

                match agg.function {
                    AggregateType::Max => {
                        query_object.insert_projection(&agg.column, "Max");
                    }
                    AggregateType::Min => {
                        query_object.insert_projection(&agg.column, "Min");
                    }
                    AggregateType::Avg => {
                        query_object.insert_projection(&agg.column, "Avg");
                    }
                    AggregateType::Sum => {
                        query_object.insert_projection(&agg.column, "Sum");
                    }
                    AggregateType::Count => {
                        query_object.insert_projection(&agg.column, "Count");
                    }
                }
            }
            SelectClause::ComplexValue(col_ref, op, val, _alias) => {
                let value = match val {
                    AquaLiteral::Float(val) => format!("{:.2}", val),
                    AquaLiteral::Integer(val) => val.to_string(),
                    AquaLiteral::Boolean(val) => val.to_string(),
                    AquaLiteral::String(val) => val.clone(),
                    AquaLiteral::ColumnRef(column_ref) => {
                        convert_column_ref(&column_ref, query_object)
                    }
                };

                if op == "^" {
                    let data_type = query_object.get_type(&col_ref);
                    if data_type != "f64" && data_type != "i64" {
                        panic!("Invalid type for power operation");
                    }
                    let projection_string = ".pow(".to_string() + &value + ")";
                    query_object.insert_projection(&col_ref, &projection_string);
                } else {
                    let projection_string = format!("{} {}", op, value);
                    query_object.insert_projection(&col_ref, &projection_string);
                }
            }
        }
    }
    // call function
    create_map_string(query_object)
}

fn build_output_struct_mapping(
    query_object: &QueryObject,
    values: Vec<String>,
    is_aggregate: bool,
) -> String {
    let mut output = String::from("OutputStruct { ");

    for (i, (col, _)) in query_object.result_column_to_input.iter().enumerate() {
        if i > 0 {
            output.push_str(", ");
        }

        // If has_join, append the table alias/name as suffix
        let field_name = if query_object.has_join {
            let (_, _, table_name) = &query_object.result_column_to_input[col];
            let suffix = query_object
                .get_alias(table_name)
                .unwrap_or(table_name)
                .to_string();
            format!("{}_{}", col, suffix)
        } else {
            col.to_string()
        };

        if is_aggregate {
            output.push_str(&format!("{}: Some({})", field_name, values[i]));
        } else {
            output.push_str(&format!("{}: {}", field_name, values[i]));
        }
    }

    output.push_str(" }");
    output
}

fn create_map_string(query_object: &QueryObject) -> String {
    // Check if we need to add a fold (if there are any aggregates)
    let has_aggregates = query_object
        .projections
        .iter()
        .any(|(_, op)| matches!(op.as_str(), "Max" | "Min" | "Avg" | "Sum" | "Count"));

    let mut has_avg = false;

    if has_aggregates {
        let mut final_string = String::new();

        // Add initial map operation
        final_string.push_str(".map(|x| ");

        // Check if we have only one projection and it's an aggregate
        let is_single_aggregate = query_object.projections.len() == 1
            && matches!(
                query_object.projections.iter().next().unwrap().1.as_str(),
                "Max" | "Min" | "Avg" | "Sum" | "Count"
            );

        // Don't add parentheses for single aggregate
        if !is_single_aggregate {
            final_string.push('(');
        }

        for (i, (col_ref, operation)) in query_object.projections.iter().enumerate() {
            if i > 0 {
                final_string.push_str(", ");
            }

            if operation == "Count" && col_ref.column == "*" {
                // For COUNT(*), use the first column from the table
                let table_name = match &col_ref.table {
                    Some(t) => t.clone(),
                    None => query_object.get_all_table_names().first().unwrap().clone(),
                };
                let first_column = query_object
                    .table_to_struct
                    .get(&table_name)
                    .unwrap()
                    .keys()
                    .next()
                    .unwrap()
                    .clone();

                // Create a new ColumnRef with the first column
                let first_col_ref = ColumnRef {
                    table: col_ref.table.clone(),
                    column: first_column,
                };
                final_string.push_str(&convert_column_ref(&first_col_ref, query_object));
                final_string.push_str(".unwrap()");
            } else {
                final_string.push_str(&convert_column_ref(col_ref, query_object));
                final_string.push_str(".unwrap()");
            }

            if operation.as_str() == "Avg" {
                has_avg = true;

                final_string.push_str(", ");
                final_string.push_str(&convert_column_ref(col_ref, query_object));
                final_string.push_str(".unwrap()");
            }

            // Add any complex operations (but not aggregates)
            if !matches!(
                operation.as_str(),
                "" | "Max" | "Min" | "Avg" | "Sum" | "Count"
            ) {
                final_string.push_str(operation);
            }
        }

        if !is_single_aggregate {
            final_string.push(')');
        }
        final_string.push(')');

        // Get tuple type for fold accumulator
        let mut type_declarations = query_object
            .projections
            .iter()
            .map(|(col_ref, op)| (query_object.get_type(&col_ref), op.clone()))
            .collect::<Vec<(String, String)>>();

        let type_str = if is_single_aggregate {
            type_declarations[0].0.clone().to_string()
        } else {
            let mut temp = String::new();
            let mut temp2: Vec<((String, String), usize)> = Vec::new();
            for i in 0..type_declarations.len() {
                if type_declarations[i].1 == "Avg" {
                    temp2.push(((type_declarations[i].0.clone(), "Avg".to_string()), i));
                }
                if i == 0 {
                    temp.push_str(&format!("({} ", type_declarations[i].0));
                    if type_declarations[i].1 == "Avg" {
                        temp.push_str(&format!(",{} ", type_declarations[i].0));
                    }
                } else {
                    temp.push_str(&format!(",{} ", type_declarations[i].0));
                    if type_declarations[i].1 == "Avg" {
                        temp.push_str(&format!(",{} ", type_declarations[i].0));
                    }
                }
            }
            temp.push(')');
            if !temp2.is_empty() {
                for temp2 in temp2.iter() {
                    type_declarations.insert(temp2.1, temp2.0.clone());
                }
            }
            temp
        };

        // Add fold operation
        final_string.push_str(&format!(
            ".fold(None, |acc: &mut Option<{}>, x| {{ match acc {{",
            type_str
        ));

        // Initialize with first values case
        final_string.push_str("\n            None => *acc = Some((");

        for i in 0..type_declarations.len() {
            if i > 0 {
                final_string.push_str(", ");
            }
            //if we have a count(*), we need to initialize the accumulator with 1.0
            if type_declarations[i].1 == "Count" {
                final_string.push_str("1.0");
            } 
            else {
                if is_single_aggregate {
                    final_string.push_str("x");
                } else {
                    final_string.push_str(&format!("x.{}", i));
                }
            }
        }

        final_string.push_str(")),");

        // Update values case
        final_string.push_str("\n            Some(");
        if !is_single_aggregate {
            final_string.push('(');
        }
        // Declare variables for accumulated values
        let mut acc_vars = Vec::new();
        for i in 0..type_declarations.len() {
            if i > 0 {
                final_string.push_str(", ");
            }
            acc_vars.push(format!("acc{}", i));
            final_string.push_str(&format!("acc{}", i));
        }
        if !is_single_aggregate {
            final_string.push(')');
        }
        final_string.push_str(") => {\n                *acc = Some(");
        if !is_single_aggregate {
            final_string.push('(');
        }

        let mut type_declarations2 = type_declarations.clone();

        // index that increments in the same way as i. if we have an average, it increments by one more
        let mut k: usize = 0;
        // Process each projection
        for (i, (_, operation)) in query_object.projections.iter().enumerate() {
            if i > 0 {
                final_string.push_str(", ");
            }

            let x_value = if is_single_aggregate {
                "x"
            } else {
                &format!("x.{}", k)
            };

            match operation.as_str() {
                "Max" => final_string.push_str(&format!(
                    "{}::max(*acc{}, {})",
                    type_declarations2[i].0, k, x_value
                )),
                "Min" => final_string.push_str(&format!(
                    "{}::min(*acc{}, {})",
                    type_declarations2[i].0, k, x_value
                )),
                "Sum" => final_string.push_str(&format!("*acc{} + {}", k, x_value)),
                "Count" => {
                    if final_string.contains("None =>") {
                        final_string = final_string.replace(&format!("x.{}", k), "1.0");
                    }
                    final_string.push_str(&format!("*acc{} + 1.0", k));
                }
                "Avg" => {
                    // first, we push the sum
                    final_string.push_str(&format!("*acc{} + {}", k, x_value));
                    // then, we push the count
                    if final_string.contains("None =>") {
                        final_string = final_string.replace(&format!("x.{}", k + 1), "1.0");
                    }
                    final_string.push_str(&format!(", *acc{} + 1.0", k + 1));

                    type_declarations2.remove(i + 1);

                    k += 1;
                }
                _ => final_string.push_str(&format!("*acc{}", k)), // Non-aggregated columns keep original value
            }
            k += 1;
        }

        if !is_single_aggregate {
            final_string.push(')');
        }
        final_string.push_str(");\n            }\n        }})");


        //add final mapping to OutputStruct
        final_string.push_str(".map(|x| match x {");
        final_string.push_str("\n        Some(");

        if query_object.projections.len() == 1 {
            if has_avg {
                final_string.push_str("(sum, count)");
                let avg_value = "sum / count".to_string();
                final_string.push_str(&format!(
                    ") => Some({}),",
                    build_output_struct_mapping(query_object, vec![avg_value], true)
                ));
            } else {
                final_string.push_str("value");
                final_string.push_str(&format!(
                    ") => Some({}),",
                    build_output_struct_mapping(query_object, vec!["value".to_string()], true)
                ));
            }
        } else {
            // Multiple projections case
            let mut input_field_values = Vec::new();
            let mut output_field_values = Vec::new();
            let mut current_pos = 0;
            let mut input_value;
            let mut output_value;

            for (_, operation) in query_object.projections.iter() {
                if operation == "Avg" {
                    input_value = format!("sum{} , count{}", current_pos, current_pos);
                    output_value = format!("sum{} / count{}", current_pos, current_pos);

                    current_pos += 1;
                } else {
                    input_value = format!("val{}", current_pos);
                    output_value = input_value.clone();
                    current_pos += 1;
                };
                input_field_values.push(input_value);
                output_field_values.push(output_value);
            }

            //this needs to be changed in case of AVG
            final_string.push_str(&format!("({})", input_field_values.join(", ")));

            //this is universal
            final_string.push_str(&format!(
                ") => Some({}),",
                build_output_struct_mapping(query_object, output_field_values, true)
            ));
        }

        final_string.push_str("\n        None => None,");
        final_string.push_str("\n    })");

        final_string
    } else {
        // Simple mapping case without aggregation
        let mut map_string = String::from(".map(|x| ");

        let mut values = Vec::new();
        for (col_ref, operation) in &query_object.projections {
            let mut value = convert_column_ref(col_ref, query_object).to_string();
            
            if !operation.is_empty() {
                value.push_str(operation);
                value.push_str(".unwrap()");
            }
            values.push(value);
        }

        map_string.push_str(&build_output_struct_mapping(query_object, values, false));
        map_string.push_str(")");

        map_string
    }
}
