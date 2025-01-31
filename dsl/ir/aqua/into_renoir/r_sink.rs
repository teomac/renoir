use crate::dsl::ir::aqua::r_utils::convert_column_ref;
use crate::dsl::ir::aqua::{AggregateType, AquaLiteral, QueryObject, SelectClause};

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
    if select_clauses.len() == 1 {
        if let SelectClause::Column(col, alias) = &select_clauses[0] {
            if col.column == "*" {
                return ".map(|x| x)".to_string();
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
                let data_type = query_object.get_type(&agg.column);
                if data_type != "f64" && data_type != "i64" {
                    panic!("Invalid type for aggregation");
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

fn create_map_string(query_object: &QueryObject) -> String {
    // Check if we need to add a fold (if there are any aggregates)
    let has_aggregates = query_object
        .projections
        .iter()
        .any(|(_, op)| matches!(op.as_str(), "Max" | "Min" | "Avg" | "Sum" | "Count"));

    let mut has_avg = false;

    if has_aggregates {
        let mut final_string = String::new();

        // Check if we have only one projection and it's an aggregate
        let is_single_aggregate = query_object.projections.len() == 1
            && matches!(
                query_object.projections.iter().next().unwrap().1.as_str(),
                "Max" | "Min" | "Avg" | "Sum" | "Count"
            );

        // Add initial map operation
        final_string.push_str(".map(|x| ");

        // Don't add parentheses for single aggregate
        if !is_single_aggregate {
            final_string.push('(');
        }

        for (i, (col_ref, operation)) in query_object.projections.iter().enumerate() {
            if i > 0 {
                final_string.push_str(", ");
            }

            final_string.push_str(&convert_column_ref(col_ref, query_object));
            final_string.push_str(".unwrap()");

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
            let mut temp2:Vec<((String, String), usize)>= Vec::new();
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
            if is_single_aggregate {
                final_string.push_str("x");
            } else {
                final_string.push_str(&format!("x.{}", i));
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
        println!("AAAAAAA{:?}", type_declarations2);

        
        // index that increments in the same way as i. if we have an average, it increments by one more
        let mut k:usize = 0;
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


        //TODO Check if this works without resetting k to zero
        //k = 0;

        if !is_single_aggregate {
            final_string.push(')');
        }
        final_string.push_str(");\n            }\n        }})");

        if has_avg {
            // Add another map to calculate averages
            final_string.push_str(".map(|x| match x {");
            final_string.push_str("\n        Some(");

            // If we have only one average operation
            if query_object.projections.len() == 1 && query_object.projections[0].1 == "Avg" {
                final_string.push_str("(sum, count)");
                final_string.push_str(") => Some(sum / count),");
            } else {
                // For multiple columns where some might be averages
                final_string.push('(');

                let mut current_pos = 0;
                for (i, (_, operation)) in query_object.projections.iter().enumerate() {
                    if i > 0 {
                        final_string.push_str(", ");
                    }

                    if operation == "Avg" {
                        final_string.push_str(&format!("sum{}, count{}", current_pos, current_pos));
                        current_pos += 1;
                    } else {
                        final_string.push_str(&format!("val{}", current_pos));
                        current_pos += 1;
                    }
                }

                final_string.push_str(")");
                final_string.push_str(") => Some((");

                current_pos = 0;
                for (i, (_, operation)) in query_object.projections.iter().enumerate() {
                    if i > 0 {
                        final_string.push_str(", ");
                    }

                    if operation == "Avg" {
                        final_string
                            .push_str(&format!("sum{} / count{}", current_pos, current_pos));
                        current_pos += 1;
                    } else {
                        final_string.push_str(&format!("val{}", current_pos));
                        current_pos += 1;
                    }
                }

                final_string.push_str(")),");
            }

            final_string.push_str("\n        None => None,");
            final_string.push_str("\n    })");
        }

        final_string
    } else {
        let mut map_string = String::from(".map(|x| (");

        for (i, (col_ref, operation)) in query_object.projections.iter().enumerate() {
            if i > 0 {
                map_string.push_str(", ");
            }

            map_string.push_str(&convert_column_ref(col_ref, query_object));

            if !operation.is_empty() {
                map_string.push_str(operation);
                map_string.push_str(".unwrap()");
            }
        }
        map_string.push_str("))");

        map_string
    }
}
