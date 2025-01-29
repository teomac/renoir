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
        if let SelectClause::Column(col) = &select_clauses[0] {
            if col.column == "*" {
                return ".map(|x| x)".to_string();
            }
        }
    }

    // Start building the map expression
    let mut map_internals = String::new();

    // Process each select clause
    for (i, clause) in select_clauses.iter().enumerate() {
        match clause {
            SelectClause::Column(col_ref) => {
                query_object.insert_projection(&col_ref, "");
            }
            SelectClause::Aggregate(agg) => {
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
            SelectClause::ComplexValue(col_ref, op, val) => {
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

    if has_aggregates {
        // First create the fold string - this will be our final output
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
        let type_declarations = query_object
            .projections
            .iter()
            .map(|(col_ref, op)| query_object.get_type(&col_ref))
            .collect::<Vec<String>>();

        let type_str = if is_single_aggregate {
            type_declarations[0].clone()
        } else {
            format!("({})", type_declarations.join(", "))
        };

        // Add fold operation
        final_string.push_str(&format!(
            ".fold(None, |acc: &mut Option<{}>, x| {{ match acc {{",
            type_str
        ));

        // Initialize with first values case
        final_string.push_str("\n            None => *acc = Some((");

        for i in 0..query_object.projections.len() {
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
        for i in 0..query_object.projections.len() {
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

        // Process each projection
        for (i, (_, operation)) in query_object.projections.iter().enumerate() {
            if i > 0 {
                final_string.push_str(", ");
            }

            let x_value = if is_single_aggregate {
                "x"
            } else {
                &format!("x.{}", i)
            };

            match operation.as_str() {
                "Max" => final_string.push_str(&format!(
                    "{}::max(*acc{}, {})",
                    type_declarations[i], i, x_value
                )),
                "Min" => final_string.push_str(&format!(
                    "{}::min(*acc{}, {})",
                    type_declarations[i], i, x_value
                )),
                "Sum" => final_string.push_str(&format!("*acc{} + {}", i, x_value)),
                "Count" => {
                    if final_string.contains("None =>") {
                        final_string = final_string.replace(&format!("x.{}", i), "1.0");
                    }
                    final_string.push_str(&format!("*acc{} + 1.0", i));
                }
                "Avg" => {
                    //TODO
                    unreachable!();
                }
                _ => final_string.push_str(&format!("*acc{}", i)), // Non-aggregated columns keep original value
            }
        }

        if !is_single_aggregate {
            final_string.push(')');
        }
        final_string.push_str(");\n            }\n        }})");

        final_string
    } else {
        // Rest of the code for non-aggregate case remains the same...
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
        map_string.push_str(")");
        map_string
    }
}
