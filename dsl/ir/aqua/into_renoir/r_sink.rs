use std::io::empty;

use crate::dsl::ir::aqua::ir_ast_structure::ComplexField;
use crate::dsl::ir::aqua::literal::LiteralParser;
use crate::dsl::ir::aqua::r_utils::convert_column_ref;
use crate::dsl::ir::aqua::{
    AggregateFunction, AggregateType, AquaLiteral, ColumnRef, SelectClause,
};
use crate::dsl::struct_object::object::{QueryObject, Operation};


/// Processes a `SelectClause` and generates a corresponding string representation
/// of the query operation.
///
/// # Arguments
///
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
    query_object: &mut QueryObject,
) -> String {

    //create an empty operation
    let empty_op = Operation {
        current_op: "".to_string(),
        next_op: "".to_string(),
        table: "".to_string(),
        input_column: "".to_string(),
    };

    // Check if we have a single column and it's an asterisk
    if query_object.result_column_to_input.len() == 1 {
        if let Some((col, res)) = query_object.result_column_to_input.iter().next() {
            if col == "*" || res.operations.first().unwrap_or_else(|| &empty_op).current_op.contains("*") {
                return create_select_star_map(query_object);
        }
    }}
    // Start building the map expression
    let _map_internals = String::new();

    // Check if any aggregations are present
    let has_aggregates = query_object.result_column_to_input.iter()
        .any(|(_, result_col)| {
            result_col.operations.iter().any(|op| 
                matches!(op.current_op.as_str(), "sum" | "count" | "avg" | "min" | "max")
            )
        });

    if has_aggregates {
        create_aggregate_map(query_object)
    } else {
        create_simple_map(query_object)
    }

    /*
    // Process each select clause
    for (_i, clause) in select_clauses.iter().enumerate() {
        match clause {
            SelectClause::Column(col_ref, _alias) => {
                query_object.insert_projection(
                    &ComplexField {
                        column: Some(ColumnRef {
                            table: col_ref.table.clone(),
                            column: col_ref.column.clone(),
                        }),
                        literal: None,
                        aggregate: None,
                    },
                    "",
                );
            }
            SelectClause::Aggregate(agg, _alias) => {
                if agg.column.column == "*" && agg.function != AggregateType::Count {
                    panic!("The only aggregate function that can be applied to '*' is COUNT");
                }
                if agg.column.column != "*" {
                    let data_type = query_object.get_type(&agg.column);
                    if agg.function != AggregateType::Count
                        && data_type != "f64"
                        && data_type != "i64"
                    {
                        panic!("Invalid type for aggregation");
                    }
                }

                match agg.function {
                    AggregateType::Max => {
                        query_object.insert_projection(
                            &ComplexField {
                                column: None,
                                literal: None,
                                aggregate: Some(AggregateFunction {
                                    function: AggregateType::Max,
                                    column: ColumnRef {
                                        table: agg.column.table.clone(),
                                        column: agg.column.column.clone(),
                                    },
                                }),
                            },
                            "Max",
                        );
                    }
                    AggregateType::Min => {
                        query_object.insert_projection(
                            &ComplexField {
                                column: None,
                                literal: None,
                                aggregate: Some(AggregateFunction {
                                    function: AggregateType::Min,
                                    column: ColumnRef {
                                        table: agg.column.table.clone(),
                                        column: agg.column.column.clone(),
                                    },
                                }),
                            },
                            "Min",
                        );
                    }
                    AggregateType::Avg => {
                        query_object.insert_projection(
                            &ComplexField {
                                column: None,
                                literal: None,
                                aggregate: Some(AggregateFunction {
                                    function: AggregateType::Avg,
                                    column: ColumnRef {
                                        table: agg.column.table.clone(),
                                        column: agg.column.column.clone(),
                                    },
                                }),
                            },
                            "Avg",
                        );
                    }
                    AggregateType::Sum => {
                        query_object.insert_projection(
                            &ComplexField {
                                column: None,
                                literal: None,
                                aggregate: Some(AggregateFunction {
                                    function: AggregateType::Sum,
                                    column: ColumnRef {
                                        table: agg.column.table.clone(),
                                        column: agg.column.column.clone(),
                                    },
                                }),
                            },
                            "Sum",
                        );
                    }
                    AggregateType::Count => {
                        query_object.insert_projection(
                            &ComplexField {
                                column: None,
                                literal: None,
                                aggregate: Some(AggregateFunction {
                                    function: AggregateType::Count,
                                    column: ColumnRef {
                                        table: agg.column.table.clone(),
                                        column: agg.column.column.clone(),
                                    },
                                }),
                            },
                            "Count",
                        );
                    }
                }
            }
            SelectClause::ComplexValue(left_field, op, right_field, alias) => {
                //parse left field to check if it is a ColumnRef or a Literal or an aggregate expr
                let left_is_literal = left_field.literal.is_some();
                let right_is_literal = right_field.literal.is_some();
                let left_is_aggregate = left_field.aggregate.is_some();
                let right_is_aggregate = right_field.aggregate.is_some();
                let left_is_column = left_field.column.is_some();
                let right_is_column = right_field.column.is_some();
                let left_data_type;
                let right_data_type;

                let mut left_col_ref = ColumnRef {
                    table: None,
                    column: String::new(),
                };
                let mut right_col_ref = ColumnRef {
                    table: None,
                    column: String::new(),
                };

                //check left field type
                if left_is_column {
                    left_col_ref = left_field.column.clone().unwrap();
                }

                //check right field type
                if right_is_column {
                    right_col_ref = right_field.column.clone().unwrap();
                }

                    //process left field
                    let _left;
                    if left_is_literal {
                        match &left_field.literal {
                            Some(AquaLiteral::Float(val)) => {
                                _left = format!("{:.2}", val);
                                left_data_type = "f64".to_string();
                            }
                            Some(AquaLiteral::Integer(val)) => {
                                _left = val.to_string();
                                left_data_type = "i64".to_string();
                            }
                            Some(AquaLiteral::Boolean(val)) => {
                                _left = val.to_string();
                                left_data_type = "bool".to_string();
                            }
                            Some(AquaLiteral::String(val)) => {
                                _left = val.clone();
                                left_data_type = "String".to_string();
                            }
                            Some(AquaLiteral::ColumnRef(column_ref)) => {
                                _left = convert_column_ref(&column_ref, query_object);
                                left_data_type = query_object.get_type(&column_ref);
                            }
                            None => panic!("Invalid left field"),
                        }
                    } else {
                        _left = convert_column_ref(&left_col_ref, query_object);
                        left_data_type = query_object.get_type(&left_col_ref);
                    }

                    //process right field
                    let right;
                    if right_is_literal {
                        match &right_field.literal {
                            Some(AquaLiteral::Float(val)) => {
                                right = format!("{:.2}", val);
                                right_data_type = "f64".to_string();
                            }
                            Some(AquaLiteral::Integer(val)) => {
                                right = val.to_string();
                                right_data_type = "i64".to_string();
                            }
                            Some(AquaLiteral::Boolean(val)) => {
                                right = val.to_string();
                                right_data_type = "bool".to_string();
                            }
                            Some(AquaLiteral::String(val)) => {
                                right = val.clone();
                                right_data_type = "String".to_string();
                            }
                            Some(AquaLiteral::ColumnRef(column_ref)) => {
                                right = convert_column_ref(&column_ref, query_object);
                                right_data_type = query_object.get_type(&column_ref);
                            }
                            None => panic!("Invalid right field"),
                        }
                    } else {
                        right = convert_column_ref(&right_col_ref, query_object);
                        right_data_type = query_object.get_type(&right_col_ref);
                    }

                    if op == "^" {
                        if left_data_type != "f64"
                            && left_data_type != "i64"
                            && right_data_type != "f64"
                            && right_data_type != "i64"
                        {
                            panic!("Invalid type for power operation");
                        }
                        if left_data_type != right_data_type {
                            panic!("Data types for power operation must be the same");
                        }
                        let projection_string;
                        if left_data_type == "i64" {
                            if left_is_literal {
                                projection_string = "_i64.pow(".to_string() + &right + ".unwrap())";
                            } else {
                                projection_string = ".pow(".to_string() + &right + ")";
                            }
                            query_object.insert_projection(&left_field, &projection_string);
                        } else {
                            if left_is_literal {
                                projection_string =
                                    "_f64.powf(".to_string() + &right + ".unwrap())";
                            } else {
                                projection_string = ".powf(".to_string() + &right + ")";
                            }
                            query_object.insert_projection(&left_field, &projection_string);
                        }
                    } else {
                        if left_is_literal {
                            let projection_string = format!("{} {}.unwrap()", op, right);
                            query_object.insert_projection(&left_field, &projection_string);
                        } else {
                            let projection_string = format!("{} {}", op, right);
                            query_object.insert_projection(&left_field, &projection_string);
                        }
                    }
                
            }
        }
    }
    // call function
    create_map_string(query_object)*/
}

fn build_output_struct_mapping(
    query_object: &QueryObject,
    values: Vec<String>,
    is_aggregate: bool,
) -> String {
    let mut output = String::from("OutputStruct { ");

    println!("values: {:?}", values);
    println!("query_object: {:?}", query_object.projections);

    for (i, (col, _)) in query_object.result_column_to_input.iter().enumerate() {
        if i > 0 {
            output.push_str(", ");
        }
        if is_aggregate {
            output.push_str(&format!("{}: Some({})", col, values[i]));
        } else {
            output.push_str(&format!("{}: {}", col, values[i]));
        }
    }

    output.push_str(" }");
    output
}

fn create_aggregate_string(query_object: &QueryObject) -> String {

    let mut has_avg = false;

        let mut final_string = String::new();
        let empty_operation = Operation {
            current_op: "".to_string(),
            table: "".to_string(),
            input_column: "".to_string(),
            next_op: "".to_string(),
        };

        // Add initial map operation
        final_string.push_str(".map(|x| (");

        // Check if we have only one projection and it's an aggregate
        let is_single_aggregate = query_object.result_column_to_input.len() == 1
            && matches!(
                query_object.result_column_to_input.first().unwrap().1.operations.first().unwrap_or_else(|| &empty_operation).current_op.as_str(),
                "Max" | "Min" | "Avg" | "Sum" | "Count"
            );

        for (i, (column, operation)) in query_object.result_column_to_input.iter().enumerate() {
            if i > 0 {
                final_string.push_str(", ");
            }

            if operation.operations.first().unwrap_or_else(|| &empty_operation).current_op == "count" && (column == "count(*)" || operation.operations.first().unwrap_or_else(|| &empty_operation).input_column.contains("*")) {
                
                //we know that is a count(*), so column is count(*) or the alias
                // operation.first().unwrap().input_column is * for sure

                // For COUNT(*), use the first column from the table
                let mut table_name = operation.operations.first().unwrap_or_else(|| &empty_operation).table;
                if table_name.is_empty() {
                   table_name = query_object.get_all_table_names().first().unwrap().clone();
                }
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
                    table: Some(table_name),
                    column: first_column,
                };

                final_string.push_str(&convert_column_ref(&first_col_ref, query_object));
                final_string.push_str(".unwrap()");
            }
            //case of other aggregates (no count(*))
             else {
                let new_col_ref;






                if col_ref.column.is_some() {
                    new_col_ref =
                        convert_column_ref(&col_ref.column.as_ref().unwrap(), query_object);
                    final_string.push_str(new_col_ref.as_str());
                    final_string.push_str(".unwrap()");
                } else if col_ref.aggregate.is_some() {
                    new_col_ref = convert_column_ref(
                        &col_ref.aggregate.as_ref().unwrap().column,
                        query_object,
                    );
                    final_string.push_str(new_col_ref.as_str());
                    final_string.push_str(".unwrap()");
                } else {
                    //parse literal
                    new_col_ref =
                        LiteralParser::parse_aqua_literal(&col_ref.literal.as_ref().unwrap());
                    final_string.push_str(new_col_ref.as_str());
                }
            }

            if operation.as_str() == "Avg" {
                has_avg = true;
                let new_col_ref = col_ref.aggregate.as_ref().unwrap().column.clone();
                final_string.push_str(", ");
                final_string.push_str(&convert_column_ref(&new_col_ref, query_object));
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
        final_string.push_str("))");

        // Get tuple type for fold accumulator
        let mut type_declarations = query_object
            .projections
            .iter()
            .map(|(col_ref, op)| {
                if col_ref.literal.is_some() {
                    (
                        LiteralParser::get_literal_type(&col_ref.literal.as_ref().unwrap()),
                        op.clone(),
                    )
                } else {
                    (
                        query_object.get_type(
                            &col_ref
                                .column
                                .as_ref()
                                .unwrap_or_else(|| &col_ref.aggregate.as_ref().unwrap().column),
                        ),
                        op.clone(),
                    )
                }
            })
            .collect::<Vec<(String, String)>>();

        let type_str = {
            let mut temp = String::new();
            let mut temp2: Vec<((String, String), usize)> = Vec::new();

            // Add tuple type for fold accumulator
            for i in 0..type_declarations.len() {
                // If we have an average, we need to add an extra field for the count
                if type_declarations[i].1 == "Avg" {
                    temp2.push(((type_declarations[i].0.clone(), "Avg".to_string()), i));
                }

                //if it is the first element of the tuple
                if i == 0 {
                    //case of count()
                    if type_declarations[i].1 == "Count" {
                        temp.push_str("(usize");
                    } else {
                        temp.push_str(&format!("({} ", type_declarations[i].0));
                        if type_declarations[i].1 == "Avg" {
                            temp.push_str(&format!(",{} ", type_declarations[i].0));
                        }
                    }
                }
                //otherwise we push the comma before the type
                else {
                    //case of count()
                    if type_declarations[i].1 == "Count" {
                        temp.push_str(", usize");
                    } else {
                        temp.push_str(&format!(",{} ", type_declarations[i].0));
                        if type_declarations[i].1 == "Avg" {
                            temp.push_str(&format!(",{} ", type_declarations[i].0));
                        }
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

        let is_single_avg = is_single_aggregate && type_declarations.len() > 1;

        for i in 0..type_declarations.len() {
            if i > 0 {
                final_string.push_str(", ");
            }
            //if we have a count(*), we need to initialize the accumulator with 1.0
            if type_declarations[i].1 == "Count" {
                final_string.push_str("1");
            } else {
                if is_single_aggregate && !is_single_avg {
                    final_string.push_str("x");
                } else {
                    final_string.push_str(&format!("x.{}", i));
                }
            }
        }

        final_string.push_str(")),");

        // Update values case
        final_string.push_str("\n            Some(");
        if !is_single_aggregate || is_single_avg {
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
        if !is_single_aggregate || is_single_avg {
            final_string.push(')');
        }
        final_string.push_str(") => {\n                *acc = Some(");

        if !is_single_aggregate || is_single_avg {
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

            let x_value = if is_single_aggregate && type_declarations.len() == 1 {
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
                        final_string = final_string.replace(&format!("x.{}", k), "1");
                    }
                    final_string.push_str(&format!("*acc{} + 1", k));
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
                _ =>{// Non-aggregated columns keep original value

                    //if the column is a string, we need to dereference it, clone and convert it to a string
                    if type_declarations2[i].0 == "String" {
                        final_string.push_str(&format!("acc{}.clone().to_string()", k));
                    } else {
                    final_string.push_str(&format!("*acc{}", k))}

                } 
            }
            k += 1;
        }

        if !is_single_aggregate || is_single_avg {
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
}

use crate::dsl::ir::aqua::r_utils::check_alias;
// helper function to process aggregation
fn process_aggregation(agg_func: &AggregateFunction, query_object: &QueryObject) -> String {
    let col_ref = &agg_func.column;
    let table_access = if !query_object.has_join {
        format!("x.{}.unwrap()", col_ref.column)
    } else {
        let table = col_ref.table.as_ref().unwrap();
        let table_name = check_alias(table, query_object);
        format!(
            "x{}.{}.unwrap()",
            query_object.table_to_tuple_access.get(&table_name).unwrap(),
            col_ref.column
        )
    };

    match agg_func.function {
        AggregateType::Max => format!(
            "{}::max(*acc, {})",
            query_object.get_type(col_ref),
            table_access
        ),
        AggregateType::Min => format!(
            "{}::min(*acc, {})",
            query_object.get_type(col_ref),
            table_access
        ),
        AggregateType::Sum => format!("*acc + {}", table_access),
        AggregateType::Count => "*acc + 1".to_string(),
        AggregateType::Avg => format!("*acc + {}", table_access),
    }
}

fn create_select_star_map(query_object: &QueryObject) -> String {
    let mut result = String::from(".map(|x| OutputStruct { ");

    if query_object.has_join {
        // Handle joined case - need to use tuple access
        let fields: Vec<String> = query_object.result_column_to_input.iter()
            .map(|(field_name, result_col)| {
                let op = &result_col.operations[0];
                let tuple_access = query_object.table_to_tuple_access
                    .get(&op.table)
                    .expect("Table not found in tuple access map");
                format!("{}: x{}.{}.unwrap()", field_name, tuple_access, op.input_column)
            })
            .collect();

        result.push_str(&fields.join(", "));
    } else {
        // Simple case - direct access
        let fields: Vec<String> = query_object.result_column_to_input.iter()
            .map(|(field_name, _)| format!("{}: x.{}.unwrap()", field_name, field_name))
            .collect();

        result.push_str(&fields.join(", "));
    }

    result.push_str(" })");
    result
}

fn create_simple_map(query_object: &QueryObject) -> String {
    let mut map_string = String::from(".map(|x| OutputStruct { ");
    let empty_op = Operation {
        current_op: "".to_string(),
        table: "".to_string(),
        input_column: "".to_string(),
        next_op: "".to_string(),
    };

    let fields: Vec<String> = query_object.result_column_to_input.iter()
        .map(|(field_name, result_col)| {
            //iterate now on the operations
            let op = &result_col.operations;
            
            let value = if query_object.has_join {
                let tuple_access = query_object.table_to_tuple_access
                    .get(&op.first().unwrap_or_else(|| &empty_op).table)
                    .expect("Table not found in tuple access map");
                format!("x{}.{}", tuple_access, op.first().unwrap_or_else(|| &empty_op).input_column)
            } else {
                format!("x.{}", op.first().unwrap_or_else(|| &empty_op).input_column)
            };

            let mut final_value = String::new();
            //iterate with an index from zero to the length of the operations
            for mut i in 0..op.len() {
                // Handle operations if present
                if !op[i].next_op.is_empty() {
                    match op[i].next_op.as_str() {
                        "+" | "-" | "*" | "/" => final_value.push_str(format!("{}.unwrap() {}", value, op[i].next_op,
                        ).as_str()),
                        "^" => {
                            if result_col.r_type == "i64" || result_col.r_type == "f64" {
                                final_value.push_str(format!("{}.unwrap().{}({}.unwrap())", value,
                                    if result_col.r_type == "i64" { format!("pow") }
                                    else { format!("powf") },
                                    if i + 1 < op.len() {
                                        let second_op = &op[i + 1];
                                        if query_object.has_join {
                                            let empty_string = "".to_string();
                                            format!("x{}.{}", 
                                                query_object.table_to_tuple_access.get(&second_op.table).unwrap_or_else(|| &empty_string),
                                                second_op.input_column)
                                        } else {
                                            format!("x.{}", second_op.input_column)
                                        }
                                    } else {
                                        panic!("Missing second operand for power operation");
                                    }
                                    
                                ).as_str());
                            }
                            else{
                                panic!("Invalid type for power operation");
                            }
                            i += 1;
                        },
                        _ => final_value.push_str(format!("{}.unwrap()", value).as_str()),
                    }
                } else {
                    final_value.push_str(format!("{}.unwrap()", value).as_str());
                };
            }
            format!("{}: {}", field_name, final_value)
        })
        .collect();

    map_string.push_str(&fields.join(", "));
    map_string.push_str(" })");
    map_string
}