use core::panic;

use crate::dsl::ir::ir_ast_structure::{ComplexField, SelectColumn};
use crate::dsl::ir::r_group::{GroupAccumulatorInfo, GroupAccumulatorValue};
use crate::dsl::ir::r_utils::check_alias;
use crate::dsl::ir::AggregateFunction;
use crate::dsl::ir::{AggregateType, ColumnRef, IrLiteral};
use crate::dsl::struct_object::object::QueryObject;
use indexmap::IndexMap;

// struct to store the accumulator value
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum AccumulatorValue {
    Aggregate(AggregateType, ColumnRef),
    Column(ColumnRef),
}

#[derive(Debug)]
struct AccumulatorInfo {
    value_positions: IndexMap<AccumulatorValue, (usize, String)>, // (position, type)
}

impl AccumulatorInfo {
    fn new() -> Self {
        AccumulatorInfo {
            value_positions: IndexMap::new(),
        }
    }

    fn add_value(&mut self, value: AccumulatorValue, val_type: String) -> usize {
        if let Some((pos, _)) = self.value_positions.get(&value) {
            *pos
        } else {
            let pos = self.value_positions.len();
            self.value_positions.insert(value, (pos, val_type));
            pos
        }
    }

    fn add_avg(&mut self, column: ColumnRef, val_type: String) -> (usize, usize) {
        let sum_pos = self.add_value(
            AccumulatorValue::Aggregate(AggregateType::Sum, column.clone()),
            val_type,
        );
        let count_pos = self.add_value(
            AccumulatorValue::Aggregate(AggregateType::Count, column),
            "usize".to_string(),
        );
        (sum_pos, count_pos)
    }
}

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

pub fn process_select_clauses(
    select_clauses: &Vec<SelectColumn>,
    query_object: &mut QueryObject,
) -> String {
    // Check for SELECT * case
    if select_clauses.len() == 1 {
        match &select_clauses[0] {
            SelectColumn::Column(col_ref, _) if col_ref.column == "*" => {
                return create_select_star_map(query_object);
            }
            _ => {}
        }
    }
    // Check if any aggregations are present using recursive traversal
    let has_aggregates: bool = select_clauses.iter().any(|clause| match clause {
        SelectColumn::Aggregate(_, _) => true,
        SelectColumn::ComplexValue(field, _) => has_aggregate_in_complex_field(field),
        _ => false,
    });

    if has_aggregates {
        create_aggregate_map(select_clauses, query_object)
    } else {
        create_simple_map(select_clauses, query_object)
    }
}

// function to create aggregate fold and map
fn create_aggregate_map(select_clauses: &Vec<SelectColumn>, query_object: &QueryObject) -> String {
    let mut acc_info = AccumulatorInfo::new();
    let mut result = String::new();

    let mut check_list = Vec::new();

    // First analyze all clauses to build accumulator info
    for (i, clause) in select_clauses.iter().enumerate() {
        let result_type = query_object.result_column_types.get_index(i).unwrap().1;
        match clause {
            SelectColumn::Aggregate(agg, _) => match agg.function {
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
            SelectColumn::ComplexValue(field, _) => {
                process_complex_field_for_accumulator(
                    field,
                    &mut acc_info,
                    query_object,
                    &mut check_list,
                );
            }
            SelectColumn::Column(col, _) => {
                acc_info.add_value(AccumulatorValue::Column(col.clone()), result_type.clone());
            }
        }
    }

    // Initialize the fold accumulator with correct types and initial values
    let mut tuple_types = Vec::new();
    let mut tuple_inits = Vec::new();

    println!("acc_info: {:?}", acc_info);

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
                // Regular columns stay the same
                match val_type.as_str() {
                    "f64" => tuple_inits.push("0.0".to_string()),
                    "i64" => tuple_inits.push("0".to_string()),
                    "usize" => tuple_inits.push("0".to_string()),
                    "bool" => tuple_inits.push("false".to_string()),
                    "String" => tuple_inits.push("String::new()".to_string()),
                    _ => panic!("Unsupported type: {}", val_type),
                }
                tuple_types.push(val_type.clone());
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
                let col_access = if query_object.has_join {
                    let table = col.table.as_ref().unwrap();
                    let table_name = check_alias(table, query_object);
                    format!(
                        "x{}.{}",
                        query_object.table_to_tuple_access.get(&table_name).unwrap(),
                        col.column
                    )
                } else {
                    format!("x.{}", col.column)
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
                let col_access = if query_object.has_join {
                    let table = col.table.as_ref().unwrap();
                    let table_name = check_alias(table, query_object);
                    format!(
                        "x{}.{}",
                        query_object.table_to_tuple_access.get(&table_name).unwrap(),
                        col.column
                    )
                } else {
                    format!("x.{}", col.column)
                };

                update_code.push_str(&format!(
                    "    if let Some(val) = {} {{ acc{} = val; }}\n",
                    col_access, index_acc
                ));
            }
        }
    }

    result.push_str(&update_code);
    result.push_str("})\n");

    // Generate final map to OutputStruct
    result.push_str(".map(|acc| OutputStruct {\n");

    for (i, clause) in select_clauses.iter().enumerate() {
        check_list.clear();
        let field_name = query_object.result_column_types.get_index(i).unwrap().0;
        let value = match clause {
            SelectColumn::Aggregate(agg, _) => {
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
                        format!("if let Some(sum) = acc.{} {{ Some(sum as f64 / acc.{} as f64) }} else {{ None }}", 
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
                            format!("Some(acc)")
                        } else {
                            format!("Some(acc.{})", pos)
                        }
                    }
                }
            }
            SelectColumn::ComplexValue(field, _) => {
                let temp = process_complex_field_for_accumulator(
                    field,
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
            SelectColumn::Column(col, _) => {
                let pos = acc_info
                    .value_positions
                    .get(&AccumulatorValue::Column(col.clone()))
                    .unwrap()
                    .0;
                format!("Some(acc.{})", pos)
            }
        };
        result.push_str(&format!("    {}: {},\n", field_name, value));
    }

    result.push_str("})");
    result
}

fn process_complex_field_for_accumulator(
    field: &ComplexField,
    acc_info: &mut AccumulatorInfo,
    query_object: &QueryObject,
    check_list: &mut Vec<String>,
) -> String {
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
                            acc_info,
                            query_object,
                            check_list
                        ),
                        op,
                        process_complex_field_for_accumulator(
                            right,
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
                        acc_info,
                        query_object,
                        check_list,
                    );
                    let right_expr = process_complex_field_for_accumulator(
                        right,
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
                    process_complex_field_for_accumulator(left, acc_info, query_object, check_list);
                let right_expr = process_complex_field_for_accumulator(
                    right,
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
                    process_complex_field_for_accumulator(left, acc_info, query_object, check_list),
                    op,
                    process_complex_field_for_accumulator(
                        right,
                        acc_info,
                        query_object,
                        check_list
                    )
                );
            }

            // Special handling for power operation (^)
            if op == "^" {
                let left_expr =
                    process_complex_field_for_accumulator(left, acc_info, query_object, check_list);
                let right_expr = process_complex_field_for_accumulator(
                    right,
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
                process_complex_field_for_accumulator(left, acc_info, query_object, check_list),
                op,
                process_complex_field_for_accumulator(right, acc_info, query_object, check_list)
            )
        }
    } else if let Some(ref col) = field.column_ref {
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

fn create_select_star_map(query_object: &QueryObject) -> String {
    let mut result = String::from(".map(|x| OutputStruct { ");

    if query_object.has_join {
        // Handle joined case - need to use tuple access
        let tables = query_object.get_all_table_names();
        let empty_string = "".to_string();

        //for table in tables, build all the columns mapping in the .map
        let mut offset: usize = 0;

        for table_index in 0..tables.len() {
            let table = &tables[table_index];
            let tuple_access = query_object
                .table_to_tuple_access
                .get(table)
                .unwrap_or_else(|| &empty_string);
            let table_struct = query_object.table_to_struct.get(table).unwrap();

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
            .table_to_struct
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

fn create_simple_map(select_clauses: &Vec<SelectColumn>, query_object: &QueryObject) -> String {
    let mut map_string = String::from(".map(|x| OutputStruct { ");
    let empty_string = "".to_string();

    let mut check_list = Vec::new();

    let fields: Vec<String> = select_clauses
        .iter()
        .enumerate() // Add enumerate to track position
        .map(|(i, clause)| {
            match clause {
                SelectColumn::Column(col_ref, _) => {
                    let field_name = query_object
                        .result_column_types
                        .get_index(i)
                        .unwrap_or_else(|| (&empty_string, &empty_string))
                        .0;
                    let value = if query_object.has_join {
                        let table = check_alias(&col_ref.table.as_ref().unwrap(), query_object);
                        let tuple_access = query_object
                            .table_to_tuple_access
                            .get(&table)
                            .expect("Table not found in tuple access map");
                        format!("x{}.{}", tuple_access, col_ref.column)
                    } else {
                        format!("x.{}", col_ref.column)
                    };
                    format!("{}: {}", field_name, value)
                }
                SelectColumn::ComplexValue(complex_field, alias) => {
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
        let empty_string = String::new();
        if query_object.has_join {
            let table = col.table.as_ref().unwrap_or_else(|| &empty_string);
            let table = &check_alias(&table, query_object);

            let tuple_access = query_object
                .table_to_tuple_access
                .get(table)
                .expect("Table not found in tuple access map");

            check_list.push(format!("x{}.{}.is_some()", tuple_access, col.column));
            format!("x{}.{}.unwrap()", tuple_access, col.column)
        } else {
            check_list.push(format!("x.{}.is_some()", col.column));
            format!("x.{}.unwrap()", col.column)
        }
    } else if let Some(ref lit) = field.literal {
        // Handle literal value
        match lit {
            IrLiteral::Integer(i) => i.to_string(),
            IrLiteral::Float(f) => format!("{:.2}", f),
            IrLiteral::String(s) => format!("\"{}\"", s),
            IrLiteral::Boolean(b) => b.to_string(),
            IrLiteral::ColumnRef(col_ref) => {
                if query_object.has_join {
                    let table = col_ref.table.as_ref().unwrap();
                    let tuple_access = query_object
                        .table_to_tuple_access
                        .get(table)
                        .expect("Table not found in tuple access map");
                    format!("x{}.{}.unwrap()", tuple_access, col_ref.column)
                } else {
                    format!("x.{}.unwrap()", col_ref.column)
                }
            }
        }
    } else {
        panic!("Invalid ComplexField - no valid content");
    }
}
// Recursive function to check for aggregates in ComplexField
fn has_aggregate_in_complex_field(field: &ComplexField) -> bool {
    // Check if this field has an aggregate
    if field.aggregate.is_some() {
        return true;
    }

    // Recursively check nested expressions
    if let Some(nested) = &field.nested_expr {
        let (left, _, right) = &**nested;
        // Check both sides of the nested expression
        return has_aggregate_in_complex_field(left) || has_aggregate_in_complex_field(right);
    }

    false
}

pub fn collect_sink_aggregates(query_object: &QueryObject) -> Vec<AggregateFunction> {
    let mut aggregates = Vec::new();

    for clause in query_object.ir_ast.clone().unwrap().select.select {
        match clause {
            SelectColumn::Aggregate(agg, _) => {
                aggregates.push(AggregateFunction {
                    function: agg.function.clone(),
                    column: agg.column.clone(),
                });
            }
            SelectColumn::ComplexValue(field, _) => {
                collect_aggregates_in_complex_field(&field, &mut aggregates);
            }
            _ => {}
        }
    }

    aggregates
}

fn collect_aggregates_in_complex_field(
    field: &ComplexField,
    aggregates: &mut Vec<AggregateFunction>,
) {
    if let Some(ref nested) = field.nested_expr {
        let (left, _, right) = &**nested;
        collect_aggregates_in_complex_field(left, aggregates);
        collect_aggregates_in_complex_field(right, aggregates);
    } else if let Some(ref agg) = field.aggregate {
        aggregates.push(AggregateFunction {
            function: agg.function.clone(),
            column: agg.column.clone(),
        });
    }
}

///////////////////////////////////////////////////////////////////
/// Logic to process the select in the case of group by

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
