use crate::dsl::ir::ir_ast_structure::{
    AggregateType, ComplexField, Group, GroupBaseCondition, GroupClause, NullOp,
};
use crate::dsl::ir::r_sink::{collect_sink_aggregates, process_grouping_projections};
use crate::dsl::ir::r_utils::check_alias;
use crate::dsl::ir::{AggregateFunction, BinaryOp, ComparisonOp, IrLiteral};
use crate::dsl::ir::{ColumnRef, QueryObject};
use indexmap::IndexMap;

// Base enum for tracking accumulator values
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GroupAccumulatorValue {
    Aggregate(AggregateType, ColumnRef),
}

//AccumulatorInfo to track all accumulators position in the .fold() operation
#[derive(Debug)]
pub struct GroupAccumulatorInfo {
    // Track positions of aggregates
    pub agg_positions: IndexMap<GroupAccumulatorValue, (usize, String)>, // (position, type)
}

impl GroupAccumulatorInfo {
    fn new() -> Self {
        GroupAccumulatorInfo {
            agg_positions: IndexMap::new(),
        }
    }

    fn add_aggregate(&mut self, value: GroupAccumulatorValue, val_type: String) -> usize {
        if let Some((pos, _)) = self.agg_positions.get(&value) {
            *pos
        } else {
            let pos = self.agg_positions.len();
            self.agg_positions.insert(value, (pos, val_type));
            pos
        }
    }

    // Special handling for AVG which needs both sum and count
    fn add_avg(&mut self, column: ColumnRef, val_type: String) -> (usize, usize) {
        let sum_pos = self.add_aggregate(
            GroupAccumulatorValue::Aggregate(AggregateType::Sum, column.clone()),
            val_type,
        );
        let count_pos = self.add_aggregate(
            GroupAccumulatorValue::Aggregate(AggregateType::Count, column),
            "usize".to_string(),
        );
        (sum_pos, count_pos)
    }

    fn get_agg_position(&self, agg: &AggregateFunction) -> usize {
        let col = &agg.column;
        let agg_value = GroupAccumulatorValue::Aggregate(agg.function.clone(), col.clone());
        if agg.function == AggregateType::Avg {
            let sum_col = GroupAccumulatorValue::Aggregate(AggregateType::Sum, col.clone());
            if let Some((pos, _)) = self.agg_positions.get(&sum_col) {
                *pos
            } else {
                panic!("Aggregate {:?} not found in accumulator", sum_col);
            }
        } else if let Some((pos, _)) = self.agg_positions.get(&agg_value) {
            *pos
        } else {
            panic!("Aggregate {:?} not found in accumulator", agg_value);
        }
    }
}

/// Process the GroupByClause from Ir AST and generate the corresponding Renoir operator string.
///
/// # Arguments
///
/// * `group_by` - The GroupByClause from the Ir AST containing group by columns and having conditions
/// * `query_object` - The QueryObject containing metadata about tables and columns
///
/// # Returns
///
/// A String containing the Renoir operator chain for the group by operation
pub fn process_group_by(group_by: &Group, query_object: &QueryObject) -> String {
    let mut group_string = String::new();
    let table_names = query_object.get_all_table_names();

    // Validate GROUP BY columns
    for col in &group_by.columns {
        if query_object.has_join {
            let table = col
                .table
                .as_ref()
                .expect("Column in GROUP BY must have table reference in JOIN query");
            let table_name = check_alias(table, query_object);
            query_object.check_column_validity(col, &table_name);
        } else {
            let table_name = table_names
                .first()
                .expect("No tables found in query object");
            query_object.check_column_validity(col, table_name);
        }
    }

    // Generate GROUP BY operation
    let group_by_keys = process_group_by_keys(&group_by.columns, query_object);
    group_string.push_str(&format!(".group_by(|x| ({}))", group_by_keys));

    // Process having conditions if present
    let mut acc_info = GroupAccumulatorInfo::new();
    if let Some(ref group_condition) = group_by.group_condition {
        // First parse conditions and collect information
        parse_group_conditions(group_condition, query_object, &mut acc_info, group_by);

        //now collect all the aggregates from the sink. We need to add them to the fold
        let sink_agg = collect_sink_aggregates(query_object);

        //insert all the aggregates from the sink into the accumulator
        sink_agg.iter().for_each(|agg| {
            let col_type = query_object.get_type(&agg.column);
            let agg_value =
                GroupAccumulatorValue::Aggregate(agg.function.clone(), agg.column.clone());
            if agg.function == AggregateType::Avg {
                acc_info.add_avg(agg.column.clone(), col_type);
            } else if agg.function == AggregateType::Count {
                acc_info.add_aggregate(agg_value, "usize".to_string());
            } else {
                acc_info.add_aggregate(agg_value, col_type);
            }
        });

        // Then generate operations using the collected information
        group_string.push_str(&create_fold_operation(&acc_info, group_by, query_object));

        group_string.push_str(&create_filter_operation(
            group_condition,
            group_by,
            query_object,
            &acc_info,
        ));

        // Process select clauses, keeping in mind the grouping
        group_string.push_str(&process_grouping_projections(query_object, &acc_info));
    } else {
        //now collect all the aggregates from the sink. We need to add them to the fold
        let sink_agg = collect_sink_aggregates(query_object);

        //insert all the aggregates from the sink into the accumulator
        sink_agg.iter().for_each(|agg| {
            let col_type = query_object.get_type(&agg.column);
            let agg_value =
                GroupAccumulatorValue::Aggregate(agg.function.clone(), agg.column.clone());
            if agg.function == AggregateType::Avg {
                let _ = acc_info.add_avg(agg.column.clone(), col_type);
            } else if agg.function == AggregateType::Count {
                let _ = acc_info.add_aggregate(agg_value, "usize".to_string());
            } else {
                let _ = acc_info.add_aggregate(agg_value, col_type);
            }
        });
        // Then generate operations using the collected information
        group_string.push_str(&create_fold_operation(&acc_info, group_by, query_object));
        // Process select clauses, keeping in mind the grouping
        group_string.push_str(&process_grouping_projections(query_object, &acc_info));
    }

    group_string.push_str(".drop_key()");
    group_string
}

/// Process the group by keys and generate the corresponding tuple of column references.
///
/// # Arguments
///
/// * `columns` - Vector of ColumnRef representing the group by columns
/// * `query_object` - The QueryObject containing metadata about tables and columns
///
/// # Returns
///
/// A String containing the tuple of column references for group by
fn process_group_by_keys(columns: &Vec<ColumnRef>, query_object: &QueryObject) -> String {
    if !query_object.has_join {
        // No joins - simple reference to columns
        columns
            .iter()
            .map(|col| {
                query_object.check_column_validity(col, &String::new());
                format!("x.{}.clone()", col.column)
            })
            .collect::<Vec<_>>()
            .join(", ")
    } else {
        // With joins - need to handle tuple access
        columns
            .iter()
            .map(|col| {
                let table = col.table.as_ref().unwrap();
                let table_name = check_alias(table, query_object);
                query_object.check_column_validity(col, &table_name);
                format!(
                    "x{}.{}.clone()",
                    query_object.table_to_tuple_access.get(&table_name).unwrap(),
                    col.column
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////
/// //Logic to handle the .fold() operation

// New function to recursively collect all aggregates from conditions
// Function to parse group conditions and collect necessary information
fn parse_group_conditions(
    condition: &GroupClause,
    query_object: &QueryObject,
    acc_info: &mut GroupAccumulatorInfo,
    group_by: &Group,
) {
    // Collect and validate aggregates in the condition
    match condition {
        GroupClause::Base(base_cond) => {
            match base_cond {
                GroupBaseCondition::Comparison(comp) => {
                    // Process both sides of comparison
                    collect_field_aggregates(&comp.left_field, acc_info, query_object, group_by);
                    collect_field_aggregates(&comp.right_field, acc_info, query_object, group_by);

                    // Type check the comparison
                    let left_type = query_object.get_complex_field_type(&comp.left_field);
                    let right_type = query_object.get_complex_field_type(&comp.right_field);

                    // Validate types are compatible for comparison
                    if left_type != right_type {
                        if !((left_type == "f64" || left_type == "i64" || left_type == "usize")
                            && (right_type == "f64"
                                || right_type == "i64"
                                || right_type == "usize"))
                        {
                            panic!(
                                "Invalid comparison between incompatible types: {} and {}",
                                left_type, right_type
                            );
                        }
                    }
                }
                GroupBaseCondition::NullCheck(null_check) => {
                    collect_field_aggregates(&null_check.field, acc_info, query_object, group_by);
                }
            }
        }
        GroupClause::Expression { left, op: _, right } => {
            // Recursively process both sides of the expression
            parse_group_conditions(left, query_object, acc_info, group_by);
            parse_group_conditions(right, query_object, acc_info, group_by);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////

// Helper function to collect aggregates from a ComplexField
fn collect_field_aggregates(
    field: &ComplexField,
    acc_info: &mut GroupAccumulatorInfo,
    query_object: &QueryObject,
    group_by: &Group,
) {
    match field {
        ComplexField {
            column_ref: Some(col),
            ..
        } => {
            // Validate that the column is either in GROUP BY or used in aggregate
            if !group_by.columns.iter().any(|c| c.column == col.column) {
                panic!(
                    "Column {} must appear in GROUP BY or be used in aggregate function",
                    col.column
                );
            }
        }
        ComplexField {
            aggregate: Some(agg),
            ..
        } => {
            // Process aggregate function
            let col_type = query_object.get_type(&agg.column);
            match agg.function {
                AggregateType::Avg => {
                    acc_info.add_avg(agg.column.clone(), col_type);
                }
                AggregateType::Count => {
                    acc_info.add_aggregate(
                        GroupAccumulatorValue::Aggregate(agg.function.clone(), agg.column.clone()),
                        "usize".to_string(),
                    );
                }
                _ => {
                    acc_info.add_aggregate(
                        GroupAccumulatorValue::Aggregate(agg.function.clone(), agg.column.clone()),
                        col_type,
                    );
                }
            }
        }
        ComplexField {
            nested_expr: Some(nested),
            ..
        } => {
            // Process nested expressions recursively
            let (left, op, right) = &**nested;

            collect_field_aggregates(left, acc_info, query_object, group_by);
            collect_field_aggregates(right, acc_info, query_object, group_by);

            // Validate operation types
            let left_type = query_object.get_complex_field_type(left);
            let right_type = query_object.get_complex_field_type(right);

            // Check arithmetic operations are only performed on numeric types
            if op == "+" || op == "-" || op == "*" || op == "/" || op == "^" {
                if left_type != "f64" && left_type != "i64" {
                    panic!(
                        "Invalid arithmetic operation on non-numeric type: {}",
                        left_type
                    );
                }
                if right_type != "f64" && right_type != "i64" {
                    panic!(
                        "Invalid arithmetic operation on non-numeric type: {}",
                        right_type
                    );
                }
            }
        }
        ComplexField {
            literal: Some(_), ..
        } => {
            // Literals don't need special processing for aggregates
        }
        _ => panic!("Invalid ComplexField - no valid content"),
    }
}

// Function to create fold operation if needed
fn create_fold_operation(
    acc_info: &GroupAccumulatorInfo,
    _group_by: &Group,
    query_object: &QueryObject,
) -> String {
    let mut tuple_types = Vec::new();
    let mut tuple_inits = Vec::new();
    let mut update_code = String::new();

    //if there are no aggregates, return empty string
    if acc_info.agg_positions.is_empty() {
        return "".to_string();
    }

    let single_agg = acc_info.agg_positions.len() == 1;

    println!("acc_info: {:?}", acc_info.agg_positions);

    // First add types and initializers for regular columns and aggregates
    for (value, (pos, val_type)) in &acc_info.agg_positions {
        match value {
            GroupAccumulatorValue::Aggregate(agg_type, _) => {
                match agg_type {
                    AggregateType::Max | AggregateType::Min | AggregateType::Sum => {
                        // These will be Option types
                        let actual_type = match (agg_type, val_type.as_str()) {
                            (AggregateType::Max | AggregateType::Min, "i64") => {
                                "Option<f64>".to_string()
                            }
                            _ => format!("Option<{}>", val_type),
                        };
                        tuple_types.push(actual_type);
                        tuple_inits.push("None".to_string());
                    }
                    AggregateType::Count => {
                        // Count stays as is
                        tuple_types.push(val_type.clone());
                        match val_type.as_str() {
                            "f64" => tuple_inits.push("0.0".to_string()),
                            "i64" | "usize" => tuple_inits.push("0".to_string()),
                            _ => panic!("Unsupported type for Count: {}", val_type),
                        }
                    }
                    AggregateType::Avg => {
                        // Avg is handled through Sum and Count
                        tuple_types.push(val_type.clone());
                        tuple_inits.push("0.0".to_string());
                    }
                }

                // Generate update code
                match value {
                    GroupAccumulatorValue::Aggregate(agg_type, col) => {
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
                                    update_code.push_str(&format!(
                                        "{}acc{} += 1;\n",
                                        if !single_agg {
                                            String::from("")
                                        } else {
                                            String::from("*")
                                        },
                                        if single_agg {
                                            String::from("")
                                        } else {
                                            format!(".{}", pos)
                                        }
                                    ));
                                } else {
                                    update_code.push_str(&format!(
                                        "    if {}.is_some() {{ {}acc{} += 1; }}\n",
                                        col_access,
                                        if !single_agg {
                                            String::from("")
                                        } else {
                                            String::from("*")
                                        },
                                        if single_agg {
                                            String::from("")
                                        } else {
                                            format!(".{}", pos)
                                        }
                                    ));
                                }
                            }
                            AggregateType::Sum => {
                                update_code.push_str(&format!(
                                    "    if let Some(val) = {} {{ 
                                        {}acc{} = Some({}acc{}.unwrap_or(0.0) + val);
                                    }}\n",
                                    col_access,
                                    if !single_agg {
                                        String::from("")
                                    } else {
                                        String::from("*")
                                    },
                                    if single_agg {
                                        String::from("")
                                    } else {
                                        format!(".{}", pos)
                                    },
                                    if !single_agg {
                                        String::from("")
                                    } else {
                                        String::from("*")
                                    },
                                    if single_agg {
                                        String::from("")
                                    } else {
                                        format!(".{}", pos)
                                    }
                                ));
                            }
                            AggregateType::Max => {
                                update_code.push_str(&format!(
                                    "    if let Some(val) = {} {{
                                        {}acc{} = Some(match {}acc{} {{
                                            Some(current_max) => current_max.max(val as f64),
                                            None => val as f64
                                        }});
                                    }}\n",
                                    col_access,
                                    if !single_agg {
                                        String::from("")
                                    } else {
                                        String::from("*")
                                    },
                                    if single_agg {
                                        String::from("")
                                    } else {
                                        format!(".{}", pos)
                                    },
                                    if !single_agg {
                                        String::from("")
                                    } else {
                                        String::from("*")
                                    },
                                    if single_agg {
                                        String::from("")
                                    } else {
                                        format!(".{}", pos)
                                    }
                                ));
                            }
                            AggregateType::Min => {
                                update_code.push_str(&format!(
                                    "    if let Some(val) = {} {{
                                        {}acc{} = Some(match {}acc{} {{
                                            Some(current_min) => current_min.min(val as f64),
                                            None => val as f64
                                        }});
                                    }}\n",
                                    col_access,
                                    if !single_agg {
                                        String::from("")
                                    } else {
                                        String::from("*")
                                    },
                                    if single_agg {
                                        String::from("")
                                    } else {
                                        format!(".{}", pos)
                                    },
                                    if !single_agg {
                                        String::from("")
                                    } else {
                                        String::from("*")
                                    },
                                    if single_agg {
                                        String::from("")
                                    } else {
                                        format!(".{}", pos)
                                    }
                                ));
                            }
                            AggregateType::Avg => {} // Handled through Sum and Count
                        }
                    }
                }
            }
        }
    }

    // Generate the fold string
    let tuple_type = format!("({})", tuple_types.join(", "));
    let tuple_init = format!("({})", tuple_inits.join(", "));

    let mut fold_str = format!(".fold({}, |acc: &mut {}, x| {{\n", tuple_init, tuple_type);
    fold_str.push_str(&update_code);
    fold_str.push_str("\n})\n");

    fold_str
}

//////////////////////////////////////////////////////////////////////////
/// Logic for the .filter()

// Function to create the filter operation
fn create_filter_operation(
    condition: &GroupClause,
    group_by: &Group,
    query_object: &QueryObject,
    acc_info: &GroupAccumulatorInfo,
) -> String {
    let mut filter_str = String::new();
    filter_str.push_str(".filter(|x| ");

    // Process the conditions recursively
    filter_str.push_str(&process_filter_condition(
        condition,
        group_by,
        query_object,
        acc_info,
    ));

    filter_str.push_str(")");
    filter_str
}

// Function to process filter conditions recursively
fn process_filter_condition(
    condition: &GroupClause,
    group_by: &Group,
    query_object: &QueryObject,
    acc_info: &GroupAccumulatorInfo,
) -> String {
    let mut check_list: Vec<String> = Vec::new();
    match condition {
        GroupClause::Base(base_condition) => {
            match base_condition {
                GroupBaseCondition::Comparison(comp) => {
                    let operator = match comp.operator {
                        ComparisonOp::GreaterThan => ">",
                        ComparisonOp::LessThan => "<",
                        ComparisonOp::Equal => "==",
                        ComparisonOp::GreaterThanEquals => ">=",
                        ComparisonOp::LessThanEquals => "<=",
                        ComparisonOp::NotEqual => "!=",
                    };

                    // Get types for both sides of comparison
                    let left_type = query_object.get_complex_field_type(&comp.left_field);
                    let right_type = query_object.get_complex_field_type(&comp.right_field);

                    // Process left and right expressions
                    let left_expr = process_filter_field(
                        &comp.left_field,
                        group_by,
                        query_object,
                        acc_info,
                        &mut check_list,
                    );
                    let right_expr = process_filter_field(
                        &comp.right_field,
                        group_by,
                        query_object,
                        acc_info,
                        &mut check_list,
                    );

                    let is_check_list_empty = check_list.is_empty(); // if true there is only one or more count
                                                                     // Deduplicate and the check list
                    check_list.sort();
                    check_list.dedup();

                    // Handle type conversions for comparison - improved handling for numeric types
                    if left_type != right_type {
                        if (left_type == "f64" || left_type == "i64" || left_type == "usize")
                            && (right_type == "f64" || right_type == "i64" || right_type == "usize")
                        {
                            if is_check_list_empty {
                                format!(
                                    "({} as f64) {} ({} as f64)",
                                    left_expr, operator, right_expr
                                )
                            } else {
                                // Cast both to f64
                                format!(
                                    "if {} {{({} as f64) {} ({} as f64)}} else {{ false }}",
                                    check_list.join(" && "),
                                    left_expr,
                                    operator,
                                    right_expr
                                )
                            }
                        } else {
                            if is_check_list_empty {
                                format!("{} {} {}", left_expr, operator, right_expr)
                            } else {
                                // Different non-numeric types - this should already be caught during validation
                                format!(
                                    "if {} {{({}) {} ({})}} else {{ false }}",
                                    check_list.join(" && "),
                                    left_expr,
                                    operator,
                                    right_expr
                                )
                            }
                        }
                    } else {
                        if is_check_list_empty {
                            format!("{} {} {}", left_expr, operator, right_expr)
                        } else {
                            // Same types
                            format!(
                                "if {} {{({}) {} ({})}} else {{ false }}",
                                check_list.join(" && "),
                                left_expr,
                                operator,
                                right_expr
                            )
                        }
                    }
                }
                GroupBaseCondition::NullCheck(null_check) => {
                    // Get the column reference that's being checked for null
                    let col_ref = if let Some(ref col) = null_check.field.column_ref {
                        col
                    } else {
                        panic!("NULL check must be on a column reference");
                    };

                    // Check if this column is part of the GROUP BY key
                    let is_key_field = group_by.columns.iter().any(|c| {
                        c.column == col_ref.column
                            && (c.table.is_none()
                                || col_ref.table.is_none()
                                || c.table == col_ref.table)
                    });

                    // Get column access based on whether it's a key field
                    let col_access = if is_key_field {
                        // Get the position in the group by key tuple
                        let key_position = group_by
                            .columns
                            .iter()
                            .position(|c| {
                                c.column == col_ref.column
                                    && (c.table.is_none()
                                        || col_ref.table.is_none()
                                        || c.table == col_ref.table)
                            })
                            .unwrap();

                        if group_by.columns.len() == 1 {
                            // Single key column
                            "x.0".to_string()
                        } else {
                            // Multiple key columns - access by position
                            format!("x.0.{}", key_position)
                        }
                    } else {
                        // Not a key column - must be in the accumulated values or aggregates
                        if query_object.has_join {
                            let table = check_alias(&col_ref.table.as_ref().unwrap(), query_object);
                            format!(
                                "x.1{}.{}",
                                query_object.table_to_tuple_access.get(&table).unwrap(),
                                col_ref.column
                            )
                        } else {
                            format!("x.1.{}", col_ref.column)
                        }
                    };

                    // Generate the appropriate null check
                    match null_check.operator {
                        NullOp::IsNull => format!("{}.is_none()", col_access),
                        NullOp::IsNotNull => format!("{}.is_some()", col_access),
                    }
                }
            }
        }
        GroupClause::Expression { left, op, right } => {
            let op_str = match op {
                BinaryOp::And => "&&",
                BinaryOp::Or => "||",
            };

            format!(
                "({} {} {})",
                process_filter_condition(left, group_by, query_object, acc_info),
                op_str,
                process_filter_condition(right, group_by, query_object, acc_info)
            )
        }
    }
}

// Helper function to process fields in filter conditions
fn process_filter_field(
    field: &ComplexField,
    group_by: &Group,
    query_object: &QueryObject,
    acc_info: &GroupAccumulatorInfo,
    mut check_list: &mut Vec<String>, // Added parameter
) -> String {
    if let Some(ref nested) = field.nested_expr {
        let (left, op, right) = &**nested;

        let left_type = query_object.get_complex_field_type(left);
        let right_type = query_object.get_complex_field_type(right);

        let left_expr =
            process_filter_field(left, group_by, query_object, acc_info, &mut check_list);
        let right_expr =
            process_filter_field(right, group_by, query_object, acc_info, &mut check_list);

        // Improved type handling for arithmetic operations
        if left_type != right_type {
            if (left_type == "f64" || left_type == "i64")
                && (right_type == "f64" || right_type == "i64")
            {
                // Division always results in f64
                if op == "/" {
                    return format!("({} as f64) {} ({} as f64)", left_expr, op, right_expr);
                }

                // Special handling for power operation (^)
                if op == "^" {
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

                // Add proper type conversion for other operations
                if left_type == "i64" && right_type == "f64" {
                    return format!("({} as f64) {} {}", left_expr, op, right_expr);
                } else if left_type == "f64" && right_type == "i64" {
                    return format!("{} {} ({} as f64)", left_expr, op, right_expr);
                }
            }
        } else {
            // Same types
            if op == "/" {
                return format!("({} as f64) {} ({} as f64)", left_expr, op, right_expr);
            } else if op == "^" {
                if left_type == "f64" {
                    return format!("({}).powf({})", left_expr, right_expr);
                } else {
                    return format!("({}).pow({} as u32)", left_expr, right_expr);
                }
            }
        }

        format!("({} {} {})", left_expr, op, right_expr)
    } else if let Some(ref col) = field.column_ref {
        //get type
        let as_ref = if query_object.get_type(col) == "String" {
            ".as_ref()"
        } else {
            ""
        };
        // Handle column reference - check if it's a key or not
        if let Some(key_position) = group_by.columns.iter().position(|c| c.column == col.column) {
            // It's a key - use its position in the group by tuple
            if group_by.columns.len() == 1 {
                check_list.push(format!("x.0{}.is_some()", as_ref));
                format!("x.0{}.unwrap()", as_ref)
            } else {
                check_list.push(format!("x.0.{}{}.is_some()", key_position, as_ref));
                format!("x.0.{}{}.unwrap()", key_position, as_ref)
            }
        } else {
            // Not a key - use x.1
            if query_object.has_join {
                let table = check_alias(&col.table.as_ref().unwrap(), query_object);

                check_list.push(format!(
                    "x.1{}.{}{}.is_some()",
                    query_object.table_to_tuple_access.get(&table).unwrap(),
                    col.column,
                    as_ref
                ));

                format!(
                    "x.1{}.{}{}.unwrap()",
                    query_object.table_to_tuple_access.get(&table).unwrap(),
                    col.column,
                    as_ref
                )
            } else {
                check_list.push(format!("x.1.{}{}.is_some()", col.column, as_ref));
                format!("x.1.{}{}.unwrap()", col.column, as_ref)
            }
        }
    } else if let Some(ref lit) = field.literal {
        match lit {
            IrLiteral::Integer(i) => i.to_string(),
            IrLiteral::Float(f) => format!("{:.2}", f),
            IrLiteral::String(s) => format!("\"{}\"", s),
            IrLiteral::Boolean(b) => b.to_string(),
            IrLiteral::ColumnRef(col_ref) => {
                // Check if it's a key and get its position
                if let Some(key_position) = group_by
                    .columns
                    .iter()
                    .position(|c| c.column == col_ref.column)
                {
                    if group_by.columns.len() == 1 {
                        format!("x.0")
                    } else {
                        format!("x.0.{}", key_position)
                    }
                } else {
                    if query_object.has_join {
                        let table = check_alias(&col_ref.table.as_ref().unwrap(), query_object);
                        format!(
                            "x.1{}.{}.unwrap()",
                            query_object.table_to_tuple_access.get(&table).unwrap(),
                            col_ref.column
                        )
                    } else {
                        format!("x.1.{}.unwrap()", col_ref.column)
                    }
                }
            }
        }
    } else if let Some(ref agg) = field.aggregate {
        //retrive aggregate position from the accumulator
        let agg_pos = acc_info.get_agg_position(agg);
        // Aggregates are always in x.1
        let col = &agg.column;
        let col_access = if acc_info.agg_positions.len() == 1 {
            format!("x.1")
        } else {
            format!("x.1.{}", agg_pos)
        };

        if agg.function != AggregateType::Count {
            check_list.push(format!("{}.is_some()", col_access));
        }

        match agg.function {
            AggregateType::Count => {
                
                    format!("{}", col_access)
                
            }
            AggregateType::Max | AggregateType::Min | AggregateType::Sum => {
                format!("{}.unwrap()", col_access)
            }
            AggregateType::Avg => {
                //get the sum and count positions. Sum position corresponds to the position of the aggregate in the accumulator
                let count_pos = acc_info.get_agg_position(&AggregateFunction {
                    function: AggregateType::Count,
                    column: col.clone(),
                });
                format!(
                    "{}.unwrap() / {} as f64",
                    col_access,
                    format!("x.1.{}", count_pos)
                )
            }
        }
    } else {
        panic!("Invalid ComplexField - no valid content")
    }
}
