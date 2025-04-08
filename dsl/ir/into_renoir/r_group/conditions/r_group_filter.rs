use core::panic;

use crate::dsl::ir::ir_ast_structure::{
    AggregateType, ComplexField, GroupBaseCondition, GroupClause, NullOp,
};
use crate::dsl::ir::r_group::r_group_keys::GroupAccumulatorInfo;
use crate::dsl::ir::r_utils::convert_literal;
use crate::dsl::ir::{AggregateFunction, BinaryOp, ComparisonOp, InCondition, IrLiteral};
use crate::dsl::ir::{ColumnRef, QueryObject};
use crate::dsl::struct_object::utils::check_column_validity;

// Function to create the filter operation
pub fn create_filter_operation(
    condition: &GroupClause,
    keys: &Vec<ColumnRef>,
    query_object: &QueryObject,
    acc_info: &GroupAccumulatorInfo,
) -> String {
    let mut filter_str = String::new();
    filter_str.push_str(".filter(move |x| ");

    // Process the conditions recursively
    filter_str.push_str(&process_filter_condition(
        condition,
        keys,
        query_object,
        acc_info,
    ));

    filter_str.push(')');

    filter_str
}

// Function to process filter conditions recursively
fn process_filter_condition(
    condition: &GroupClause,
    keys: &Vec<ColumnRef>,
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

                    println!("Left type: {}", left_type);
                    println!("Right type: {}", right_type);

                    // Process left and right expressions
                    let left_expr = process_filter_field(
                        &comp.left_field,
                        keys,
                        query_object,
                        acc_info,
                        &mut check_list,
                    );
                    let right_expr = process_filter_field(
                        &comp.right_field,
                        keys,
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
                        } else if is_check_list_empty {
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
                    } else if is_check_list_empty {
                        format!("{} {} {}", left_expr, operator, right_expr)
                    } else {
                        // Same types
                        if left_type == "i64" && right_type == "i64" {
                            format!(
                                "if {} {{({} as f64) {} ({} as f64)}} else {{ false }}",
                                check_list.join(" && "),
                                left_expr,
                                operator,
                                right_expr
                            )
                        } else {
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
                    if null_check.field.column_ref.is_some() {
                        // Get the column reference that's being checked for null
                        let col_ref = if let Some(ref col) = null_check.field.column_ref {
                            col
                        } else {
                            panic!("NULL check must be on a column reference");
                        };

                        // Check if this column is part of the GROUP BY key
                        let is_key_field = keys.iter().any(|c| {
                            c.column == col_ref.column
                                && (c.table.is_none()
                                    || col_ref.table.is_none()
                                    || c.table == col_ref.table)
                        });

                        // Get column access based on whether it's a key field
                        let col_access = if is_key_field {
                            // Get the position in the group by key tuple
                            let key_position = keys
                                .iter()
                                .position(|c| {
                                    c.column == col_ref.column
                                        && (c.table.is_none()
                                            || col_ref.table.is_none()
                                            || c.table == col_ref.table)
                                })
                                .unwrap();

                            if keys.len() == 1 {
                                // Single key column
                                "x.0".to_string()
                            } else {
                                // Multiple key columns - access by position
                                format!("x.0.{}", key_position)
                            }
                        } else {
                            // Not a key column - must be in the accumulated values or aggregates

                            let stream_name = if col_ref.table.is_some() {
                                query_object
                                    .get_stream_from_alias(col_ref.table.as_ref().unwrap())
                                    .unwrap()
                            } else if query_object.streams.len() == 1 {
                                query_object.streams.first().unwrap().0
                            } else {
                                panic!("Column reference must have a table reference")
                            };
                            let stream = query_object.get_stream(stream_name);
                            stream.check_if_column_exists(&col_ref.column);

                            format!(
                                "x.1{}.{}",
                                stream.get_access().get_base_path(),
                                col_ref.column
                            )
                        };

                        // Generate the appropriate null check
                        match null_check.operator {
                            NullOp::IsNull => format!("{}.is_none()", col_access),
                            NullOp::IsNotNull => format!("{}.is_some()", col_access),
                        }
                    } else if null_check.field.literal.is_some() {
                        let lit = null_check.field.literal.as_ref().unwrap();
                        match lit {
                            IrLiteral::Boolean(_) | IrLiteral::Integer(_) | IrLiteral::Float(_) => {
                                match null_check.operator {
                                    NullOp::IsNull => "false".to_string(),
                                    NullOp::IsNotNull => "true".to_string(),
                                }
                            }
                            IrLiteral::String(string) => match null_check.operator {
                                NullOp::IsNull => format!("{}", string.is_empty()),
                                NullOp::IsNotNull => format!("{}", !string.is_empty()),
                            },
                            IrLiteral::ColumnRef(_) => {
                                panic!("We should not be here.")
                            }
                        }
                    } else if null_check.field.aggregate.is_some() {
                        match null_check.operator {
                            NullOp::IsNull => "false".to_string(),
                            NullOp::IsNotNull => "true".to_string(),
                        }
                    } else {
                        panic!("Invalid NULL check - must be on a column reference or literal")
                    }
                }
                GroupBaseCondition::In(in_condition) => match in_condition {
                    InCondition::InOldVersion {
                        field,
                        values,
                        negated,
                    } => {
                        // Get the values
                        let values_str = values
                            .iter()
                            .map(|value| match value {
                                IrLiteral::Integer(i) => i.to_string(),
                                IrLiteral::Float(f) => format!("{:.2}", f),
                                IrLiteral::String(s) => format!("\"{}\"", s),
                                IrLiteral::Boolean(b) => b.to_string(),
                                IrLiteral::ColumnRef(_) => {
                                    panic!(
                                        "Invalid InCondition - column reference not expected here"
                                    )
                                }
                            })
                            .collect::<Vec<String>>()
                            .join(", ");

                        let col_ref = if field.column_ref.is_some() {
                            field.column_ref.as_ref().unwrap()
                        } else {
                            panic!("IN condition must be on a column reference")
                        };

                        let is_key = keys.iter().any(|k| k.column == col_ref.column);
                        let key_position = if is_key {
                            keys.iter()
                                .position(|k| k.column == col_ref.column)
                                .unwrap()
                        } else {
                            panic!("Field in IN condition must be a group by key")
                        };

                        // Generate the condition with correct tuple access
                        let single_key = keys.len() == 1;
                        let c_type = query_object.get_type(col_ref);

                        let access_str = if single_key {
                            format!("x.0{}", if c_type == "String" { ".as_ref()" } else { "" })
                        } else {
                            format!(
                                "x.0.{}{}",
                                key_position,
                                if c_type == "String" { ".as_ref()" } else { "" }
                            )
                        };

                        // Generate the final string with proper null checks
                        format!(
                                "if {}.is_some() {{{}vec![{}].contains(&{}.unwrap(){})}} else {{false}}",
                                access_str,
                                if *negated { "!" } else { "" },
                                values_str,
                                access_str,
                                if c_type == "String" { ".as_str()" } else { "" }
                                                            )
                    }
                    InCondition::InSubquery { .. } => panic!("We should not have InSubquery here"),
                    InCondition::InVec {
                        field,
                        vector_name,
                        vector_type,
                        negated,
                    } => {
                        {
                            //first case, field is a tuple with vec name and type
                            if field.subquery_vec.is_some() {
                                let (field_name, field_type) = field.subquery_vec.clone().unwrap();

                                //compare field type with vector type
                                if *field_type != *vector_type {
                                    //check if they are both numbers
                                    if (field_type == "f64" || field_type == "i64")
                                        && (*vector_type == "f64" || *vector_type == "i64")
                                    {
                                        //needs to cast the field_type to the actual vector type
                                        let cast_type =
                                            if field_type == "f64" { "i64" } else { "f64" };

                                        format!(
                                            "{}{}.contains(&{}.first().unwrap() as {})",
                                            if *negated { "!" } else { "" },
                                            vector_name,
                                            field_name,
                                            cast_type,
                                        )
                                    } else {
                                        panic!("Invalid InCondition - column type {} does not match vector type {}", field_type, vector_type);
                                    }
                                } else {
                                    //standard case

                                    // Generate the final string
                                    format!(
                                        "{}{}.contains(&{}.first().unwrap())",
                                        if *negated { "!" } else { "" },
                                        vector_name,
                                        field_name,
                                    )
                                }
                            }
                            //second case - col_ref
                            else if field.column_ref.is_some() {
                                //as for now we only handle the col_ref case

                                let col_ref = field.column_ref.clone().unwrap();
                                // Check if the field is a column reference
                                let stream_name = if col_ref.table.is_some() {
                                    query_object
                                        .get_stream_from_alias(col_ref.table.as_ref().unwrap())
                                        .unwrap()
                                } else {
                                    let all_streams = &query_object.streams;
                                    if all_streams.len() > 1 {
                                        panic!("Invalid column reference - missing table name");
                                    }
                                    all_streams.first().unwrap().0
                                };

                                // Validate column
                                check_column_validity(&col_ref, stream_name, query_object);

                                let c_type = query_object.get_type(&col_ref);

                                //we need also to check if the column is a key or not
                                let is_key = keys.iter().any(|k| k.column == col_ref.column);
                                let key_position = if is_key {
                                    keys.iter()
                                        .position(|k| k.column == col_ref.column)
                                        .unwrap()
                                } else {
                                    panic!("Field in IN condition must be a group by key")
                                };

                                // Generate the access string based on whether it's a key or not
                                let access_str = if keys.len() == 1 {
                                    "x.0".to_string()
                                } else {
                                    format!("x.0.{}", key_position,)
                                };

                                //compare column type with vector type
                                if c_type != *vector_type {
                                    //check if they are both numbers
                                    if (c_type == "f64" || c_type == "i64")
                                        && (*vector_type == "f64" || *vector_type == "i64")
                                    {
                                        //needs to cast the c_type to the actual vector type
                                        let cast_type = if c_type == "f64" { "i64" } else { "f64" };
                                        let condition_str = if cast_type == "f64" {
                                            format!(
                                                "&OrderedFloat({}{}.unwrap() as {})",
                                                access_str,
                                                if !is_key {
                                                    format!(".{}", col_ref.column)
                                                } else {
                                                    "".to_string()
                                                },
                                                cast_type
                                            )
                                        } else {
                                            format!(
                                                "{}{}.as_ref().unwrap() as {}",
                                                access_str,
                                                if !is_key {
                                                    format!(".{}", col_ref.column)
                                                } else {
                                                    "".to_string()
                                                },
                                                cast_type
                                            )
                                        };

                                        format!(
                                                "if {}{}.as_ref().is_some() {{{}{}.contains({})}} else {{false}}",
                                                access_str,
                                                if !is_key {format!(".{}", col_ref.column)} else {"".to_string()},
                                                if *negated { "!" } else { "" },
                                                vector_name,
                                                condition_str
                                                                    )
                                    } else {
                                        panic!("Invalid InCondition - column type {} does not match vector type {}", c_type, vector_type);
                                    }
                                } else {
                                    //standard case
                                    // Generate the condition
                                    let condition_str = if c_type == "f64" {
                                        format!(
                                            "&{}{}.unwrap()",
                                            access_str,
                                            if !is_key {
                                                format!(".{}", col_ref.column)
                                            } else {
                                                "".to_string()
                                            },
                                        )
                                    } else {
                                        format!(
                                            "{}{}.as_ref().unwrap()",
                                            access_str,
                                            if !is_key {
                                                format!(".{}", col_ref.column)
                                            } else {
                                                "".to_string()
                                            },
                                        )
                                    };

                                    // Generate the final string
                                    format!(
                                            "if {}{}.as_ref().is_some() {{{}{}.contains({})}} else {{false}}",
                                            access_str,
                                            if !is_key {format!(".{}", col_ref.column)} else {"".to_string()},
                                            if *negated { "!" } else { "" },
                                            vector_name,
                                            condition_str
                                                            )
                                }
                            }
                            //third - literal case
                            else if field.literal.is_some() {
                                let lit = field.literal.as_ref().unwrap();

                                match lit {
                                    IrLiteral::Boolean(_) => {
                                        if vector_type != "bool" {
                                            panic!("Invalid InCondition - boolean literal does not match vector type {}", vector_type);
                                        }
                                        format!(
                                            "{}{}.contains(&{})",
                                            if *negated { "!" } else { "" },
                                            vector_name,
                                            convert_literal(lit)
                                        )
                                    }
                                    IrLiteral::Float(_) | IrLiteral::Integer(_) => {
                                        let literal_type = if let IrLiteral::Float(_) = lit {
                                            "f64"
                                        } else {
                                            "i64"
                                        };
                                        if vector_type != "f64" && vector_type != "i64" {
                                            panic!("Invalid InCondition - numeric literal does not match vector type {}", vector_type);
                                        }
                                        let mut cast_type = String::new();
                                        if vector_type != literal_type {
                                            cast_type = format!(" as {}", vector_type);
                                        }

                                        if vector_type == "f64" {
                                            format!(
                                                "{}{}.contains(&OrderedFloat({}{}))",
                                                if *negated { "!" } else { "" },
                                                vector_name,
                                                convert_literal(lit),
                                                cast_type
                                            )
                                        } else {
                                            //case i64
                                            format!(
                                                "{}{}.contains(&({}{}))",
                                                if *negated { "!" } else { "" },
                                                vector_name,
                                                convert_literal(lit),
                                                cast_type
                                            )
                                        }
                                    }
                                    IrLiteral::String(string) => {
                                        if vector_type != "String" {
                                            panic!("Invalid InCondition - string literal does not match vector type {}", vector_type);
                                        }
                                        //check if the string is empty
                                        if string.is_empty() {
                                            panic!("Invalid InCondition - empty string literal");
                                        }
                                        format!(
                                            "{}{}.contains(\"{}\")",
                                            if *negated { "!" } else { "" },
                                            vector_name,
                                            string
                                        )
                                    }
                                    _ => {
                                        panic!("Invalid InCondition - missing field")
                                    }
                                }
                            }
                            //fourth - aggregate case
                            else if field.aggregate.is_some() {
                                todo!("Aggregate case not implemented yet")
                            }
                            //fifth - arithmetic expr case
                            else if field.nested_expr.is_some() {
                                todo!("Nested expr case not implemented yet")
                            } else {
                                panic!("Invalid Incondition in group clause")
                            }
                        }
                    }
                },
                GroupBaseCondition::Exists(_, _) => {
                    panic!("Exists condition should be already parsed")
                }
                GroupBaseCondition::Boolean(boolean) => boolean.to_string(),
                GroupBaseCondition::ExistsVec(vec, negated) => {
                    format!(" {}{}.is_empty()", if *negated { "" } else { "!" }, vec)
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
                process_filter_condition(left, keys, query_object, acc_info),
                op_str,
                process_filter_condition(right, keys, query_object, acc_info)
            )
        }
    }
}

// Helper function to process fields in filter conditions
fn process_filter_field(
    field: &ComplexField,
    keys: &Vec<ColumnRef>,
    query_object: &QueryObject,
    acc_info: &GroupAccumulatorInfo,
    check_list: &mut Vec<String>, // Added parameter
) -> String {
    if let Some(ref nested) = field.nested_expr {
        let (left, op, right) = &**nested;

        let left_type = query_object.get_complex_field_type(left);
        let right_type = query_object.get_complex_field_type(right);

        let left_expr = process_filter_field(left, keys, query_object, acc_info, check_list);
        let right_expr = process_filter_field(right, keys, query_object, acc_info, check_list);

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
        if let Some(key_position) = keys.iter().position(|c| c.column == col.column) {
            let col_type = query_object.get_type(col);
            // It's a key - use its position in the group by tuple
            if keys.len() == 1 {
                check_list.push(format!("x.0{}.is_some()", as_ref));
                format!(
                    "x.0{}.unwrap(){}",
                    as_ref,
                    if col_type == "f64" {
                        ".into_inner()"
                    } else {
                        ""
                    }
                )
            } else {
                check_list.push(format!("x.0.{}{}.is_some()", key_position, as_ref));
                format!(
                    "x.0.{}{}.unwrap(){}",
                    key_position,
                    as_ref,
                    if col_type == "f64" {
                        ".into_inner()"
                    } else {
                        ""
                    }
                )
            }
        } else {
            // Not a key - use x.1

            let stream_name = if col.table.is_some() {
                query_object
                    .get_stream_from_alias(col.table.as_ref().unwrap())
                    .unwrap()
            } else if query_object.streams.len() == 1 {
                query_object.streams.first().unwrap().0
            } else {
                panic!("Column reference must have a table reference")
            };

            let stream = query_object.get_stream(stream_name);

            stream.check_if_column_exists(&col.column);

            check_list.push(format!(
                "x.1{}.{}{}.is_some()",
                stream.get_access().get_base_path(),
                col.column,
                as_ref
            ));

            format!(
                "x.1{}.{}{}.unwrap()",
                stream.get_access().get_base_path(),
                col.column,
                as_ref
            )
        }
    } else if let Some(ref lit) = field.literal {
        match lit {
            IrLiteral::Integer(i) => i.to_string(),
            IrLiteral::Float(f) => format!("{:.2}", f),
            IrLiteral::String(s) => format!("\"{}\"", s),
            IrLiteral::Boolean(b) => b.to_string(),
            IrLiteral::ColumnRef(_) => {
                panic!("ColumnRef should have been handled earlier")
            }
        }
    } else if let Some(ref agg) = field.aggregate {
        //retrive aggregate position from the accumulator
        let agg_pos = acc_info.get_agg_position(agg);
        // Aggregates are always in x.1
        let col = &agg.column;
        let col_access = if acc_info.agg_positions.len() == 1 {
            "x.1".to_string()
        } else {
            format!("x.1.{}", agg_pos)
        };

        if agg.function != AggregateType::Count {
            check_list.push(format!("{}.is_some()", col_access));
        }

        match agg.function {
            AggregateType::Count => col_access.to_string(),
            AggregateType::Max | AggregateType::Min | AggregateType::Sum => {
                format!("{}.unwrap()", col_access)
            }
            AggregateType::Avg => {
                //get the sum and count positions. Sum position corresponds to the position of the aggregate in the accumulator
                let count_pos = acc_info.get_agg_position(&AggregateFunction {
                    function: AggregateType::Count,
                    column: col.clone(),
                });
                format!("{}.unwrap() / x.1.{} as f64", col_access, count_pos)
            }
        }
    } else {
        panic!("Invalid ComplexField - no valid content")
    }
}
