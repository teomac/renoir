use crate::dsl::ir::ir_ast_structure::ComplexField;
use crate::dsl::ir::ir_ast_structure::{
    ColumnRef, FilterConditionType, IrLiteral, NullCondition, NullOp,
};
use crate::dsl::ir::r_utils::convert_literal;
use crate::dsl::ir::FilterClause;
use crate::dsl::ir::QueryObject;
use crate::dsl::ir::{BinaryOp, InCondition};
use crate::dsl::ir::{ComparisonOp, Condition};
use crate::dsl::struct_object::utils::*;
use core::panic;

pub fn process_filter_clause(
    clause: &FilterClause,
    stream_name: &String,
    query_object: &mut QueryObject,
) -> Result<(), Box<dyn std::error::Error>> {
    let filter_string = process_filter(clause, query_object);

    let final_string = format!(".filter(move |x| {})", filter_string);

    let stream = query_object.get_mut_stream(stream_name);
    stream.insert_op(final_string);

    Ok(())
}

pub fn process_filter(clause: &FilterClause, query_object: &mut QueryObject) -> String {
    match clause {
        FilterClause::Base(condition) => process_condition(condition, query_object),
        FilterClause::Expression {
            left,
            binary_op,
            right,
        } => {
            let op_str = match binary_op {
                BinaryOp::And => "&&",
                BinaryOp::Or => "||",
            };

            // Look for the specific patterns that need parentheses
            let left_needs_parens = matches!(
                **left,
                FilterClause::Expression {
                    binary_op: BinaryOp::Or,
                    ..
                }
            );
            let right_needs_parens = matches!(
                **right,
                FilterClause::Expression {
                    binary_op: BinaryOp::Or,
                    ..
                }
            );

            let left_str = if left_needs_parens {
                format!("({})", process_filter(left, query_object))
            } else {
                process_filter(left, query_object)
            };

            let right_str = if right_needs_parens {
                format!("({})", process_filter(right, query_object))
            } else {
                process_filter(right, query_object)
            };

            format!("{} {} {}", left_str, op_str, right_str)
        }
    }
}

// Added new helper function to process arithmetic expressions
fn process_arithmetic_expression(
    field: &ComplexField,
    check_list: &mut Vec<String>,
    casting_type: &mut String,
    query_object: &QueryObject,
) -> String {
    let needs_casting = !casting_type.is_empty();

    if let Some(ref nested) = field.nested_expr {
        let (left, op, right, is_par) = &**nested;

        let left_type = query_object.get_complex_field_type(left);
        let right_type = query_object.get_complex_field_type(right);

        //type checking
        //if types are different: case 1. they are both numeric, case 2. one is numeric and the other is not
        if left_type != right_type {
            //check if they are both numbers
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
                    *casting_type = "f64".to_string();
                }
            }

            let left_expr =
                process_arithmetic_expression(left, check_list, casting_type, query_object);
            let right_expr =
                process_arithmetic_expression(right, check_list, casting_type, query_object);

            // Special handling for power operation (^)
            if op == "^" {
                // If either operand is f64, use powf
                if left_type == "f64" || right_type == "f64" || casting_type == "f64" {
                    return format!("({}).powf({})", left_expr, right_expr);
                } else {
                    // Both are integers, use pow
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

            // Special handling for power operation (^)
            if op == "^" {
                let left_expr =
                    process_arithmetic_expression(left, check_list, casting_type, query_object);
                let right_expr =
                    process_arithmetic_expression(right, check_list, casting_type, query_object);

                // If both are f64, use powf
                if left_type == "f64" || casting_type == "f64" {
                    return format!("({}).powf({})", left_expr, right_expr);
                } else {
                    // Both are integers, use pow
                    return format!("({}).pow({})", left_expr, right_expr);
                }
            }

            // Regular arithmetic with same types
            format!(
                "{}{} {} {}{}",
                if *is_par { "(" } else { "" },
                process_arithmetic_expression(left, check_list, casting_type, query_object),
                op,
                process_arithmetic_expression(right, check_list, casting_type, query_object),
                if *is_par { ")" } else { "" }
            )
        }
    } else if let Some(ref col) = field.column_ref {
        let stream_name = if col.table.is_some() {
            query_object
                .get_stream_from_alias(col.table.as_ref().unwrap())
                .unwrap()
        } else {
            let all_streams = &query_object.streams;
            if all_streams.len() > 1 {
                panic!("Invalid column reference - missing table name");
            }
            all_streams.first().unwrap().0
        };
        // Validate column
        check_column_validity(col, stream_name, query_object);

        let stream = query_object.get_stream(stream_name);
        let c_type = query_object.get_type(col);

        check_list.push(format!(
            "x{}.{}.is_some()",
            stream.get_access().get_base_path(),
            col.column
        ));

        if needs_casting && c_type != "f64" {
            format!(
                "(x{}.{}.unwrap() as {})",
                stream.get_access().get_base_path(),
                col.column,
                casting_type
            )
        } else {
            format!(
                "x{}.{}{}.unwrap()",
                stream.get_access().get_base_path(),
                col.column,
                if c_type == "String" { ".clone()" } else { "" }
            )
        }
    } else if let Some(ref lit) = field.literal {
        match lit {
            IrLiteral::Integer(i) => {
                if needs_casting {
                    format!("{}.0", i)
                } else {
                    i.to_string()
                }
            }
            IrLiteral::Float(f) => format!("{:.2}", f),
            IrLiteral::String(s) => format!("\"{}\"", s),
            IrLiteral::Boolean(b) => b.to_string(),
            IrLiteral::ColumnRef(_) => {
                panic!("Invalid ComplexField - column reference not expected here");
            }
        }
    } else if let Some((sub_name, sub_type)) = &field.subquery_vec {
        //check if the subquery is empty
        check_list.push(format!("!{}.is_empty()", sub_name));
        //check if the type is correct
        if sub_type == "f64" {
            format!("{}.first().unwrap().unwrap().into_inner()", sub_name)
        } else {
            if needs_casting {
                format!(
                    "({}.first().unwrap().unwrap() as {})",
                    sub_name, casting_type
                )
            } else {
                format!("{}.first().unwrap().unwrap()", sub_name)
            }
        }
    } else {
        panic!("Invalid ComplexField - no valid content");
    }
}

/// Process a condition which can be either a comparison or a null check
fn process_condition(condition: &FilterConditionType, query_object: &QueryObject) -> String {
    match condition {
        FilterConditionType::Comparison(comparison) => {
            process_comparison_condition(comparison, query_object)
        }
        FilterConditionType::NullCheck(null_check) => {
            process_null_check_condition(null_check, query_object)
        }
        FilterConditionType::In(in_condition) => {
            match in_condition {
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
                                panic!("Invalid InCondition - column reference not expected here")
                            }
                        })
                        .collect::<Vec<String>>()
                        .join(", ");

                    //check if the complex field is a column reference
                    let col_ref = if field.column_ref.is_some() {
                        field.column_ref.as_ref().unwrap()
                    } else {
                        panic!("Invalid InCondition - missing column reference")
                    };

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
                    check_column_validity(col_ref, stream_name, query_object);

                    let stream = query_object.get_stream(stream_name);
                    let c_type = query_object.get_type(col_ref);

                    // Generate the condition
                    let condition_str = format!(
                        "x{}.{}.as_ref().unwrap(){}",
                        stream.get_access().get_base_path(),
                        col_ref.column,
                        if c_type == "String" { ".as_str()" } else { "" }
                    );

                    // Generate the final string
                    format!(
                        "if x{}.{}.as_ref().is_some() {{{}vec![{}].contains(&{})}} else {{false}}",
                        stream.get_access().get_base_path(),
                        col_ref.column,
                        if *negated { "!" } else { "" },
                        values_str,
                        condition_str
                    )
                }
                InCondition::InSubquery { .. } => panic!("We should not have InSubquery here"),
                InCondition::InVec {
                    field,
                    vector_name,
                    vector_type,
                    negated,
                } => {
                    //first, we have the name and type of the vector
                    if field.subquery_vec.is_some() {
                        let (field_name, field_type) = field.subquery_vec.clone().unwrap();

                        //compare field type with vector type
                        if *field_type != *vector_type {
                            //check if they are both numbers
                            if (field_type == "f64" || field_type == "i64" || field_type == "usize")
                                && (*vector_type == "f64"
                                    || *vector_type == "i64"
                                    || *vector_type == "usize")
                            {
                                //needs to cast the field_type to the actual vector type
                                let cast_type = vector_type;

                                if *vector_type == "f64" {
                                    format!(
                                        "{}{}.contains(&Some(OrderedFloat({}.first().unwrap().unwrap() as {})))",
                                        if *negated { "!" } else { "" },
                                        vector_name,
                                        field_name,
                                        cast_type,
                                    )
                                } else {
                                    format!(
                                        "{}{}.contains(&Some({}.first().unwrap().unwrap() as {}))",
                                        if *negated { "!" } else { "" },
                                        vector_name,
                                        field_name,
                                        cast_type,
                                    )
                                }
                            } else {
                                panic!("Invalid InCondition - column type {} does not match vector type {}", field_type, vector_type);
                            }
                        } else {
                            //standard case
                            // Generate the final string
                            format!(
                                "{}{}.contains({}.first().unwrap())",
                                if *negated { "!" } else { "" },
                                vector_name,
                                field_name,
                            )
                        }
                    } else if field.column_ref.is_some() {
                        //second, we have another type of complexField
                        //as for now we only manage the Column Ref case
                        let col_ref = if field.column_ref.is_some() {
                            field.column_ref.as_ref().unwrap()
                        } else {
                            panic!("Invalid InCondition - missing column reference")
                        };

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
                        check_column_validity(col_ref, stream_name, query_object);

                        let stream = query_object.get_stream(stream_name);
                        let c_type = query_object.get_type(col_ref);

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
                                        "&Some(OrderedFloat(x{}.{}.unwrap() as {}))",
                                        stream.get_access().get_base_path(),
                                        col_ref.column,
                                        cast_type
                                    )
                                } else {
                                    format!(
                                        "&Some(x{}.{}.as_ref().unwrap() as {})",
                                        stream.get_access().get_base_path(),
                                        col_ref.column,
                                        cast_type
                                    )
                                };
                                format!(
                                            "if x{}.{}.as_ref().is_some() {{{}{}.contains({})}} else {{false}}",
                                            stream.get_access().get_base_path(),
                                            col_ref.column,
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
                                    "&Some(OrderedFloat(x{}.{}.unwrap()))",
                                    stream.get_access().get_base_path(),
                                    col_ref.column,
                                )
                            } else {
                                format!(
                                    "&x{}.{}",
                                    stream.get_access().get_base_path(),
                                    col_ref.column,
                                )
                            };

                            // Generate the final string
                            format!(
                                "if x{}.{}.as_ref().is_some() {{{}{}.contains({})}} else {{false}}",
                                stream.get_access().get_base_path(),
                                col_ref.column,
                                if *negated { "!" } else { "" },
                                vector_name,
                                condition_str
                            )
                        }
                    } else if field.literal.is_some() {
                        //third, we have a literal

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
                                        "{}{}.contains(&Some(OrderedFloat({}{})))",
                                        if *negated { "!" } else { "" },
                                        vector_name,
                                        convert_literal(lit),
                                        cast_type
                                    )
                                } else {
                                    //case i64
                                    format!(
                                        "{}{}.contains(&Some({}{}))",
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
                                    "{}{}.contains(&Some(\"{}\".to_string()))",
                                    if *negated { "!" } else { "" },
                                    vector_name,
                                    string
                                )
                            }
                            _ => {
                                panic!("Invalid InCondition - missing field")
                            }
                        }
                    } else if field.nested_expr.is_some() {
                        let mut cast = String::new();
                        let mut check_list: Vec<String> = Vec::new();
                        //fourth, we have a nested expression
                        let cond: String = if vector_type != "f64" {
                            format!(
                                "{}{}.contains(&Some({}))",
                                if *negated { "!" } else { "" },
                                vector_name,
                                process_arithmetic_expression(
                                    field,
                                    &mut check_list,
                                    &mut cast,
                                    query_object
                                )
                            )
                        } else {
                            format!(
                                "{}{}.contains(&Some(OrderedFloat(({} as f64))))",
                                if *negated { "!" } else { "" },
                                vector_name,
                                process_arithmetic_expression(
                                    field,
                                    &mut check_list,
                                    &mut cast,
                                    query_object
                                )
                            )
                        };
                        format!(
                            "if {} {{ {} }} else {{false}}",
                            check_list.join(" && "),
                            cond,
                        )
                    } else {
                        //other cases such as AggregateFunction, Subquery, etc.
                        //we should not have these cases here
                        panic!("Invalid InCondition - not supported")
                    }
                }
            }
        }
        FilterConditionType::Exists(_, _) => panic!("Exists condition should be already parsed"),
        FilterConditionType::Boolean(boolean) => boolean.to_string(),
        FilterConditionType::ExistsVec(vec, bool) => {
            format!(" {}{}.is_empty()", if *bool { "" } else { "!" }, vec)
        }
    }
}

/// Process a null check condition (IS NULL or IS NOT NULL)
fn process_null_check_condition(condition: &NullCondition, query_object: &QueryObject) -> String {
    let field = &condition.field;

    //case column reference
    if field.column_ref.is_some() {
        let col_ref = if condition.field.column_ref.is_some() {
            condition.field.column_ref.as_ref().unwrap()
        } else {
            panic!("Invalid null check condition - missing column reference")
        };

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

        let stream = query_object.get_stream(stream_name);

        let field = if condition.field.column_ref.is_some() {
            //validate column
            check_column_validity(col_ref, stream_name, query_object);
            format!(
                "x{}.{}",
                stream.get_access().get_base_path(),
                col_ref.column
            )
        } else {
            panic!("Invalid null check condition - missing column reference")
        };

        match condition.operator {
            NullOp::IsNull => format!("{}.is_none()", field),
            NullOp::IsNotNull => format!("{}.is_some()", field),
        }
    }
    //case it is a literal
    else if field.literal.is_some() {
        let lit = field.literal.as_ref().unwrap();
        match lit {
            IrLiteral::Boolean(_) => match condition.operator {
                NullOp::IsNull => "false".to_string(),
                NullOp::IsNotNull => "true".to_string(),
            },

            IrLiteral::Float(_) | IrLiteral::Integer(_) => match condition.operator {
                NullOp::IsNull => "false".to_string(),
                NullOp::IsNotNull => "true".to_string(),
            },

            IrLiteral::String(string) => match condition.operator {
                NullOp::IsNull => format!("{}", string.is_empty()),
                NullOp::IsNotNull => format!("{}", !string.is_empty()),
            },

            _ => {
                panic!("Invalid null check condition - missing field")
            }
        }
    } else if let Some((sub_name, _)) = &field.subquery_vec {
        match condition.operator {
            NullOp::IsNull => format!("{}.is_empty()", sub_name),
            NullOp::IsNotNull => format!("!{}.is_empty()", sub_name),
        }
    } else if field.nested_expr.is_some() {
        //case nested expression
        //collect column null checks
        let mut null_checks = Vec::new();
        collect_column_null_checks(field, query_object, &mut null_checks);

        //now the actual final string is only composed by null checks
        match condition.operator {
            NullOp::IsNull => format!("!({})", null_checks.join(" && ")),
            NullOp::IsNotNull => format!("{}", null_checks.join(" && ")),
        }
    }
    //case nested_expr: TODO
    else {
        panic!("Invalid null check condition - missing field")
    }
}

/// Process a comparison condition (>, <, =, etc.)
fn process_comparison_condition(condition: &Condition, query_object: &QueryObject) -> String {
    let mut cast = String::new();
    let mut check_list: Vec<String> = Vec::new();
    let operator_str = match condition.operator {
        ComparisonOp::GreaterThan => ">",
        ComparisonOp::LessThan => "<",
        ComparisonOp::Equal => "==",
        ComparisonOp::GreaterThanEquals => ">=",
        ComparisonOp::LessThanEquals => "<=",
        ComparisonOp::NotEqual => "!=",
    };

    // Get types for both sides of comparison
    let left_type = query_object.get_complex_field_type(&condition.left_field);
    let right_type = query_object.get_complex_field_type(&condition.right_field);

    let has_left_column = has_column_reference(&condition.left_field);
    let has_right_column = has_column_reference(&condition.right_field);

    //type checking
    //if types are different
    if left_type != right_type {
        //check if they are both numbers
        if left_type != "f64"
            && left_type != "i64"
            && left_type != "usize"
            && right_type != "f64"
            && right_type != "i64"
            && right_type != "usize"
        {
            panic!(
                "Invalid comparison expression - non-numeric types: {} and {}",
                left_type, right_type
            );
        } else {
            //they are both numbers
            if left_type == "f64" || right_type == "f64" {
                cast = "f64".to_string();
            }
        }
    } else {
        //if operand is plus, minus, multiply, division, or power and types are not numeric, panic
        if (operator_str == "+"
            || operator_str == "-"
            || operator_str == "*"
            || operator_str == "/"
            || operator_str == "^")
            && left_type != "f64"
            && left_type != "i64"
        {
            panic!(
                "Invalid arithmetic expression - non-numeric types: {} and {}",
                left_type, right_type
            );
        }
    }

    if !query_object.has_join {
        // Case with at least one column reference - need null checking
        if has_left_column || has_right_column {
            let mut null_checks = Vec::new();
            if has_left_column {
                collect_column_null_checks(&condition.left_field, query_object, &mut null_checks);
            }
            if has_right_column {
                collect_column_null_checks(&condition.right_field, query_object, &mut null_checks);
            }

            // Remove duplicates from null_checks
            null_checks.sort();
            null_checks.dedup();

            let null_check_str = null_checks.join(" && ");
            format!(
                "if {} {{ {} {} {} }} else {{ false }}",
                null_check_str,
                process_arithmetic_expression(
                    &condition.left_field,
                    &mut check_list,
                    &mut cast,
                    query_object
                ),
                operator_str,
                process_arithmetic_expression(
                    &condition.right_field,
                    &mut check_list,
                    &mut cast,
                    query_object
                ),
            )
        } else {
            // No column references - direct comparison
            format!(
                "{} {} {}",
                process_arithmetic_expression(
                    &condition.left_field,
                    &mut check_list,
                    &mut cast,
                    query_object
                ),
                operator_str,
                process_arithmetic_expression(
                    &condition.right_field,
                    &mut check_list,
                    &mut cast,
                    query_object
                ),
            )
        }
    } else {
        // Handle JOIN case
        if has_left_column || has_right_column {
            let mut null_checks = Vec::new();

            // For JOIN case, we need to get the correct table names
            if has_left_column {
                let left_columns = collect_columns(&condition.left_field);
                for _ in left_columns {
                    collect_column_null_checks(
                        &condition.left_field,
                        query_object,
                        &mut null_checks,
                    );
                }
            }
            if has_right_column {
                let right_columns = collect_columns(&condition.right_field);
                for _ in right_columns {
                    collect_column_null_checks(
                        &condition.right_field,
                        query_object,
                        &mut null_checks,
                    );
                }
            }

            //remove duplicates from null_checks
            null_checks.sort();
            null_checks.dedup();

            let null_check_str = null_checks.join(" && ");
            format!(
                "if {} {{ {} {} {} }} else {{ false }}",
                null_check_str,
                process_arithmetic_expression(
                    &condition.left_field,
                    &mut check_list,
                    &mut cast,
                    query_object
                ),
                operator_str,
                process_arithmetic_expression(
                    &condition.right_field,
                    &mut check_list,
                    &mut cast,
                    query_object
                ),
            )
        } else {
            // No column references - direct comparison
            format!(
                "{} {} {}",
                process_arithmetic_expression(
                    &condition.left_field,
                    &mut check_list,
                    &mut cast,
                    query_object
                ),
                operator_str,
                process_arithmetic_expression(
                    &condition.right_field,
                    &mut check_list,
                    &mut cast,
                    query_object
                ),
            )
        }
    }
}

// Helper function to check if a ComplexField contains any column references
fn has_column_reference(field: &ComplexField) -> bool {
    if field.column_ref.is_some() {
        return true;
    }
    if let Some(ref nested) = field.nested_expr {
        let (left, _, right, _) = &**nested;
        return has_column_reference(left) || has_column_reference(right);
    }
    if let Some(IrLiteral::ColumnRef(_)) = field.literal {
        return true;
    }
    if let Some(ref _agg) = field.aggregate {
        return true;
    }
    false
}

// Helper function to collect all column references from a ComplexField
fn collect_columns(field: &ComplexField) -> Vec<ColumnRef> {
    let mut columns = Vec::new();

    if let Some(ref col) = field.column_ref {
        columns.push(col.clone());
    }
    if let Some(ref nested) = field.nested_expr {
        let (left, _, right, _) = &**nested;
        columns.extend(collect_columns(left));
        columns.extend(collect_columns(right));
    }
    if let Some(IrLiteral::ColumnRef(col)) = field.literal.as_ref() {
        columns.push(col.clone());
    }
    if let Some(ref agg) = field.aggregate {
        columns.push(agg.column.clone());
    }

    columns
}

// Helper function to collect null checks for all columns in a ComplexField
fn collect_column_null_checks(
    field: &ComplexField,
    query_object: &QueryObject,
    checks: &mut Vec<String>,
) {
    if let Some(ref col) = field.column_ref {
        let stream_name = if col.table.is_some() {
            query_object
                .get_stream_from_alias(col.table.as_ref().unwrap())
                .unwrap()
        } else {
            let all_streams = &query_object.streams;
            if all_streams.len() > 1 {
                panic!("Invalid column reference - missing table name");
            }
            all_streams.first().unwrap().0
        };

        check_column_validity(col, stream_name, query_object);

        let stream = query_object.get_stream(stream_name);

        checks.push(format!(
            "x{}.{}.is_some()",
            stream.get_access().get_base_path(),
            col.column
        ));
    }
    if let Some(ref nested) = field.nested_expr {
        let (left, _, right, _) = &**nested;
        collect_column_null_checks(left, query_object, checks);
        collect_column_null_checks(right, query_object, checks);
    }
    if field.literal.is_some() {
        //skip literal check
        return;
    }
    if let Some(ref agg) = field.aggregate {
        let stream_name = if agg.column.table.is_some() {
            query_object
                .get_stream_from_alias(agg.column.table.as_ref().unwrap())
                .unwrap()
        } else {
            let all_streams = &query_object.streams;
            if all_streams.len() > 1 {
                panic!("Invalid column reference - missing table name");
            }
            all_streams.first().unwrap().0
        };

        check_column_validity(&agg.column, stream_name, query_object);

        let stream = query_object.get_stream(stream_name);

        checks.push(format!(
            "x{}.{}.is_some()",
            stream.get_access().get_base_path(),
            agg.column.column
        ));
    }
    if let Some((sub_name, _)) = &field.subquery_vec {
        //check if the subquery is empty
        checks.push(format!("!{}.is_empty()", sub_name));
    }
}
