use crate::dsl::ir::ir_ast_structure::ComplexField;
use crate::dsl::ir::ir_ast_structure::{
    ColumnRef, IrLiteral, NullCondition, NullOp, FilterConditionType,
};
use crate::dsl::ir::BinaryOp;
use crate::dsl::ir::QueryObject;
use crate::dsl::ir::FilterClause;
use crate::dsl::ir::{ComparisonOp, Condition};
use crate::dsl::struct_object::utils::*;


pub fn process_filter_clause(clause: &FilterClause, stream_name: &String, query_object: &mut QueryObject) -> Result<(), Box<dyn std::error::Error>> {
    let filter_string = process_filter(clause, query_object);
    
    let final_string = format!(".filter(|x| {})", filter_string);

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

            format!(
                "{} {} {}",
                left_str, op_str, right_str
            )
        }
    }
}

// Added new helper function to process arithmetic expressions
fn process_arithmetic_expression(field: &ComplexField, query_object: &QueryObject) -> String {
    if let Some(ref nested) = field.nested_expr {
        let (left, op, right) = &**nested;

        let left_type = query_object.get_complex_field_type(left);
        let right_type = query_object.get_complex_field_type(right);

        //type checking
        //if types are different: case 1. they are both numeric, case 2. one is numeric and the other is not
        if left_type != right_type {
            if (left_type == "f64" || left_type == "i64")
                && (right_type == "f64" || right_type == "i64")
            {
                // Division always results in f64
                if op == "/" {
                    return format!(
                        "({} as f64) {} ({} as f64)",
                        process_arithmetic_expression(left, query_object),
                        op,
                        process_arithmetic_expression(right, query_object)
                    );
                }

                // Special handling for power operation (^)
                if op == "^" {
                    let left_expr = process_arithmetic_expression(left, query_object);
                    let right_expr = process_arithmetic_expression(right, query_object);

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
                        return format!("({}).pow({} as i64)", left_expr, right_expr);
                    }
                }

                let left_expr = process_arithmetic_expression(left, query_object);
                let right_expr = process_arithmetic_expression(right, query_object);

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
            if (op == "+" || op == "-" || op == "*" || op == "/" || op == "^") && left_type != "f64" && left_type != "i64" {
                panic!(
                    "Invalid arithmetic expression - non-numeric types: {} and {}",
                    left_type, right_type
                );
            }

            // Division always results in f64
            if op == "/" {
                return format!(
                    "({} as f64) {} ({} as f64)",
                    process_arithmetic_expression(left, query_object),
                    op,
                    process_arithmetic_expression(right, query_object)
                );
            }

            // Special handling for power operation (^)
            if op == "^" {
                let left_expr = process_arithmetic_expression(left, query_object);
                let right_expr = process_arithmetic_expression(right, query_object);

                // If both are f64, use powf
                if left_type == "f64" {
                    return format!("({}).powf({})", left_expr, right_expr);
                } else {
                    // Both are integers, use pow
                    return format!("({}).pow({})", left_expr, right_expr);
                }
            }

            // Regular arithmetic with same types
            format!(
                "({} {} {})",
                process_arithmetic_expression(left, query_object),
                op,
                process_arithmetic_expression(right, query_object)
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
        format!(
            "x{}.{}{}.unwrap()",
            stream.get_access().get_base_path(),
            col.column,
            if c_type == "String" { ".clone()" } else { "" }
        )
    } else if let Some(ref lit) = field.literal {
        match lit {
            IrLiteral::Integer(i) => i.to_string(),
            IrLiteral::Float(f) => format!("{:.2}", f),
            IrLiteral::String(s) => format!("\"{}\"", s),
            IrLiteral::Boolean(b) => b.to_string(),
            IrLiteral::ColumnRef(_) => {
                panic!("Invalid ComplexField - column reference not expected here");
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
    }
}

/// Process a null check condition (IS NULL or IS NOT NULL)
fn process_null_check_condition(condition: &NullCondition, query_object: &QueryObject) -> String {
    let field = &condition.field;

    //case column reference
    if field.column_ref.is_some(){
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
    else if field.literal.is_some(){
        let lit = field.literal.as_ref().unwrap();
        match lit {
            IrLiteral::Boolean(_) => {
                match condition.operator {
                    NullOp::IsNull => "false".to_string(),
                    NullOp::IsNotNull => "true".to_string(),
                }
            }

            IrLiteral::Float(_) | IrLiteral::Integer(_) => {
                match condition.operator {
                    NullOp::IsNull => "false".to_string(),
                    NullOp::IsNotNull => "true".to_string(),
                }
            }

            IrLiteral::String(string) => {
                match condition.operator {
                    NullOp::IsNull => format!("{}", string.is_empty()),
                    NullOp::IsNotNull => format!("{}", !string.is_empty()),
                }
            }

            _ => {
                panic!("Invalid null check condition - missing field")
        }
    }}

    //case nested_expr: TODO

    else{
        panic!("Invalid null check condition - missing field")
    }


    

   
}

/// Process a comparison condition (>, <, =, etc.)
fn process_comparison_condition(condition: &Condition, query_object: &QueryObject) -> String {
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

    // Handle type conversions for comparison
    let (left_conversion, right_conversion) = if left_type == "f64" && right_type == "i64" {
        ("", " as f64")
    } else if left_type == "i64" && right_type == "f64" {
        (" as f64", "")
    } else {
        ("", "")
    };

    //type checking
    //if types are different
    if left_type != right_type {
        if (left_type == "String" && right_type != "String")
            || (left_type != "String" && right_type == "String")
        {
            panic!("Invalid comparison - cannot compare string with other type");
        }
        if (left_type == "bool" && right_type != "bool")
            || (left_type != "bool" && right_type == "bool")
        {
            panic!("Invalid comparison - cannot compare boolean with other type");
        }
    } else {
        //if operand is plus, minus, multiply, division, or power and types are not numeric, panic
        if (operator_str == "+"
            || operator_str == "-"
            || operator_str == "*"
            || operator_str == "/" || operator_str == "^") && left_type != "f64" && left_type != "i64" {
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
                "if {} {{ ({}{}) {} ({}{}) }} else {{ false }}",
                null_check_str,
                process_arithmetic_expression(&condition.left_field, query_object),
                left_conversion,
                operator_str,
                process_arithmetic_expression(&condition.right_field, query_object),
                right_conversion
            )
        } else {
            // No column references - direct comparison
            format!(
                "{}{} {} {}{}",
                process_arithmetic_expression(&condition.left_field, query_object),
                left_conversion,
                operator_str,
                process_arithmetic_expression(&condition.right_field, query_object),
                right_conversion
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
                "if {} {{ ({}{}) {} ({}{}) }} else {{ false }}",
                null_check_str,
                process_arithmetic_expression(&condition.left_field, query_object),
                left_conversion,
                operator_str,
                process_arithmetic_expression(&condition.right_field, query_object),
                right_conversion
            )
        } else {
            // No column references - direct comparison
            format!(
                "{}{} {} {}{}",
                process_arithmetic_expression(&condition.left_field, query_object),
                left_conversion,
                operator_str,
                process_arithmetic_expression(&condition.right_field, query_object),
                right_conversion
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
        let (left, _, right) = &**nested;
        return has_column_reference(left) || has_column_reference(right);
    }
    if let Some(ref lit) = field.literal {
        if let IrLiteral::ColumnRef(_) = lit {
            return true;
        }
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
        let (left, _, right) = &**nested;
        columns.extend(collect_columns(left));
        columns.extend(collect_columns(right));
    }
    if let IrLiteral::ColumnRef(col) = field.literal.as_ref().unwrap() {
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
        let (left, _, right) = &**nested;
        collect_column_null_checks(left, query_object, checks);
        collect_column_null_checks(right, query_object, checks);
    }
    if field.literal.is_some() {
        panic!("Invalid ComplexField - literal not expected here");
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
}
