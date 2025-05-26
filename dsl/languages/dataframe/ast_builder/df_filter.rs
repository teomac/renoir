use crate::dsl::ir::{
    BinaryOp, ComparisonOp, ComplexField, Condition, FilterClause, FilterConditionType, IrLiteral,
    IrPlan, NullCondition, NullOp,
};
use crate::dsl::languages::dataframe::conversion_error::ConversionError;
use serde_json::Value;
use std::sync::Arc;

use super::df_subqueries::process_scalar_subquery;
use super::df_utils::ConverterObject;

/// Process a Filter (WHERE) node from a Catalyst plan
pub(crate) fn process_filter(
    node: &Value,
    input_plan: Arc<IrPlan>,
    project_count: &mut i64,
    conv_object: &mut ConverterObject,
) -> Result<Arc<IrPlan>, Box<ConversionError>> {
    // Extract the condition array
    let condition_array = node
        .get("condition")
        .and_then(|c| c.as_array())
        .ok_or_else(|| Box::new(ConversionError::MissingField("condition".to_string())))?;

    // The first element is usually the root condition expression
    if condition_array.is_empty() {
        return Err(Box::new(ConversionError::InvalidExpression));
    }

    // Process the condition, starting from the first element (index 0)
    // Note: We don't need to track expr ID updates for filter conditions
    // since they don't create new projections, just use existing column references
    let (filter_clause, _) =
        process_condition_node(condition_array, 0, project_count, conv_object)?;

    // Create the Filter node
    Ok(Arc::new(IrPlan::Filter {
        input: input_plan,
        predicate: filter_clause,
    }))
}

/// Process a condition node in the condition array
/// Returns the processed FilterClause and the next index to process
pub(crate) fn process_condition_node(
    condition_array: &[Value],
    idx: usize,
    project_count: &mut i64,
    conv_object: &mut ConverterObject,
) -> Result<(FilterClause, usize), Box<ConversionError>> {
    if idx >= condition_array.len() {
        return Err(Box::new(ConversionError::InvalidExpression));
    }

    let node = &condition_array[idx];

    // Get the class name
    let class = node
        .get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    let node_type = class
        .split('.')
        .last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    match node_type {
        "And" | "Or" => {
            process_binary_op_node(condition_array, node_type, idx, project_count, conv_object)
        }
        "Not" => process_not_node(condition_array, idx, project_count, conv_object),
        "IsNotNull" | "IsNull" => {
            process_null_node(condition_array, node_type, idx, project_count, conv_object)
        }
        "EqualTo" | "GreaterThan" | "LessThan" | "GreaterThanOrEqual" | "LessThanOrEqual" | "NotEqualTo" => {
            process_comparison_node(condition_array, node_type, idx, project_count, conv_object)
        }
        "AttributeReference" => {
            // Check if it's a boolean column
            let data_type = node
                .get("dataType")
                .and_then(|dt| dt.as_str())
                .unwrap_or("");

            if data_type == "boolean" {
                // This is a boolean column being used directly, equivalent to "column == true"
                // Resolve using expr ID
                let (_, column_name, source_name) = 
                    conv_object.resolve_projection_column(node)
                        .map_err(|_| Box::new(ConversionError::InvalidExpression))?;

                let column_ref = crate::dsl::ir::ColumnRef {
                    table: Some(source_name),
                    column: column_name,
                };

                // Create a complex field for the column
                let left_field = ComplexField {
                    column_ref: Some(column_ref),
                    literal: None,
                    aggregate: None,
                    nested_expr: None,
                    subquery: None,
                    subquery_vec: None,
                };

                // Create a complex field for "true" literal
                let right_field = ComplexField {
                    column_ref: None,
                    literal: Some(IrLiteral::Boolean(true)),
                    aggregate: None,
                    nested_expr: None,
                    subquery: None,
                    subquery_vec: None,
                };

                Ok((
                    FilterClause::Base(FilterConditionType::Comparison(Condition {
                        left_field,
                        operator: ComparisonOp::Equal,
                        right_field,
                    })),
                    idx + 1,
                ))
            } else {
                // Not a boolean column - this is unexpected as a direct condition
                Err(Box::new(ConversionError::UnsupportedExpressionType(
                    format!(
                        "Non-boolean column used directly as condition: {}",
                        node_type
                    ),
                )))
            }
        }
        _ => Err(Box::new(ConversionError::UnsupportedExpressionType(
            node_type.to_string(),
        ))),
    }
}

/// Process an AND/OR node (left AND/OR right)
fn process_binary_op_node(
    condition_array: &[Value],
    op: &str,
    idx: usize,
    project_count: &mut i64,
    conv_object: &mut ConverterObject,
) -> Result<(FilterClause, usize), Box<ConversionError>> {
    // Process left operand (always the next node)
    let (left_clause, next_idx) =
        process_condition_node(condition_array, idx + 1, project_count, conv_object)?;

    // Process right operand (starts after the left branch is complete)
    let (right_clause, final_idx) =
        process_condition_node(condition_array, next_idx, project_count, conv_object)?;

    let binary_op = match op {
        "And" => BinaryOp::And,
        "Or" => BinaryOp::Or,
        _ => {
            return Err(Box::new(ConversionError::UnsupportedExpressionType(
                op.to_string(),
            )))
        }
    };

    Ok((
        FilterClause::Expression {
            left: Box::new(left_clause),
            binary_op,
            right: Box::new(right_clause),
        },
        final_idx,
    ))
}

/// Process a NOT node
fn process_not_node(
    condition_array: &[Value],
    idx: usize,
    project_count: &mut i64,
    conv_object: &mut ConverterObject,
) -> Result<(FilterClause, usize), Box<ConversionError>> {
    // Get the child index
    let child_idx = idx + 1; // Child is always the next node

    // Check if the child is an AttributeReference (direct column reference)
    // This would indicate a pattern like "col("dead") == False" which is represented
    // as "NOT dead" in the Catalyst plan
    let child_node = &condition_array[child_idx];
    if let Some(child_class) = child_node.get("class").and_then(|c| c.as_str()) {
        if child_class.ends_with("AttributeReference") {
            // This is a boolean column being negated, equivalent to "column == false"
            // Resolve using expr ID
            let (_, column_name, source_name) = 
                conv_object.resolve_projection_column(child_node)
                    .map_err(|_| Box::new(ConversionError::InvalidExpression))?;

            let column_ref = crate::dsl::ir::ColumnRef {
                table: Some(source_name),
                column: column_name,
            };

            // Create a complex field for the column
            let left_field = ComplexField {
                column_ref: Some(column_ref),
                literal: None,
                aggregate: None,
                nested_expr: None,
                subquery: None,
                subquery_vec: None,
            };

            // Create a complex field for "false" literal
            let right_field = ComplexField {
                column_ref: None,
                literal: Some(IrLiteral::Boolean(false)),
                aggregate: None,
                nested_expr: None,
                subquery: None,
                subquery_vec: None,
            };

            return Ok((
                FilterClause::Base(FilterConditionType::Comparison(Condition {
                    left_field,
                    operator: ComparisonOp::Equal,
                    right_field,
                })),
                child_idx + 1,
            ));
        }
    }

    // Process the child condition
    let (child_clause, next_idx) =
        process_condition_node(condition_array, child_idx, project_count, conv_object)?;

    // Negate the condition based on its type
    match child_clause {
        FilterClause::Base(FilterConditionType::Comparison(Condition {
            left_field,
            operator,
            right_field,
        })) => {
            // Invert comparison operator
            let negated_operator = match operator {
                ComparisonOp::Equal => ComparisonOp::NotEqual,
                ComparisonOp::NotEqual => ComparisonOp::Equal,
                ComparisonOp::GreaterThan => ComparisonOp::LessThanEquals,
                ComparisonOp::LessThan => ComparisonOp::GreaterThanEquals,
                ComparisonOp::GreaterThanEquals => ComparisonOp::LessThan,
                ComparisonOp::LessThanEquals => ComparisonOp::GreaterThan,
            };

            Ok((
                FilterClause::Base(FilterConditionType::Comparison(Condition {
                    left_field,
                    operator: negated_operator,
                    right_field,
                })),
                next_idx,
            ))
        }
        FilterClause::Base(FilterConditionType::NullCheck(NullCondition { field, operator })) => {
            // Invert null check operator
            let negated_operator = match operator {
                NullOp::IsNull => NullOp::IsNotNull,
                NullOp::IsNotNull => NullOp::IsNull,
            };

            Ok((
                FilterClause::Base(FilterConditionType::NullCheck(NullCondition {
                    field,
                    operator: negated_operator,
                })),
                next_idx,
            ))
        }
        FilterClause::Base(FilterConditionType::Boolean(value)) => {
            // Invert boolean value
            Ok((
                FilterClause::Base(FilterConditionType::Boolean(!value)),
                next_idx,
            ))
        }
        _ => Err(Box::new(ConversionError::UnsupportedExpressionType(
            "Negation of complex condition".to_string(),
        ))),
    }
}

/// Process a NULL node (IS NULL / IS NOT NULL)
fn process_null_node(
    condition_array: &[Value],
    op: &str,
    idx: usize,
    project_count: &mut i64,
    conv_object: &mut ConverterObject,
) -> Result<(FilterClause, usize), Box<ConversionError>> {
    // The attribute reference should be the next node
    let attr_idx = idx + 1;

    if attr_idx >= condition_array.len() {
        return Err(Box::new(ConversionError::InvalidExpression));
    }

    // Process the attribute reference
    let (field, next_idx) =
        process_expression(condition_array, attr_idx, project_count, conv_object)?;

    let null_op = match op {
        "IsNotNull" => NullOp::IsNotNull,
        "IsNull" => NullOp::IsNull,
        _ => {
            return Err(Box::new(ConversionError::UnsupportedExpressionType(
                op.to_string(),
            )))
        }
    };

    Ok((
        FilterClause::Base(FilterConditionType::NullCheck(NullCondition {
            field,
            operator: null_op,
        })),
        next_idx,
    ))
}

/// Process a comparison node (=, >, <, >=, <=, !=)
fn process_comparison_node(
    condition_array: &[Value],
    node_type: &str,
    idx: usize,
    project_count: &mut i64,
    conv_object: &mut ConverterObject,
) -> Result<(FilterClause, usize), Box<ConversionError>> {
    let node = &condition_array[idx];

    // Get indices for left and right expressions
    let left_idx = node
        .get("left")
        .and_then(|l| l.as_u64())
        .ok_or_else(|| Box::new(ConversionError::MissingField("left".to_string())))?
        as usize;

    // Process left expression
    let (left_field, next_idx) = process_expression(
        condition_array,
        idx + left_idx + 1,
        project_count,
        conv_object,
    )?;

    // Process right expression
    let (right_field, final_idx) =
        process_expression(condition_array, next_idx, project_count, conv_object)?;

    let comparison_op: ComparisonOp = match node_type {
        "EqualTo" => ComparisonOp::Equal,
        "NotEqualTo" => ComparisonOp::NotEqual,
        "GreaterThan" => ComparisonOp::GreaterThan,
        "LessThan" => ComparisonOp::LessThan,
        "GreaterThanOrEqual" => ComparisonOp::GreaterThanEquals,
        "LessThanOrEqual" => ComparisonOp::LessThanEquals,
        _ => {
            return Err(Box::new(ConversionError::UnsupportedExpressionType(
                node_type.to_string(),
            )))
        }
    };

    Ok((
        FilterClause::Base(FilterConditionType::Comparison(Condition {
            left_field,
            operator: comparison_op,
            right_field,
        })),
        final_idx,
    ))
}

/// Process a power operation node (base ^ exponent)
fn process_pow_node(
    condition_array: &[Value],
    idx: usize,
    project_count: &mut i64,
    conv_object: &mut ConverterObject,
) -> Result<(ComplexField, usize), Box<ConversionError>> {
    let node = &condition_array[idx];

    // Get indices for base and exponent expressions
    let base_idx = node
        .get("left")
        .and_then(|b| b.as_u64())
        .ok_or_else(|| Box::new(ConversionError::MissingField("left".to_string())))?
        as usize;

    // Process base expression
    let (base_field, next_idx) = process_expression(
        condition_array,
        idx + 1 + base_idx,
        project_count,
        conv_object,
    )?;

    // Process exponent expression
    let (exponent_field, final_idx) =
        process_expression(condition_array, next_idx, project_count, conv_object)?;

    // Create a nested expression for the power operation
    let nested_expr = Box::new((base_field, "^".to_string(), exponent_field, true));

    Ok((
        ComplexField {
            column_ref: None,
            literal: None,
            aggregate: None,
            nested_expr: Some(nested_expr),
            subquery: None,
            subquery_vec: None,
        },
        final_idx,
    ))
}

/// Process an arithmetic operation node (Add, Subtract, Multiply, Divide)
fn process_arithmetic_node(
    condition_array: &[Value],
    idx: usize,
    op: &str,
    project_count: &mut i64,
    conv_object: &mut ConverterObject,
) -> Result<(ComplexField, usize), Box<ConversionError>> {
    let node = &condition_array[idx];

    // Get indices for left and right expressions
    let left_idx = node
        .get("left")
        .and_then(|l| l.as_u64())
        .ok_or_else(|| Box::new(ConversionError::MissingField("left".to_string())))?
        as usize;

    // Process left expression
    let (left_field, next_idx) = process_expression(
        condition_array,
        idx + 1 + left_idx,
        project_count,
        conv_object,
    )?;

    // Process right expression
    let (right_field, final_idx) =
        process_expression(condition_array, next_idx, project_count, conv_object)?;

    // Create a nested expression for the arithmetic operation
    let nested_expr = Box::new((left_field, op.to_string(), right_field, true));

    Ok((
        ComplexField {
            column_ref: None,
            literal: None,
            aggregate: None,
            nested_expr: Some(nested_expr),
            subquery: None,
            subquery_vec: None,
        },
        final_idx,
    ))
}

/// Process an attribute reference node using expression ID resolution
fn process_attribute_reference_node(
    condition_array: &[Value],
    idx: usize,
    conv_object: &mut ConverterObject,
) -> Result<(ComplexField, usize), Box<ConversionError>> {
    let node = &condition_array[idx];

    // Resolve column using expression ID
    let (_, column_name, source_name) = 
        conv_object.resolve_projection_column(node)
            .map_err(|_| Box::new(ConversionError::InvalidExpression))?;

    // Create a column reference with resolved information
    let column_ref = crate::dsl::ir::ColumnRef {
        table: Some(source_name),
        column: column_name,
    };

    Ok((
        ComplexField {
            column_ref: Some(column_ref),
            literal: None,
            aggregate: None,
            nested_expr: None,
            subquery: None,
            subquery_vec: None,
        },
        idx + 1,
    ))
}

/// Process a literal node
fn process_literal_node(
    condition_array: &[Value],
    idx: usize,
    conv_object: &mut ConverterObject,
) -> Result<(ComplexField, usize), Box<ConversionError>> {
    let node = &condition_array[idx];

    // Extract the literal value using the utility function
    let literal = conv_object.extract_literal_value(node)?;

    Ok((
        ComplexField {
            column_ref: None,
            literal: Some(literal),
            aggregate: None,
            nested_expr: None,
            subquery: None,
            subquery_vec: None,
        },
        idx + 1,
    ))
}

/// Process an expression to create a ComplexField
/// Returns the ComplexField and the next index to process
fn process_expression(
    condition_array: &[Value],
    idx: usize,
    project_count: &mut i64,
    conv_object: &mut ConverterObject,
) -> Result<(ComplexField, usize), Box<ConversionError>> {
    if idx >= condition_array.len() {
        return Err(Box::new(ConversionError::InvalidExpression));
    }

    let node = &condition_array[idx];

    // Get the class of the expression
    let class = node
        .get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    let expr_type = class
        .split('.')
        .last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    match expr_type {
        "AttributeReference" => process_attribute_reference_node(condition_array, idx, conv_object),
        "Literal" => process_literal_node(condition_array, idx, conv_object),
        "Add" => process_arithmetic_node(condition_array, idx, "+", project_count, conv_object),
        "Subtract" => {
            process_arithmetic_node(condition_array, idx, "-", project_count, conv_object)
        }
        "Multiply" => {
            process_arithmetic_node(condition_array, idx, "*", project_count, conv_object)
        }
        "Divide" => process_arithmetic_node(condition_array, idx, "/", project_count, conv_object),
        "Pow" => process_pow_node(condition_array, idx, project_count, conv_object),
        "Cast" => process_expression(condition_array, idx + 1, project_count, conv_object),
        "ScalarSubquery" => {
            // Process scalar subquery
            let complex_field =
                process_scalar_subquery(&condition_array[idx], project_count, conv_object)?;
            Ok((complex_field, idx + 1))
        }
        _ => Err(Box::new(ConversionError::UnsupportedExpressionType(
            expr_type.to_string(),
        ))),
    }
}