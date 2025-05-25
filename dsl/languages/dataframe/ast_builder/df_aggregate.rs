use serde_json::Value;
use std::sync::Arc;

use crate::dsl::ir::{ColumnRef, IrPlan};
use crate::dsl::languages::dataframe::conversion_error::ConversionError;

use super::df_project::process_project_agg;
use super::df_utils::ConverterObject;

pub(crate) fn process_aggregate(
    node: &Value,
    input_plan: Arc<IrPlan>,
    project_count: &mut usize,
    conv_object: &mut ConverterObject,
) -> Result<Arc<IrPlan>, Box<ConversionError>> {
    // Retrieve the grouping expressions array
    let grouping_expressions = node
        .get("groupingExpressions")
        .ok_or_else(|| {
            Box::new(ConversionError::MissingField(
                "groupingExpressions".to_string(),
            ))
        })?
        .as_array()
        .unwrap();

    // Retrieve the aggregate expressions array
    let aggregate_expressions = node
        .get("aggregateExpressions")
        .ok_or_else(|| {
            Box::new(ConversionError::MissingField(
                "aggregateExpressions".to_string(),
            ))
        })?
        .as_array()
        .unwrap();

    // Safety check for the aggregate expressions
    if aggregate_expressions.is_empty() {
        return Err(Box::new(ConversionError::InvalidGroupKeys(
            "Empty aggregate expressions".to_string(),
        )));
    }

    // If grouping expressions is empty, we have a projection with aggregates
    if grouping_expressions.is_empty() {
        // This is an aggregate without GROUP BY (e.g., SELECT COUNT(*) FROM table)
        process_project_agg(
            aggregate_expressions,
            input_plan,
            project_count,
            conv_object,
        )
    } else {
        // This is a GROUP BY with aggregates
        // Parse the grouping expressions using expr ID resolution
        let (group_keys, grouping_updates) = parse_grouping_expressions(grouping_expressions, conv_object)?;

        // Apply grouping expression updates to maintain expr ID mappings
        // Note: GROUP BY columns typically keep their original names and sources
        // since they're used for grouping, not projection
        if !grouping_updates.is_empty() {
            conv_object.update_projection_mappings(grouping_updates);
        }

        // Create the GROUP BY plan
        let group_plan = Arc::new(IrPlan::GroupBy {
            input: input_plan,
            keys: group_keys,
            group_condition: None,
        });

        // Process the aggregate expressions as a projection on top of the GROUP BY
        process_project_agg(
            aggregate_expressions,
            group_plan,
            project_count,
            conv_object,
        )
    }
}

/// Parse grouping expressions using expression ID resolution
/// Returns the group keys and any expr ID updates needed
fn parse_grouping_expressions(
    group_expressions: &[Value],
    conv_object: &mut ConverterObject,
) -> Result<(Vec<ColumnRef>, Vec<(usize, String, String)>), Box<ConversionError>> {
    let mut group_keys = Vec::new();
    let mut expr_updates = Vec::new();

    // Group expressions is an array of arrays containing the column references
    for group_expression in group_expressions {
        if let Some(group_expression_array) = group_expression.as_array() {
            for group_expression_item in group_expression_array {
                // Process each grouping column using expr ID resolution
                let (expr_id, column_name, source_name) = 
                    conv_object.resolve_projection_column(group_expression_item)
                        .map_err(|_| {
                            Box::new(ConversionError::InvalidGroupKeys(
                                "Failed to resolve grouping column expression ID".to_string(),
                            ))
                        })?;

                // Create column reference with resolved information
                let column_ref = ColumnRef {
                    table: Some(source_name.clone()),
                    column: column_name.clone(),
                };

                group_keys.push(column_ref);

                // For GROUP BY columns, we typically want to preserve their original
                // names and sources, but we still track them for consistency
                // The source name remains the same since GROUP BY doesn't change the source
                expr_updates.push((expr_id, column_name, source_name));
            }
        }
    }

    // Validate that we found at least one grouping column
    if group_keys.is_empty() {
        return Err(Box::new(ConversionError::InvalidGroupKeys(
            "No valid grouping columns found".to_string(),
        )));
    }

    Ok((group_keys, expr_updates))
}

/// Alternative parsing function for cases where we need to handle more complex grouping expressions
/// This would be used if the Catalyst plan contains expressions like GROUP BY col1 + col2
fn parse_complex_grouping_expressions(
    group_expressions: &[Value],
    conv_object: &mut ConverterObject,
) -> Result<(Vec<ColumnRef>, Vec<(usize, String, String)>), Box<ConversionError>> {
    let mut group_keys = Vec::new();
    let mut expr_updates = Vec::new();

    for group_expression in group_expressions {
        if let Some(group_expression_array) = group_expression.as_array() {
            for group_expression_item in group_expression_array {
                // Get the class of the expression to determine how to process it
                let class = group_expression_item
                    .get("class")
                    .and_then(|c| c.as_str())
                    .ok_or_else(|| {
                        Box::new(ConversionError::InvalidGroupKeys(
                            "Missing class in grouping expression".to_string(),
                        ))
                    })?;

                let expr_type = class
                    .split('.')
                    .last()
                    .ok_or_else(|| {
                        Box::new(ConversionError::InvalidGroupKeys(
                            "Invalid class format in grouping expression".to_string(),
                        ))
                    })?;

                match expr_type {
                    "AttributeReference" => {
                        // Simple column reference - use standard resolution
                        let (expr_id, column_name, source_name) = 
                            conv_object.resolve_projection_column(group_expression_item)
                                .map_err(|_| {
                                    Box::new(ConversionError::InvalidGroupKeys(
                                        "Failed to resolve grouping column".to_string(),
                                    ))
                                })?;

                        let column_ref = ColumnRef {
                            table: Some(source_name.clone()),
                            column: column_name.clone(),
                        };

                        group_keys.push(column_ref);
                        expr_updates.push((expr_id, column_name, source_name));
                    }
                    "Alias" => {
                        // Handle aliased expressions in GROUP BY
                        let alias_name = group_expression_item
                            .get("name")
                            .and_then(|n| n.as_str())
                            .ok_or_else(|| {
                                Box::new(ConversionError::InvalidGroupKeys(
                                    "Missing alias name in grouping expression".to_string(),
                                ))
                            })?
                            .to_string();

                        // Get the child expression
                        let child_idx = group_expression_item
                            .get("child")
                            .and_then(|c| c.as_u64())
                            .ok_or_else(|| {
                                Box::new(ConversionError::InvalidGroupKeys(
                                    "Missing child in alias grouping expression".to_string(),
                                ))
                            })? as usize;

                        if group_expression_array.len() > child_idx + 1 {
                            let child_expr = &group_expression_array[child_idx + 1];
                            let (expr_id, _original_name, source_name) = 
                                conv_object.resolve_projection_column(child_expr)
                                    .map_err(|_| {
                                        Box::new(ConversionError::InvalidGroupKeys(
                                            "Failed to resolve aliased grouping column".to_string(),
                                        ))
                                    })?;

                            let column_ref = ColumnRef {
                                table: Some(source_name.clone()),
                                column: alias_name.clone(),
                            };

                            group_keys.push(column_ref);
                            expr_updates.push((expr_id, alias_name, source_name));
                        }
                    }
                    _ => {
                        // For now, we don't support complex expressions in GROUP BY
                        // (like GROUP BY col1 + col2). This could be extended later.
                        return Err(Box::new(ConversionError::InvalidGroupKeys(
                            format!("Unsupported grouping expression type: {}", expr_type),
                        )));
                    }
                }
            }
        }
    }

    if group_keys.is_empty() {
        return Err(Box::new(ConversionError::InvalidGroupKeys(
            "No valid grouping columns found".to_string(),
        )));
    }

    Ok((group_keys, expr_updates))
}

/// Validate that grouping columns exist in the available sources
/// This is a helper function to ensure data integrity
fn validate_grouping_columns(
    group_keys: &[ColumnRef],
    conv_object: &ConverterObject,
) -> Result<(), Box<ConversionError>> {
    for column_ref in group_keys {
        // Check if the column exists in our expression mappings
        let column_exists = if let Some(table) = &column_ref.table {
            // Look for columns from this specific source
            conv_object
                .get_expr_ids_for_source(table)
                .iter()
                .any(|expr_id| {
                    if let Some((col_name, _)) = conv_object.get_column_name(expr_id)
                        .zip(conv_object.get_source_name(expr_id)) {
                        col_name == column_ref.column
                    } else {
                        false
                    }
                })
        } else {
            // Look for the column in any source
            conv_object.expr_to_source.values()
                .any(|(col_name, _)| col_name == &column_ref.column)
        };

        if !column_exists {
            return Err(Box::new(ConversionError::InvalidGroupKeys(
                format!("Grouping column '{}' not found in available sources", column_ref.column),
            )));
        }
    }

    Ok(())
}