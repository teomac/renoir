use crate::dsl::ir::{
    AggregateFunction, AggregateType, ColumnRef, ComplexField, IrLiteral, IrPlan, ProjectionColumn,
};
use crate::dsl::languages::dataframe::ast_builder::df_subqueries::process_scalar_subquery;
use crate::dsl::languages::dataframe::conversion_error::ConversionError;
use serde_json::Value;
use std::sync::Arc;

use super::df_utils::ConverterObject;

/// Process a Project (SELECT) node from a Catalyst plan
pub(crate) fn process_project(
    node: &Value,
    input_plan: Arc<IrPlan>,
    project_count: &i64,
    conv_object: &mut ConverterObject,
) -> Result<Arc<IrPlan>, Box<ConversionError>> {
    // Get the stream name for this projection
    let stream_name = conv_object.increment_and_get_stream_name(*project_count);
    // Extract the project list
    let project_list = node
        .get("projectList")
        .and_then(|p| p.as_array())
        .ok_or_else(|| Box::new(ConversionError::MissingField("projectList".to_string())))?;

    let mut columns = Vec::new();
    let mut projection_updates = Vec::new(); // Track expr ID updates

    // Determine if we need auto-aliases (only when child is a Join node)
    let needs_auto_aliases = matches!(&*input_plan, IrPlan::Join { .. });

    // Process each projection list item
    for projection_array in project_list {
        if let Some(projections) = projection_array.as_array() {
            // Process the first expression in each projection array
            let (projection_column, expr_updates) = process_projection_array(
                projections,
                project_count,
                needs_auto_aliases,
                conv_object,
            )?;
            columns.push(projection_column);
            projection_updates.extend(expr_updates);
        }
    }

    // If no columns were processed, add a wildcard projection
    if columns.is_empty() {
        columns.push(ProjectionColumn::Column(
            ColumnRef {
                table: None,
                column: "*".to_string(),
            },
            None,
        ));
    }

    // Update projection expr IDs - but only for the expressions that are actually being projected
    // and only update the ones that correspond to the new stream we're creating
    for (expr_id, new_column_name, _) in &projection_updates {
        // Only update the source to the new stream name for expressions that are being projected
        conv_object
            .expr_to_source
            .insert(*expr_id, (new_column_name.clone(), stream_name.clone()));
    }

    let project_node = Arc::new(IrPlan::Project {
        input: input_plan,
        columns,
        distinct: false,
    });

    if *project_count > 1 {
        // Create the Scan node with the same stream name
        Ok(Arc::new(IrPlan::Scan {
            input: project_node,
            stream_name: stream_name.clone(),
            alias: Some(stream_name),
        }))
    } else {
        Ok(project_node.clone())
    }
}

/// Process a Project (SELECT) node from a Catalyst plan for aggregates
pub(crate) fn process_project_agg(
    project_list: &[Value],
    input_plan: Arc<IrPlan>,
    project_count: &i64,
    conv_object: &mut ConverterObject,
) -> Result<Arc<IrPlan>, Box<ConversionError>> {
    let mut columns = Vec::new();
    let mut projection_updates = Vec::new();
    let stream_name = conv_object.increment_and_get_stream_name(*project_count);

    // Check if we need auto-aliases (only when input is a Join node)
    let needs_auto_aliases = matches!(&*input_plan, IrPlan::Join { .. });

    // Process each projection list item
    for projection_array in project_list {
        if let Some(projections) = projection_array.as_array() {
            let (projection_column, expr_updates) = process_projection_array(
                projections,
                project_count,
                needs_auto_aliases,
                conv_object,
            )?;
            columns.push(projection_column);
            projection_updates.extend(expr_updates);
        }
    }

    if columns.is_empty() {
        columns.push(ProjectionColumn::Column(
            ColumnRef {
                table: None,
                column: "*".to_string(),
            },
            None,
        ));
    }

    // Update projection expr IDs - but only for the expressions that are actually being projected
    // and only update the ones that correspond to the new stream we're creating
    for (expr_id, new_column_name, _) in &projection_updates {
        // Only update the source to the new stream name for expressions that are being projected
        conv_object
            .expr_to_source
            .insert(*expr_id, (new_column_name.clone(), stream_name.clone()));
    }

    // For aggregates, we don't create a new scan node immediately
    // The calling function will handle that
    let project_node = Arc::new(IrPlan::Project {
        input: input_plan,
        columns,
        distinct: false,
    });

    if *project_count > 1 {
        // Create the Scan node with the same stream name
        Ok(Arc::new(IrPlan::Scan {
            input: project_node,
            stream_name: stream_name.clone(),
            alias: Some(stream_name),
        }))
    } else {
        Ok(project_node.clone())
    }
}

fn process_projection_array(
    projection_array: &[Value],
    project_count: &i64,
    needs_auto_aliases: bool,
    conv_object: &mut ConverterObject,
) -> Result<(ProjectionColumn, Vec<(usize, String, String)>), Box<ConversionError>> {
    if projection_array.is_empty() {
        return Err(Box::new(ConversionError::InvalidExpression));
    }

    let mut expr_updates = Vec::new();

    if let Some(expr) = projection_array.first() {
        let class = expr
            .get("class")
            .and_then(|c| c.as_str())
            .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

        let expr_type = class
            .split('.')
            .last()
            .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

        match expr_type {
            "Alias" => {
                // Check if the alias is auto generated or input by the user
                let has_alias = expr
                    .get("nonInheritableMetadataKeys")
                    .map(|keys| {
                        if keys.is_array() {
                            keys.as_array().map(|arr| !arr.is_empty()).unwrap_or(false)
                        } else if keys.is_string() {
                            !keys.as_str().unwrap_or("").is_empty()
                        } else {
                            false
                        }
                    })
                    .unwrap_or(false);

                let alias_name = if has_alias {
                    Some(
                        expr.get("name")
                            .and_then(|n| n.as_str())
                            .ok_or_else(|| {
                                Box::new(ConversionError::MissingField("name".to_string()))
                            })?
                            .to_string()
                            .replace(" ", ""),
                    )
                } else {
                    None
                };

                // Get the child expression index
                let child_idx = expr
                    .get("child")
                    .and_then(|c| c.as_u64())
                    .ok_or_else(|| Box::new(ConversionError::MissingField("child".to_string())))?;

                // Process the child expression with the alias
                let (column, _, mut child_updates) = process_expression(
                    projection_array,
                    (child_idx as usize) + 1,
                    project_count,
                    alias_name,
                    needs_auto_aliases,
                    conv_object,
                )?;

                // If this is an alias, we need to update the expr ID for the alias itself
                if let Ok(alias_expr_id) = ConverterObject::extract_expr_id(expr) {
                    let final_column_name = match &column {
                        ProjectionColumn::Column(_, Some(alias)) => alias.clone(),
                        ProjectionColumn::Column(col_ref, None) => col_ref.column.clone(),
                        ProjectionColumn::Aggregate(_, Some(alias)) => alias.clone(),
                        ProjectionColumn::Aggregate(agg, None) => {
                            format!(
                                "{}_{}",
                                agg.function.to_string().to_lowercase(),
                                agg.column.column
                            )
                        }
                        ProjectionColumn::ComplexValue(_, Some(alias)) => alias.clone(),
                        ProjectionColumn::ComplexValue(_, None) => {
                            format!("expr_{}", alias_expr_id)
                        }
                        _ => format!("col_{}", alias_expr_id),
                    };

                    child_updates.push((
                        alias_expr_id,
                        final_column_name,
                        "placeholder".to_string(),
                    ));
                }

                expr_updates.extend(child_updates);
                Ok((column, expr_updates))
            }
            _ => {
                // Directly process the expression
                let (column, _, updates) = process_expression(
                    projection_array,
                    0,
                    project_count,
                    None,
                    needs_auto_aliases,
                    conv_object,
                )?;
                expr_updates.extend(updates);
                Ok((column, expr_updates))
            }
        }
    } else {
        Err(Box::new(ConversionError::InvalidExpression))
    }
}

/// Process an expression to create a ProjectionColumn
/// Returns the ProjectionColumn, expression updates, and the next index to process
fn process_expression(
    expr_array: &[Value],
    idx: usize,
    project_count: &i64,
    alias: Option<String>,
    needs_auto_aliases: bool,
    conv_object: &mut ConverterObject,
) -> Result<(ProjectionColumn, usize, Vec<(usize, String, String)>), Box<ConversionError>> {
    if idx >= expr_array.len() {
        return Err(Box::new(ConversionError::InvalidExpression));
    }
    let expr = &expr_array[idx];
    let mut expr_updates = Vec::new();

    // Extract the expression type
    let class = expr
        .get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    let expr_type = class
        .split('.')
        .last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    match expr_type {
        "AttributeReference" => {
            // Process simple column reference using expr ID resolution
            let (expr_id, original_column, original_source) =
                conv_object.resolve_projection_column(expr)?;

            // Determine final column name and alias
            let column_alias = if let Some(alias_name) = alias {
                // User provided alias - use as-is
                Some(alias_name.clone())
            } else if needs_auto_aliases {
                // Auto-generate alias for join projections
                let auto_alias = conv_object.generate_auto_alias(
                    &original_column,
                    &original_source,
                    needs_auto_aliases,
                );
                Some(auto_alias.clone())
            } else {
                // No alias needed
                None
            };

            // Create column reference with current source info
            let column_ref = ColumnRef {
                table: Some(original_source.clone()),
                column: original_column.clone(),
            };

            // For the expression updates, use the final column name (alias if present, otherwise original)
            let final_column_name = column_alias.clone().unwrap_or(original_column);

            // Only track expression IDs that are actually being projected in this step
            // This prevents overwriting expression IDs that shouldn't be updated
            expr_updates.push((expr_id, final_column_name, "placeholder".to_string()));

            Ok((
                ProjectionColumn::Column(column_ref, column_alias),
                idx + 1,
                expr_updates,
            ))
        }
        "Literal" => {
            // Process literal value
            let literal = conv_object.extract_literal_value(expr)?;
            let complex_field = ComplexField {
                column_ref: None,
                literal: Some(literal),
                aggregate: None,
                nested_expr: None,
                subquery: None,
                subquery_vec: None,
            };
            Ok((
                ProjectionColumn::ComplexValue(complex_field, alias),
                idx + 1,
                expr_updates,
            ))
        }
        "AggregateExpression" => {
            process_aggregate(expr_array, idx + 1, alias, needs_auto_aliases, conv_object)
        }
        "Add" | "Subtract" | "Multiply" | "Divide" | "Pow" => {
            let (complex_field, next_idx, updates) = process_arithmetic_operation(
                expr_array,
                idx,
                expr_type,
                needs_auto_aliases,
                conv_object,
            )?;
            expr_updates.extend(updates);
            Ok((
                ProjectionColumn::ComplexValue(complex_field, alias),
                next_idx,
                expr_updates,
            ))
        }
        "Cast" => {
            // For Cast operations, process the child
            let child_idx = expr
                .get("child")
                .and_then(|c| c.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("child".to_string())))?
                as usize;

            process_expression(
                expr_array,
                idx + child_idx + 1,
                project_count,
                alias,
                needs_auto_aliases,
                conv_object,
            )
        }
        "ScalarSubquery" => {
            // Process scalar subquery
            let complex_field =
                process_scalar_subquery(&expr_array[idx], project_count, conv_object)?;
            Ok((
                ProjectionColumn::ComplexValue(complex_field, alias),
                idx + 1,
                expr_updates,
            ))
        }
        _ => Err(Box::new(ConversionError::UnsupportedExpressionType(
            expr_type.to_string(),
        ))),
    }
}

/// Process an aggregate function expression
fn process_aggregate(
    expr_array: &[Value],
    idx: usize,
    alias: Option<String>,
    needs_auto_aliases: bool,
    conv_object: &mut ConverterObject,
) -> Result<(ProjectionColumn, usize, Vec<(usize, String, String)>), Box<ConversionError>> {
    let expr = &expr_array[idx];
    let mut expr_updates = Vec::new();

    let class = expr
        .get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    let agg_type = class
        .split('.')
        .last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    println!("Processing aggregate type: {}", agg_type);

    let aggregate_type = match agg_type {
        "Sum" => AggregateType::Sum,
        "Min" => AggregateType::Min,
        "Max" => AggregateType::Max,
        "Avg" => AggregateType::Avg,
        "Count" => AggregateType::Count,
        "Average" => AggregateType::Avg,
        _ => {
            return Err(Box::new(ConversionError::UnsupportedExpressionType(
                agg_type.to_string(),
            )))
        }
    };

    println!("Aggregate type resolved: {:?}", aggregate_type);

    let child_idx = idx + 1;
    let child = &expr_array[child_idx];

    let child_class = child
        .get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    let child_type = child_class
        .split('.')
        .last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    if child_type == "AttributeReference" {
        // Resolve the column using expr ID
        let (_, original_column, original_source) = conv_object.resolve_projection_column(child)?;

        let column_ref = ColumnRef {
            table: Some(original_source.clone()),
            column: original_column.clone(),
        };

        let agg_func = AggregateFunction {
            function: aggregate_type.clone(),
            column: column_ref,
        };

        // Determine final alias for the aggregate
        let final_alias = if let Some(alias_name) = alias {
            Some(alias_name)
        } else if needs_auto_aliases {
            Some(format!(
                "{}_{}_{}",
                aggregate_type.to_string().to_lowercase(),
                original_column,
                original_source
            ))
        } else {
            Some(format!(
                "{}_{}",
                aggregate_type.to_string().to_lowercase(),
                original_column
            ))
        };

        // Track the aggregate result for expr ID updates if there's an expression ID
        if let Ok(agg_expr_id) = ConverterObject::extract_expr_id(expr) {
            let agg_name = final_alias.clone().unwrap_or_else(|| {
                format!(
                    "{}_{}",
                    aggregate_type.to_string().to_lowercase(),
                    original_column
                )
            });
            expr_updates.push((agg_expr_id, agg_name, "placeholder".to_string()));
        }

        Ok((
            ProjectionColumn::Aggregate(agg_func, final_alias),
            child_idx + 1,
            expr_updates,
        ))
    } else if child_type == "Literal" {
        let literal = conv_object.extract_literal_value(child)?;

        if aggregate_type != AggregateType::Count || literal != IrLiteral::Integer(1) {
            return Err(Box::new(ConversionError::UnsupportedExpressionType(
                agg_type.to_string(),
            )));
        } else {
            let column_ref = ColumnRef {
                table: None,
                column: "*".to_string(),
            };
            let agg_func = AggregateFunction {
                function: AggregateType::Count,
                column: column_ref,
            };

            let final_alias = alias.or_else(|| Some("count_star".to_string()));

            // Track count(*) for expr ID updates
            if let Ok(agg_expr_id) = ConverterObject::extract_expr_id(expr) {
                expr_updates.push((
                    agg_expr_id,
                    "count_star".to_string(),
                    "placeholder".to_string(),
                ));
            }

            return Ok((
                ProjectionColumn::Aggregate(agg_func, final_alias),
                child_idx + 1,
                expr_updates,
            ));
        }
    } else {
        return Err(Box::new(ConversionError::UnsupportedExpressionType(
            agg_type.to_string(),
        )));
    }
}

/// Process an arithmetic operation node
fn process_arithmetic_operation(
    expr_array: &[Value],
    idx: usize,
    op_type: &str,
    needs_auto_aliases: bool,
    conv_object: &mut ConverterObject,
) -> Result<(ComplexField, usize, Vec<(usize, String, String)>), Box<ConversionError>> {
    let expr = &expr_array[idx];
    let mut expr_updates = Vec::new();

    let left_idx = expr
        .get("left")
        .and_then(|l| l.as_u64())
        .ok_or_else(|| Box::new(ConversionError::MissingField("left".to_string())))?
        as usize;

    let (left_field, left_next_idx, left_updates) = process_complex_field(
        expr_array,
        idx + left_idx + 1,
        needs_auto_aliases,
        conv_object,
    )?;
    expr_updates.extend(left_updates);

    let operator = match op_type {
        "Add" => "+",
        "Subtract" => "-",
        "Multiply" => "*",
        "Divide" => "/",
        "Pow" => "^",
        _ => {
            return Err(Box::new(ConversionError::UnsupportedExpressionType(
                op_type.to_string(),
            )))
        }
    };

    let (right_field, right_next_idx, right_updates) =
        process_complex_field(expr_array, left_next_idx, needs_auto_aliases, conv_object)?;
    expr_updates.extend(right_updates);

    let nested_expr = Box::new((left_field, operator.to_string(), right_field, true));

    Ok((
        ComplexField {
            column_ref: None,
            literal: None,
            aggregate: None,
            nested_expr: Some(nested_expr),
            subquery: None,
            subquery_vec: None,
        },
        left_next_idx.max(right_next_idx),
        expr_updates,
    ))
}

/// Process an expression node into a ComplexField
fn process_complex_field(
    expr_array: &[Value],
    idx: usize,
    needs_auto_aliases: bool,
    conv_object: &mut ConverterObject,
) -> Result<(ComplexField, usize, Vec<(usize, String, String)>), Box<ConversionError>> {
    if idx >= expr_array.len() {
        return Err(Box::new(ConversionError::InvalidExpression));
    }

    let expr = &expr_array[idx];
    let mut expr_updates = Vec::new();

    let class = expr
        .get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    let expr_type = class
        .split('.')
        .last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    match expr_type {
        "AttributeReference" => {
            // Resolve using expr ID
            let (expr_id, original_column, original_source) =
                conv_object.resolve_projection_column(expr)?;

            let column_ref = ColumnRef {
                table: Some(original_source.clone()),
                column: original_column.clone(),
            };

            // Track for updates
            expr_updates.push((expr_id, original_column, "placeholder".to_string()));

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
                expr_updates,
            ))
        }
        "Literal" => {
            let literal = conv_object.extract_literal_value(expr)?;
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
                expr_updates,
            ))
        }
        "Sum" | "Min" | "Max" | "Avg" | "Count" => {
            process_aggregate_field(expr_array, idx, conv_object)
        }
        "Add" | "Subtract" | "Multiply" | "Divide" | "Pow" => process_arithmetic_operation(
            expr_array,
            idx,
            expr_type,
            needs_auto_aliases,
            conv_object,
        ),
        "Cast" => {
            let child_idx = expr
                .get("child")
                .and_then(|c| c.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("child".to_string())))?
                as usize;

            process_complex_field(
                expr_array,
                idx + child_idx + 1,
                needs_auto_aliases,
                conv_object,
            )
        }
        "AggregateExpression" => {
            // Extract the actual aggregate function from the AggregateExpression wrapper
            let agg_func_idx = expr
                .get("aggregateFunction")
                .and_then(|af| af.as_u64())
                .ok_or_else(|| {
                    Box::new(ConversionError::MissingField(
                        "aggregateFunction".to_string(),
                    ))
                })? as usize;

            // Process the actual aggregate function
            process_aggregate_field(expr_array, idx + agg_func_idx + 1, conv_object)
        }
        _ => Err(Box::new(ConversionError::UnsupportedExpressionType(
            expr_type.to_string(),
        ))),
    }
}

/// Process an aggregate function into a ComplexField
fn process_aggregate_field(
    expr_array: &[Value],
    idx: usize,
    conv_object: &mut ConverterObject,
) -> Result<(ComplexField, usize, Vec<(usize, String, String)>), Box<ConversionError>> {
    let expr = &expr_array[idx];
    let expr_updates = Vec::new();
    let child_expr = &expr_array[idx + 1];

    let agg_type = expr
        .get("class")
        .and_then(|at| at.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?
        .split('.')
        .last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    println!("Processing aggregate type: {}", agg_type);

    let aggregate_type = match agg_type {
        "Sum" => AggregateType::Sum,
        "Min" => AggregateType::Min,
        "Max" => AggregateType::Max,
        "Avg" => AggregateType::Avg,
        "Count" => AggregateType::Count,
        _ => {
            return Err(Box::new(ConversionError::UnsupportedExpressionType(
                agg_type.to_string(),
            )))
        }
    };


    if aggregate_type == AggregateType::Count
        && child_expr
            .get("class")
            .and_then(|at| at.as_str())
            .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?
            .split('.')
            .last()
            == Some("Literal")
    {
        println!("Child expression: {:?}", child_expr);

        let column_ref = ColumnRef {
            table: None,
            column: "*".to_string(),
        };

        let agg_func = AggregateFunction {
            function: AggregateType::Count,
            column: column_ref,
        };

        return Ok((
            ComplexField {
                column_ref: None,
                literal: None,
                aggregate: Some(agg_func),
                nested_expr: None,
                subquery: None,
                subquery_vec: None,
            },
            idx + 1,
            expr_updates,
        ));
    }

    // Resolve child column using expr ID

    println!("Processing child expression: {:?}", child_expr);
    let (_, original_column, original_source) =
        conv_object.resolve_projection_column(child_expr)?;

    let column_ref = ColumnRef {
        table: Some(original_source),
        column: original_column,
    };

    let agg_func = AggregateFunction {
        function: aggregate_type,
        column: column_ref,
    };

    Ok((
        ComplexField {
            column_ref: None,
            literal: None,
            aggregate: Some(agg_func),
            nested_expr: None,
            subquery: None,
            subquery_vec: None,
        },
        idx + 2,
        expr_updates,
    ))
}
