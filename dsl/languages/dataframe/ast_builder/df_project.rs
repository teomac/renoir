use crate::dsl::ir::{
    AggregateFunction, AggregateType, ColumnRef, ComplexField, IrLiteral, IrPlan, ProjectionColumn
};
use crate::dsl::languages::dataframe::conversion_error::ConversionError;
use serde_json::Value;
use std::sync::Arc;

use super::df_utils::ConverterObject;

/// Process a Project (SELECT) node from a Catalyst plan
pub(crate) fn process_project(
    node: &Value,
    input_plan: Arc<IrPlan>,
    stream_index: &mut usize,
    _project_count: &mut usize,
    conv_object: &ConverterObject,
) -> Result<Arc<IrPlan>, Box<ConversionError>> {
    // Extract the project list
    let project_list = node
        .get("projectList")
        .and_then(|p| p.as_array())
        .ok_or_else(|| Box::new(ConversionError::MissingField("projectList".to_string())))?;

    let mut columns = Vec::new();

    // Process each projection list item
    for projection_array in project_list {
        if let Some(projections) = projection_array.as_array() {
            // Process the first expression in each projection array
            let projection_column =
                process_projection_array(projections, stream_index, conv_object)?;
            columns.push(projection_column);
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
    
    // Create the Project node
    Ok(Arc::new(IrPlan::Project {
        input: input_plan,
        columns,
        distinct: false,
    }))
}

/// Process a Project (SELECT) node from a Catalyst plan
pub(crate) fn process_project_agg(
    project_list: &[Value],
    input_plan: Arc<IrPlan>,
    stream_index: &mut usize,
    _project_count: &mut usize,
    conv_object: &ConverterObject,
) -> Result<Arc<IrPlan>, Box<ConversionError>> {
    let mut columns = Vec::new();

    // Process each projection list item
    for projection_array in project_list {
        if let Some(projections) = projection_array.as_array() {
            // Process the first expression in each projection array
            let projection_column =
                process_projection_array(projections, stream_index, conv_object)?;
            columns.push(projection_column);
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

    // Create the Project node
    Ok(Arc::new(IrPlan::Project {
        input: input_plan,
        columns,
        distinct: false,
    }))
}

fn process_projection_array(
    projection_array: &[Value],
    _stream_index: &mut usize,
    conv_object: &ConverterObject,
) -> Result<ProjectionColumn, Box<ConversionError>> {
    //check if the projection array is empty
    if projection_array.is_empty() {
        return Err(Box::new(ConversionError::InvalidExpression));
    }

    let mut projection_column = Vec::new();

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
                //check if the alias is auto generated or input by the user
                let has_alias = !expr
                    .get("nonInheritableMetadataKeys")
                    .and_then(|n| n.as_str())
                    .ok_or_else(|| {
                        Box::new(ConversionError::MissingField(
                            "nonInheritableMetadataKeys".to_string(),
                        ))
                    })?
                    .is_empty();

                // This is an aliased expression
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
                let (column, _) = process_expression(
                    projection_array,
                    (child_idx as usize) + 1,
                    alias_name,
                    conv_object,
                )?;
                projection_column.push(column);
            }
            _ => {
                // Directly process the expression
                let (column, _) = process_expression(projection_array, 0, None, conv_object)?;
                projection_column.push(column);
            }
        }
    }

    // if projection_column is empty, return an error
    if projection_column.is_empty() {
        return Err(Box::new(ConversionError::InvalidExpression));
    }
    // Return the first projection column
    Ok(projection_column[0].clone())
}

/// Process an expression to create a ProjectionColumn
/// Returns the ProjectionColumn and the next index to process
fn process_expression(
    expr_array: &[Value],
    idx: usize,
    alias: Option<String>,
    conv_object: &ConverterObject,
) -> Result<(ProjectionColumn, usize), Box<ConversionError>> {
    if idx >= expr_array.len() {
        return Err(Box::new(ConversionError::InvalidExpression));
    }
    let expr = &expr_array[idx];

    println!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");

    println!("Processing expression: {:?}", expr);
    println!("Expression index: {}", idx);

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
            // Process simple column reference
            println!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
            println!("Processing AttributeReference");

            let column_ref = conv_object.create_column_ref(expr)?;
            println!("Column reference: {:?}", column_ref);
            Ok((ProjectionColumn::Column(column_ref, alias), idx + 1))
        }
        "Literal" => {
            println!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
            println!("Processing Literal");
            // Process literal value
            let literal = conv_object.extract_literal_value(expr)?;
            println!("Literal value: {:?}", literal);
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
            ))
        }
        // Aggregate functions
        "AggregateExpression" => process_aggregate(expr_array, idx + 1, alias, conv_object),
        // Arithmetic operations
        "Add" | "Subtract" | "Multiply" | "Divide" | "Pow" => {
            let (complex_field, next_idx) =
                process_arithmetic_operation(expr_array, idx, expr_type, conv_object)?;
            Ok((
                ProjectionColumn::ComplexValue(complex_field, alias),
                next_idx,
            ))
        }
        "Cast" => {
            // For Cast operations, we'll process the child and maintain the same type
            let child_idx = expr
                .get("child")
                .and_then(|c| c.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("child".to_string())))?
                as usize;

            process_expression(expr_array, idx + child_idx + 1, alias, conv_object)
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
    conv_object: &ConverterObject,
) -> Result<(ProjectionColumn, usize), Box<ConversionError>> {
    let expr = &expr_array[idx];

    //Get the aggregate type
    let class = expr
        .get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    let agg_type = class
        .split('.')
        .last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    // Get the aggregate type
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
    // Get the child index
    let child_idx = idx + 1;

    let child = &expr_array[child_idx];

    //Get the class of the child
    let child_class = child
        .get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    let child_type = child_class
        .split('.')
        .last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    //Check if the child is an AttributeReference or a Literal
    if child_type == "AttributeReference" {
        // Process the child expression to get the column reference
        let column_ref = conv_object.create_column_ref(child)?;

        let agg_func = AggregateFunction {
            function: aggregate_type,
            column: column_ref,
        };

        Ok((ProjectionColumn::Aggregate(agg_func, alias), child_idx + 1))
    }
    else if child_type == "Literal" {
        // Process literal value
        let literal = conv_object.extract_literal_value(child)?;

        //the only case in which we have a literal is when we have a count(*) operation
        //so check if the aggregate type is count
        if aggregate_type != AggregateType::Count || literal != IrLiteral::Integer(1) {
            return Err(Box::new(ConversionError::UnsupportedExpressionType(
                agg_type.to_string(),
            )));
        } else{
            //create an aggregate function for count(*)
            let column_ref = ColumnRef {
                table: None,
                column: "*".to_string(),
            };
            let agg_func = AggregateFunction {
                function: AggregateType::Count,
                column: column_ref,
            };
            return Ok((
                ProjectionColumn::Aggregate(agg_func, alias),
                child_idx + 1,
            ));
        }
    }
    else{
        return Err(Box::new(ConversionError::UnsupportedExpressionType(
            agg_type.to_string(),
        )));
    }
}

/// Process an arithmetic operation node (Add, Subtract, Multiply, Divide, Pow)
fn process_arithmetic_operation(
    expr_array: &[Value],
    idx: usize,
    op_type: &str,
    conv_object: &ConverterObject,
) -> Result<(ComplexField, usize), Box<ConversionError>> {
    let expr = &expr_array[idx];

    println!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");

    println!("Processing arithmetic operation: {:?}", expr);
    println!("Operation index: {}", idx);

    // Get indices for left and right operands
    let left_idx = expr
        .get("left")
        .and_then(|l| l.as_u64())
        .ok_or_else(|| Box::new(ConversionError::MissingField("left".to_string())))?
        as usize;

    // Process left operand
    let (left_field, left_next_idx) =
        process_complex_field(expr_array, idx + left_idx + 1, conv_object)?;

    println!("Left field: {:?}", left_field);
    println!("Left next index: {}", left_next_idx);

    // Convert operation type to operator string
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

    // Process right operand
    let (right_field, right_next_idx) =
        process_complex_field(expr_array, left_next_idx, conv_object)?;

    // Create the nested expression
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
    ))
}

/// Process an expression node into a ComplexField
fn process_complex_field(
    expr_array: &[Value],
    idx: usize,
    conv_object: &ConverterObject,
) -> Result<(ComplexField, usize), Box<ConversionError>> {
    if idx >= expr_array.len() {
        return Err(Box::new(ConversionError::InvalidExpression));
    }

    let expr = &expr_array[idx];

    println!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    println!("Processing complex field: {:?}", expr);
    println!("Complex field index: {}", idx);

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
            // Simple column reference
            let column_ref = conv_object.create_column_ref(expr)?;

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
        "Literal" => {
            // Literal value
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
            ))
        }
        // Aggregate functions
        "Sum" | "Min" | "Max" | "Avg" | "Count" => {
            process_aggregate_field(expr_array, idx, expr_type, conv_object)
        }
        // Arithmetic operations
        "Add" | "Subtract" | "Multiply" | "Divide" | "Pow" => {
            process_arithmetic_operation(expr_array, idx, expr_type, conv_object)
        }
        "Cast" => {
            // For Cast operations, we'll process the child
            let child_idx = expr
                .get("child")
                .and_then(|c| c.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("child".to_string())))?
                as usize;

            process_complex_field(expr_array, idx + child_idx + 1, conv_object)
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
    agg_type: &str,
    conv_object: &ConverterObject,
) -> Result<(ComplexField, usize), Box<ConversionError>> {
    let expr = &expr_array[idx];

    // Get the child index
    let child_idx = expr
        .get("child")
        .and_then(|c| c.as_u64())
        .ok_or_else(|| Box::new(ConversionError::MissingField("child".to_string())))?
        as usize;

    // Get the aggregate type
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

    // Special handling for COUNT(*) which might not have a child expression
    if aggregate_type == AggregateType::Count && expr.get("isDistinct").is_some() {
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
        ));
    }

    // Process the child expression to get the column reference
    let child_expr = &expr_array[idx + child_idx + 1];
    let column_ref = conv_object.create_column_ref(child_expr)?;

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
        idx + child_idx + 2,
    ))
}
