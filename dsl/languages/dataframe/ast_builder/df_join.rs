use std::sync::Arc;

use serde_json::Value;

use crate::dsl::{
    ir::{ColumnRef, FilterClause, IrPlan, JoinCondition, JoinType, ProjectionColumn},
    languages::dataframe::{conversion_error::ConversionError, converter::process_node},
};

use super::df_utils::ConverterObject;

pub(crate) fn process_join(
    node: &Value,
    left_child: Arc<IrPlan>,
    right_child: Arc<IrPlan>,
    conv_object: &mut ConverterObject,
) -> Result<Arc<IrPlan>, Box<ConversionError>> {
    let join_type = node
        .get("joinType")
        .ok_or_else(|| Box::new(ConversionError::MissingField("joinType".to_string())))
        .unwrap();

    let join_type_str = join_type
        .get("object")
        .and_then(|o| o.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidJoinType))?
        .split('.')
        .last()
        .ok_or_else(|| Box::new(ConversionError::InvalidJoinType))?;

    let join_type_final = match join_type_str {
        "Inner$" => JoinType::Inner,
        "LeftOuter$" => JoinType::Left,
        "FullOuter$" => JoinType::Outer,
        "LeftSemi$" => JoinType::Left,
        _ => {
            return Err(Box::new(ConversionError::UnsupportedJoinType(
                join_type_str.to_string(),
            )))
        }
    };

    // Extract the condition array
    let condition_array = node
        .get("condition")
        .and_then(|c| c.as_array())
        .ok_or_else(|| Box::new(ConversionError::MissingField("condition".to_string())))?;

    // If condition array is empty, use an empty join condition
    if condition_array.is_empty() {
        return Ok(Arc::new(IrPlan::Join {
            left: left_child,
            right: right_child,
            join_type: join_type_final,
            condition: Vec::new(), // Empty condition
        }));
    }

    // Try to extract simple equality conditions for the join
    let (simple_conditions, complex_conditions) = extract_join_conditions(condition_array, conv_object)?;

    // Create the join with only simple equality conditions
    let join_node = Arc::new(IrPlan::Join {
        left: left_child,
        right: right_child,
        join_type: join_type_final,
        condition: simple_conditions,
    });

    // If there are complex conditions, add them as separate filter operations after the join
    if complex_conditions.is_empty() {
        Ok(join_node)
    } else {
        // Create a filter node for each complex condition
        let mut current_node = join_node;
        for condition in complex_conditions {
            current_node = Arc::new(IrPlan::Filter {
                input: current_node,
                predicate: condition,
            });
        }
        Ok(current_node)
    }
}

// Function to separate simple equality join conditions from complex conditions
fn extract_join_conditions(
    condition_array: &[Value],
    conv_object: &mut ConverterObject,
) -> Result<(Vec<JoinCondition>, Vec<FilterClause>), Box<ConversionError>> {
    let mut simple_conditions = Vec::new();
    let mut complex_conditions = Vec::new();

    let root_node = &condition_array[0];
    let class = root_node
        .get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    let node_type = class
        .split('.')
        .last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    match node_type {
        "And" => {
            // Process AND condition - loop through all conditions
            for (i, node) in condition_array.iter().enumerate().skip(1) {
                if i >= condition_array.len() {
                    break;
                }

                if let Some(class) = node.get("class").and_then(|c| c.as_str()) {
                    let expr_type = class.split('.').last().unwrap_or("");
                    
                    // Only process EqualTo as simple join conditions
                    if expr_type == "EqualTo" {
                        if let Ok((join_condition, _)) = process_simple_equality(condition_array, i, conv_object) {
                            simple_conditions.push(join_condition[0].clone());
                        } else {
                            // If it's not a simple equality between column references,
                            // add it as a complex condition
                            if let Ok((filter_clause, _)) = 
                                crate::dsl::languages::dataframe::ast_builder::df_filter::process_condition_node(
                                    condition_array, i,  0, conv_object
                                ) {
                                complex_conditions.push(filter_clause);
                            }
                        }
                    } else if expr_type == "GreaterThan" || expr_type == "LessThan" || 
                              expr_type == "GreaterThanOrEqual" || expr_type == "LessThanOrEqual" {
                        // These operators are always complex conditions
                        if let Ok((filter_clause, _)) = 
                            crate::dsl::languages::dataframe::ast_builder::df_filter::process_condition_node(
                                condition_array, i,  0, conv_object
                            ) {
                            complex_conditions.push(filter_clause);
                        }
                    }
                }
            }
        },
        "EqualTo" => {
            // Single equality condition
            if let Ok((join_condition, _)) = process_simple_equality(condition_array, 0, conv_object) {
                simple_conditions.push(join_condition[0].clone());
            } else {
                // Not a simple equality
                if let Ok((filter_clause, _)) = 
                    crate::dsl::languages::dataframe::ast_builder::df_filter::process_condition_node(
                        condition_array, 0, 0, conv_object
                    ) {
                    complex_conditions.push(filter_clause);
                }
            }
        },
        _ => {
            // Any other condition is treated as complex
            if let Ok((filter_clause, _)) = 
                crate::dsl::languages::dataframe::ast_builder::df_filter::process_condition_node(
                    condition_array, 0,  0, conv_object
                ) {
                complex_conditions.push(filter_clause);
            }
        }
    }

    Ok((simple_conditions, complex_conditions))
}

// Process only simple equality joins between column references using expr ID resolution
fn process_simple_equality(
    condition_array: &[Value],
    idx: usize,
    conv_object: &mut ConverterObject,
) -> Result<(Vec<JoinCondition>, usize), Box<ConversionError>> {
    let node = &condition_array[idx];

    // Ensure it's an equality operation
    let class = node
        .get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    let node_type = class
        .split('.')
        .last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    if node_type != "EqualTo" {
        return Err(Box::new(ConversionError::UnsupportedExpressionType(
            format!("Expected EqualTo, got {}", node_type)
        )));
    }

    // Get the left and right expressions
    let left_idx = node
        .get("left")
        .and_then(|l| l.as_u64())
        .ok_or_else(|| Box::new(ConversionError::MissingField("left".to_string())))?
        as usize;

    // Get the child nodes
    let left_child_idx = idx + left_idx + 1;
    if left_child_idx >= condition_array.len() {
        return Err(Box::new(ConversionError::InvalidExpression));
    }
    
    let left_node = &condition_array[left_child_idx];
    
    // Ensure the left side is an attribute reference
    let left_class = left_node
        .get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    let left_type = left_class
        .split('.')
        .last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    if left_type != "AttributeReference" {
        return Err(Box::new(ConversionError::UnsupportedExpressionType(
            format!("Complex join conditions not supported. Left side is: {}", left_type)
        )));
    }

    // Process the left side using expr ID resolution
    let (_, left_column_name, left_source_name) = 
        conv_object.resolve_projection_column(left_node)
            .map_err(|_| Box::new(ConversionError::InvalidExpression))?;

    let left_col = ColumnRef {
        table: Some(left_source_name),
        column: left_column_name,
    };
    
    // Calculate right index
    let right_idx = left_child_idx + 1;
    if right_idx >= condition_array.len() {
        return Err(Box::new(ConversionError::InvalidExpression));
    }
    
    let right_node = &condition_array[right_idx];
    
    // Ensure the right side is also an attribute reference
    let right_class = right_node
        .get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    let right_type = right_class
        .split('.')
        .last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    if right_type != "AttributeReference" {
        return Err(Box::new(ConversionError::UnsupportedExpressionType(
            format!("Complex join conditions not supported. Right side is: {}", right_type)
        )));
    }
    
    // Process the right side using expr ID resolution
    let (_, right_column_name, right_source_name) = 
        conv_object.resolve_projection_column(right_node)
            .map_err(|_| Box::new(ConversionError::InvalidExpression))?;

    let right_col = ColumnRef {
        table: Some(right_source_name),
        column: right_column_name,
    };

    Ok((
        vec![JoinCondition { left_col, right_col }],
        right_idx + 1,
    ))
}

pub(crate) fn process_join_child(
    child_index: usize,
    full_plan: &[Value],
    project_count: &mut i64,
    conv_object: &mut ConverterObject,
) -> Result<(Arc<IrPlan>, usize), Box<ConversionError>> {
    
    // Process the child node using the process_node function
    let child_ir = process_node(
        full_plan,
        child_index,
        project_count,
        conv_object,
    )?;

    let processed_child_node = match &*child_ir.0 {
        // If the child node is a Project node, we need to create a Scan node with the project as input
        IrPlan::Project { columns, .. } => {
            // Create a Scan node with the project as input
            // Extract alias from the first column that has a table reference
            let alias = extract_alias_from_columns(columns);
            
            // Get the next stream name for this join child
            let stream_name = if let Some(last) = conv_object.stream_names.last() {
                last.clone()
            } else {
                conv_object.increment_and_get_stream_name(*project_count)
            };
            
            let scan_node = IrPlan::Scan {
                input: child_ir.0,
                stream_name: stream_name.clone(),
                alias,
            };
            Arc::new(scan_node)
        },
        _ => {
            // For any other type of node, just return it as is
            child_ir.0.clone()
        }
    };

    // Return the processed child node and the updated index
    Ok((processed_child_node, child_ir.1))
}

/// Extract alias from project columns by looking at the table references
/// This function now works with the updated expression ID system
fn extract_alias_from_columns(columns: &[ProjectionColumn]) -> Option<String> {
    for column in columns {
        match column {
            ProjectionColumn::Column(col_ref, _) => {
                if let Some(ref table) = col_ref.table {
                    // The table field in ColumnRef should contain the source name
                    // For joins, this should be the stream name from the previous operation
                    return Some(table.clone());
                }
            }
            ProjectionColumn::Aggregate(agg_func, _) => {
                if let Some(ref table) = agg_func.column.table {
                    return Some(table.clone());
                }
            }
            ProjectionColumn::ComplexValue(complex_field, _) => {
                if let Some(ref col_ref) = complex_field.column_ref {
                    if let Some(ref table) = col_ref.table {
                        return Some(table.clone());
                    }
                }
                // For complex values, try to extract from aggregate
                if let Some(ref agg_func) = complex_field.aggregate {
                    if let Some(ref table) = agg_func.column.table {
                        return Some(table.clone());
                    }
                }
            }
            _ => continue,
        }
    }
    None
}