use crate::dsl::ir::{ColumnRef, ComplexField, IrLiteral, IrPlan, ProjectionColumn};
use crate::dsl::languages::dataframe::conversion_error::ConversionError;
use serde_json::Value;
use std::sync::Arc;

use super::df_utils::ConverterObject;

/// Process a Project (SELECT) node from a Catalyst plan
/// Process a Project node (SELECT operation)
pub fn process_project(node: &Value, input_plan: Arc<IrPlan>, current_index: usize, conv_object: &ConverterObject) -> Result<Arc<IrPlan>, Box<ConversionError>> {
    // Extract the project list
    let project_list = node.get("projectList")
        .and_then(|p| p.as_array())
        .ok_or_else(|| Box::new(ConversionError::MissingField("projectList".to_string())))?;
    
    let mut columns = Vec::new();
    
    // Process each projection list item
    for projection_array in project_list {
        if let Some(projections) = projection_array.as_array() {
            // Process alias expressions (typically the first element is an Alias, followed by its child)
            if let Some(alias_expr) = projections.first() {
                let class = alias_expr.get("class")
                    .and_then(|c| c.as_str())
                    .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;
                
                let expr_type = class.split('.').last()
                    .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;
                
                match expr_type {
                    "Alias" => {
                        // This is an aliased expression
                        let alias_name = alias_expr.get("name")
                            .and_then(|n| n.as_str())
                            .ok_or_else(|| Box::new(ConversionError::MissingField("name".to_string())))?
                            .to_string();
                        
                        // Get the child expression index
                        let child_idx = alias_expr.get("child")
                            .and_then(|c| c.as_u64())
                            .ok_or_else(|| Box::new(ConversionError::MissingField("child".to_string())))?;
                        
                        // Find the child expression in the projection array (typically at index 1)
                        let child_expr = &projections[child_idx as usize + 1];
                        
                        // Process the child expression
                        let column = process_expression(child_expr, Some(alias_name), conv_object)?;
                        columns.push(column);
                    },
                    "AttributeReference" => {
                        // Direct column reference without alias
                        let column = process_expression(alias_expr, None, conv_object)?;
                        columns.push(column);
                    },
                    _ => {
                        return Err(Box::new(ConversionError::UnsupportedExpressionType(expr_type.to_string())));
                    }
                }
            }
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

/// Process an expression to create a ProjectionColumn
fn process_expression(expr: &Value, alias: Option<String>, conv_object: &ConverterObject) -> Result<ProjectionColumn, Box<ConversionError>> {
    // Extract the expression type
    let class = expr.get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;
    
    let expr_type = class.split('.').last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;
    
    match expr_type {
        "AttributeReference" => {
            // Extract column name
            let column_name = expr.get("name")
                .and_then(|n| n.as_str())
                .ok_or_else(|| Box::new(ConversionError::MissingField("name".to_string())))?
                .to_string();
            
            // Extract expression ID to look up table name
            let expr_id = get_expr_id(expr)?;
            
            // Look up table name from mapping
            let table = conv_object.expr_to_table.get(&expr_id).cloned();
            
            // Create column reference
            let column_ref = ColumnRef {
                table,
                column: column_name,
            };
            
            Ok(ProjectionColumn::Column(column_ref, alias))
        },
        "Literal" => {
            // Extract literal value based on data type
            let literal = process_literal_value(expr)?;
            
            // Create complex field for the literal
            let complex_field = ComplexField {
                column_ref: None,
                literal: Some(literal),
                aggregate: None,
                nested_expr: None,
                subquery: None,
                subquery_vec: None,
            };
            
            Ok(ProjectionColumn::ComplexValue(complex_field, alias))
        },
        _ => Err(Box::new(ConversionError::UnsupportedExpressionType(expr_type.to_string())))
    }
}

/// Extract an expression ID as a string
fn get_expr_id(expr: &Value) -> Result<String, Box<ConversionError>> {
    let expr_id = expr.get("exprId")
        .ok_or_else(|| Box::new(ConversionError::MissingField("exprId".to_string())))?;
    
    let id = expr_id.get("id")
        .and_then(|id| id.as_u64())
        .ok_or_else(|| Box::new(ConversionError::MissingField("id".to_string())))?;
    
    let jvm_id = expr_id.get("jvmId")
        .and_then(|j| j.as_str())
        .ok_or_else(|| Box::new(ConversionError::MissingField("jvmId".to_string())))?;
    
    Ok(format!("{}_{}", id, jvm_id))
}

/// Process a literal value
fn process_literal_value(expr: &Value) -> Result<IrLiteral, Box<ConversionError>> {
    // Get the literal value
    let value = expr.get("value")
        .ok_or_else(|| Box::new(ConversionError::MissingField("value".to_string())))?;
    
    // Get the data type
    let data_type = expr.get("dataType")
        .and_then(|dt| dt.as_str())
        .ok_or_else(|| Box::new(ConversionError::MissingField("dataType".to_string())))?;
    
    // Convert the value based on data type
    match data_type {
        "long" | "int" => {
            if let Some(i) = value.as_i64() {
                Ok(IrLiteral::Integer(i))
            } else {
                Err(Box::new(ConversionError::InvalidExpression))
            }
        },
        "float" | "double" => {
            if let Some(f) = value.as_f64() {
                Ok(IrLiteral::Float(f))
            } else {
                Err(Box::new(ConversionError::InvalidExpression))
            }
        },
        "string" => {
            if let Some(s) = value.as_str() {
                Ok(IrLiteral::String(s.to_string()))
            } else {
                Err(Box::new(ConversionError::InvalidExpression))
            }
        },
        "boolean" => {
            if let Some(b) = value.as_bool() {
                Ok(IrLiteral::Boolean(b))
            } else {
                Err(Box::new(ConversionError::InvalidExpression))
            }
        },
        _ => Err(Box::new(ConversionError::UnsupportedExpressionType(data_type.to_string())))
    }
}