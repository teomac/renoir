// dsl/languages/dataframe/ast_builder/df_utils.rs

use crate::dsl::ir::{ColumnRef, IrLiteral};
use crate::dsl::languages::dataframe::conversion_error::ConversionError;
use serde_json::Value;
use std::collections::HashMap;

pub struct ConverterObject {
    pub expr_to_table: HashMap<String, String>,
}

impl ConverterObject {
    pub fn new(
        expr_to_table: HashMap<String, String>,
    ) -> Self {
        ConverterObject {
            expr_to_table,
        }
    }

    /// Convert an expression ID to a table name using the expression to table mapping
    pub fn expr_id_to_table_name(
        expr_id: &str,
        expr_to_table: &HashMap<String, String>,
    ) -> Option<String> {
        expr_to_table.get(expr_id).cloned()
    }

    /// Extract expression ID from a node
    pub fn extract_expr_id(node: &Value) -> Result<String, Box<ConversionError>> {
        if let Some(expr_id) = node.get("exprId") {
            let id = expr_id
                .get("id")
                .and_then(|id| id.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("id".to_string())))?;

            let jvm_id = expr_id
                .get("jvmId")
                .and_then(|j| j.as_str())
                .ok_or_else(|| Box::new(ConversionError::MissingField("jvmId".to_string())))?;

            Ok(format!("{}_{}", id, jvm_id))
        } else {
            Err(Box::new(ConversionError::MissingField(
                "exprId".to_string(),
            )))
        }
    }

    /// Extract literal value from a node
    pub fn extract_literal_value(&self, node: &Value) -> Result<IrLiteral, Box<ConversionError>> {
        // Get the literal value
        let value = node
            .get("value")
            .ok_or_else(|| Box::new(ConversionError::MissingField("value".to_string())))?;

        // Get the data type
        let data_type = node
            .get("dataType")
            .and_then(|dt| dt.as_str())
            .ok_or_else(|| Box::new(ConversionError::MissingField("dataType".to_string())))?;

        // Convert based on data type
        match data_type {
            "integer" | "int" | "long" => {
                if let Some(s) = value.as_str() {
                    // Sometimes integers are represented as strings in JSON
                    match s.parse::<i64>() {
                        Ok(i) => Ok(IrLiteral::Integer(i)),
                        Err(_) => Err(Box::new(ConversionError::InvalidExpression)),
                    }
                } else if let Some(i) = value.as_i64() {
                    Ok(IrLiteral::Integer(i))
                } else {
                    Err(Box::new(ConversionError::InvalidExpression))
                }
            }
            "float" | "double" => {
                if let Some(s) = value.as_str() {
                    // Sometimes floats are represented as strings in JSON
                    match s.parse::<f64>() {
                        Ok(f) => Ok(IrLiteral::Float(f)),
                        Err(_) => Err(Box::new(ConversionError::InvalidExpression)),
                    }
                } else if let Some(f) = value.as_f64() {
                    Ok(IrLiteral::Float(f))
                } else {
                    Err(Box::new(ConversionError::InvalidExpression))
                }
            }
            "string" => {
                if let Some(s) = value.as_str() {
                    Ok(IrLiteral::String(s.to_string()))
                } else {
                    Err(Box::new(ConversionError::InvalidExpression))
                }
            }
            "boolean" => {
                if let Some(b) = value.as_bool() {
                    Ok(IrLiteral::Boolean(b))
                } else if let Some(s) = value.as_str() {
                    // Sometimes booleans are represented as strings in JSON
                    match s.to_lowercase().as_str() {
                        "true" => Ok(IrLiteral::Boolean(true)),
                        "false" => Ok(IrLiteral::Boolean(false)),
                        _ => Err(Box::new(ConversionError::InvalidExpression)),
                    }
                } else {
                    Err(Box::new(ConversionError::InvalidExpression))
                }
            }
            _ => Err(Box::new(ConversionError::UnsupportedExpressionType(
                data_type.to_string(),
            ))),
        }
    }

    /// Create a column reference from a node and expression to table mapping
    pub fn create_column_ref(&self, node: &Value) -> Result<ColumnRef, Box<ConversionError>> {
        // Extract column name
        let column_name = node
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| Box::new(ConversionError::MissingField("name".to_string())))?
            .to_string();

        // Extract expression ID
        let expr_id = Self::extract_expr_id(node)?;
        
        // Look up table name
        let table = Self::expr_id_to_table_name(&expr_id, &self.expr_to_table);

        Ok(ColumnRef {
            table,
            column: column_name,
        })
    }
}
