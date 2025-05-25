// dsl/languages/dataframe/ast_builder/df_utils.rs

use crate::dsl::ir::{ColumnRef, IrLiteral};
use crate::dsl::languages::dataframe::conversion_error::ConversionError;
use indexmap::IndexMap;
use serde_json::Value;

pub struct ConverterObject {
    pub expr_to_source: IndexMap<usize, (String, String)>,
    pub stream_index: usize,
    pub needs_alias: bool,
}

impl ConverterObject {
    pub fn new(expr_to_source: IndexMap<usize, (String, String)>) -> Self {
        ConverterObject {
            expr_to_source,
            stream_index: 0,
            needs_alias: false,
        }
    }

    /// Convert an expression ID to a (column_name, source_name) tuple using the expression mapping
    pub fn expr_id_to_column_source(
        expr_id: &usize,
        expr_to_column_source: &IndexMap<usize, (String, String)>,
    ) -> Option<(String, String)> {
        expr_to_column_source.get(expr_id).cloned()
    }

    /// Extract expression ID from a node
    pub fn extract_expr_id(node: &Value) -> Result<usize, Box<ConversionError>> {
        if let Some(expr_id) = node.get("exprId") {
            let id = expr_id
                .get("id")
                .and_then(|id| id.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("id".to_string())))?;

            Ok(id as usize)
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
    /// Create a column reference from a node and expression to column/source mapping
    pub fn create_column_ref(&self, node: &Value) -> Result<ColumnRef, Box<ConversionError>> {
        // Extract expression ID
        let expr_id = Self::extract_expr_id(node)?;

        // Look up column name and source name
        let (column_name, source_name) =
            Self::expr_id_to_column_source(&expr_id, &self.expr_to_source).unwrap_or_else(|| {
                // Fallback: try to get column name directly from node
                let fallback_column = node
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("unknown_column")
                    .to_string();
                (fallback_column, "unknown_source".to_string())
            });

        // Use source_name as table if it's not unknown_source, otherwise set to None
        let table = if source_name != "unknown_source" {
            Some(source_name)
        } else {
            None
        };

        Ok(ColumnRef {
            table,
            column: column_name,
        })
    }

    /// Update column and source information for an expression ID
    /// This can be used during query processing to update aliases and projections
    pub fn update_expr_mapping(
        &mut self,
        expr_id: usize,
        column_name: String,
        source_name: String,
    ) {
        self.expr_to_source
            .insert(expr_id, (column_name, source_name));
    }

    /// Get the current column name for an expression ID
    pub fn get_column_name(&self, expr_id: &usize) -> Option<String> {
        self.expr_to_source.get(expr_id).map(|(col, _)| col.clone())
    }

    /// Get the current source name for an expression ID
    pub fn get_source_name(&self, expr_id: &usize) -> Option<String> {
        self.expr_to_source.get(expr_id).map(|(_, src)| src.clone())
    }

    /// Get all expression IDs for a specific source
    pub fn get_expr_ids_for_source(&self, source_name: &str) -> Vec<usize> {
        self.expr_to_source
            .iter()
            .filter(|(_, (_, src))| src == source_name)
            .map(|(expr_id, _)| expr_id.clone())
            .collect()
    }
}
