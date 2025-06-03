use crate::dsl::ir::{ColumnRef, IrLiteral};
use crate::dsl::languages::dataframe::conversion_error::ConversionError;
use indexmap::IndexMap;
use serde_json::Value;


pub struct ConverterObject {
    pub expr_to_source: IndexMap<usize, (String, String)>,
    pub stream_index: usize,
    pub stream_names: Vec<String>,
}

pub struct ExprUpdate{
    pub expr_id: usize,
    pub column_name: String,
    pub source_name: String,
}

impl ExprUpdate {
    pub fn new(expr_id: usize, column_name: String, source_name: String) -> Self {
        ExprUpdate {
            expr_id,
            column_name,
            source_name,
        }
    }
    
}

impl ConverterObject {
    pub fn new(expr_to_source: IndexMap<usize, (String, String)>) -> Self {
        ConverterObject {
            expr_to_source,
            stream_index: 0,
            stream_names: Vec::new(),
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

    /// Bulk update expression mappings after processing a projection
    /// This is called after processing all projection columns to update their expr IDs
    /// with new source names and aliases
    pub fn update_projection_mappings(
        &mut self,
        projection_updates: Vec<ExprUpdate>, // (expr_id, new_column_name, new_source_name)
    ) {
        for update in projection_updates {
            self.expr_to_source
                .insert(update.expr_id, (update.column_name, update.source_name));
        }
    }

    /// Generate auto-alias for a column in join scenarios
    /// Uses the pattern: column_sourcename
    pub fn generate_auto_alias(
        &self,
        column_name: &str,
        source_name: &str,
        is_nested_projection: bool,
    ) -> String {
        if is_nested_projection {
            // For nested projections after joins, always generate aliases
            format!("{}_{}", column_name, source_name)
        } else {
            // For regular projections, use the original column name
            column_name.to_string()
        }
    }

    /// Extract expression ID from a projection column and resolve its information
    /// Returns (expr_id, original_column_name, original_source_name)
    pub fn resolve_projection_column(
        &self,
        node: &Value,
    ) -> Result<(usize, String, String), Box<ConversionError>> {
        let expr_id = Self::extract_expr_id(node)?;

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

        Ok((expr_id, column_name, source_name))
    }

    /// Increment stream index and return the new stream name
    pub fn increment_and_get_stream_name(&mut self, index: i64) -> String {
        let mut result = String::new();
        for _ in 1..index {
            result.push_str("sub");
        }
        let stream_name = format!("stream{}", self.stream_index);
        self.stream_index += 1;
        let result = format!("{}{}", result, stream_name).to_string();
        self.stream_names.push(result.clone());
        result
    }
}
