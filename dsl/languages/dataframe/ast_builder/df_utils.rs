// dsl/languages/dataframe/ast_builder/df_utils.rs

use crate::dsl::ir::{ColumnRef, IrLiteral};
use crate::dsl::languages::dataframe::conversion_error::ConversionError;
use indexmap::IndexMap;
use serde_json::Value;
use std::collections::HashMap;

pub struct ConverterObject {
    pub expr_to_table: HashMap<String, String>,
    pub input_tables: IndexMap<String, (String, IndexMap<String, String>)>,
}

impl ConverterObject {
    pub fn new(
        expr_to_table: HashMap<String, String>,
        input_tables: &IndexMap<String, (String, IndexMap<String, String>)>,
    ) -> Self {
        ConverterObject {
            expr_to_table,
            input_tables: input_tables.clone(),
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

    /// Determine if a node is a reference to a specific column
    pub fn is_column_reference(node: &Value, column_name: &str) -> bool {
        if let Some(class) = node.get("class").and_then(|c| c.as_str()) {
            if class.ends_with("AttributeReference") {
                if let Some(name) = node.get("name").and_then(|n| n.as_str()) {
                    return name == column_name;
                }
            }
        }
        false
    }

    /// Find the most specific table for a column name in the expression mapping
    pub fn find_table_for_column(
        column: &str,
        expr_to_table: &HashMap<String, String>,
        column_to_expr_id: &HashMap<String, Vec<String>>,
    ) -> Option<String> {
        // Look up expression IDs for this column
        if let Some(expr_ids) = column_to_expr_id.get(column) {
            // Find the first expression ID that has a table mapping
            for expr_id in expr_ids {
                if let Some(table) = expr_to_table.get(expr_id) {
                    return Some(table.clone());
                }
            }
        }
        None
    }

    /// Build a mapping from column names to expression IDs
    /// This is useful when looking up which table a column belongs to
    pub fn build_column_to_expr_id_map(plan: &[Value]) -> HashMap<String, Vec<String>> {
        let mut column_map: HashMap<String, Vec<String>> = HashMap::new();

        for node in plan {
            if let Some(class) = node.get("class").and_then(|c| c.as_str()) {
                if class.ends_with("AttributeReference") {
                    if let (Some(name), Ok(expr_id)) = (
                        node.get("name").and_then(|n| n.as_str()),
                        Self::extract_expr_id(node),
                    ) {
                        column_map
                            .entry(name.to_string())
                            .or_insert_with(Vec::new)
                            .push(expr_id);
                    }
                }
            }
        }

        column_map
    }

    /// Find all attribute references in a condition array
    pub fn find_attribute_references(condition_array: &[Value]) -> Vec<(String, String)> {
        let mut references = Vec::new();

        for node in condition_array {
            if let Some(class) = node.get("class").and_then(|c| c.as_str()) {
                if class.ends_with("AttributeReference") {
                    if let (Some(name), Ok(expr_id)) = (
                        node.get("name").and_then(|n| n.as_str()),
                        Self::extract_expr_id(node),
                    ) {
                        references.push((name.to_string(), expr_id));
                    }
                }
            }
        }

        references
    }

    /// Recursively index complex expression nodes in the condition array
    /// Returns a map from expression ID to operator type
    pub fn index_complex_expressions(condition_array: &[Value]) -> HashMap<String, String> {
        let mut expr_map = HashMap::new();

        for node in condition_array {
            if let Some(class) = node.get("class").and_then(|c| c.as_str()) {
                // Get the operator type from the class name
                let op_type = class.split('.').last().unwrap_or("").to_string();

                // Skip basic attribute references and literals
                if op_type == "AttributeReference" || op_type == "Literal" {
                    continue;
                }

                // For complex expressions, record the operator type
                if let Ok(expr_id) = Self::extract_expr_id(node) {
                    expr_map.insert(expr_id, op_type);
                }
            }
        }

        expr_map
    }
}
