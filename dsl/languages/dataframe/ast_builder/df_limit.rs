use crate::dsl::ir::IrPlan;
use crate::dsl::languages::dataframe::conversion_error::ConversionError;
use serde_json::Value;
use std::sync::Arc;

use super::df_utils::ConverterObject;

/// Process a GlobalLimit node from a Catalyst plan
pub(crate) fn process_limit(
    node: &Value,
    input_plan: Arc<IrPlan>,
    _conv_object: &mut ConverterObject,
) -> Result<Arc<IrPlan>, Box<ConversionError>> {
    // Extract the limitExpr array
    let limit_expr_array = node
        .get("limitExpr")
        .and_then(|l| l.as_array())
        .ok_or_else(|| Box::new(ConversionError::MissingField("limitExpr".to_string())))?;

    // Get the first (and should be only) element - the Literal
    let literal = limit_expr_array
        .first()
        .ok_or_else(|| Box::new(ConversionError::InvalidExpression))?;

    // Verify it's a Literal
    let class = literal
        .get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    let node_type = class
        .split('.')
        .last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    if node_type != "Literal" {
        return Err(Box::new(ConversionError::UnsupportedExpressionType(
            node_type.to_string(),
        )));
    }

    // Extract the integer value
    let limit_value = literal
        .get("value")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Box::new(ConversionError::MissingField("value".to_string())))?;

    // Parse to usize and validate it's positive
    let limit: i64 = limit_value
        .parse()
        .map_err(|e| Box::new(ConversionError::ParseIntError(e)))?;

    if limit == 0 {
        return Err(Box::new(ConversionError::InvalidExpression)); // Or create a specific error for invalid limit
    }

    // Create the Limit node with offset defaulted to 0
    Ok(Arc::new(IrPlan::Limit {
        input: input_plan,
        limit,
        offset: None, // Default offset to 0 as specified
    }))
}