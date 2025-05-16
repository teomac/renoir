use crate::dsl::ir::{ColumnRef, IrPlan, OrderByItem, OrderDirection};
use crate::dsl::languages::dataframe::conversion_error::ConversionError;
use serde_json::Value;
use std::sync::Arc;

use super::df_utils::ConverterObject;

/// Process a Sort (ORDER BY) node from a Catalyst plan
pub(crate) fn process_sort(
    node: &Value,
    input_plan: Arc<IrPlan>,
    conv_object: &ConverterObject,
) -> Result<Arc<IrPlan>, Box<ConversionError>> {
    // Extract the order array
    let order_array = node
        .get("order")
        .and_then(|o| o.as_array())
        .ok_or_else(|| Box::new(ConversionError::MissingField("order".to_string())))?;

    let mut order_items = Vec::new();

    // Process each order specification
    for order_spec in order_array {
        if let Some(spec_array) = order_spec.as_array() {
            // The first element is the SortOrder object
            if let Some(sort_order) = spec_array.first() {
                // Process the sort order
                let order_item = process_sort_object(sort_order, spec_array, conv_object)?;
                order_items.push(order_item);
            }
        }
    }

    // If no order items were processed, return an error
    if order_items.is_empty() {
        return Err(Box::new(ConversionError::InvalidExpression));
    }

    // Create the OrderBy node
    Ok(Arc::new(IrPlan::OrderBy {
        input: input_plan,
        items: order_items,
    }))
}

/// Process a SortOrder specification
fn process_sort_object(
    sort_order: &Value,
    spec_array: &[Value],
    conv_object: &ConverterObject,
) -> Result<OrderByItem, Box<ConversionError>> {
    // Get the direction
    let direction = sort_order
        .get("direction")
        .and_then(|d| d.get("object"))
        .and_then(|o| o.as_str())
        .ok_or_else(|| Box::new(ConversionError::MissingField("direction".to_string())))?;

    // Map direction to ASC/DESC
    let direction = match direction.split('.').last() {
        Some("Ascending$") => OrderDirection::Asc,
        Some("Descending$") => OrderDirection::Desc,
        _ => return Err(Box::new(ConversionError::UnsupportedExpressionType(direction.to_string()))),
    };

    let null_ordering = sort_order
        .get("nullOrdering")
        .and_then(|d| d.get("object"))
        .and_then(|o| o.as_str())
        .ok_or_else(|| Box::new(ConversionError::MissingField("nullOrdering".to_string())))?;

    let nulls_first = match null_ordering.split('.').last() {
        Some("NullsFirst$") => Some(true),
        Some("NullsLast$") => Some(false),
        _ => return Err(Box::new(ConversionError::UnsupportedExpressionType(null_ordering.to_string()))),
    };

    // Get the child index to find the column reference
    let child_idx = sort_order
        .get("child")
        .and_then(|c| c.as_u64())
        .ok_or_else(|| Box::new(ConversionError::MissingField("child".to_string())))?
        as usize;

    // Process the column reference (AttributeReference)
    let column_ref = process_sort_column(spec_array, child_idx + 1, conv_object)?;

    // Create the OrderByItem
    let order_item = OrderByItem { 
        column: column_ref,
        direction, 
        nulls_first,
    };

    Ok(order_item)
}

/// Process the column reference for ordering
fn process_sort_column(
    spec_array: &[Value],
    idx: usize,
    conv_object: &ConverterObject,
) -> Result<ColumnRef, Box<ConversionError>> {
    if idx >= spec_array.len() {
        return Err(Box::new(ConversionError::InvalidExpression));
    }

    let column_node = &spec_array[idx];

    // Verify it's an AttributeReference
    let class = column_node
        .get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    let node_type = class
        .split('.')
        .last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    if node_type != "AttributeReference" {
        return Err(Box::new(ConversionError::UnsupportedExpressionType(node_type.to_string())));
    }

    // Create the column reference using the utility function
    conv_object.create_column_ref(column_node)
}