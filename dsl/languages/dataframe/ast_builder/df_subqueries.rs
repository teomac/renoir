// df_subquery.rs - Implementation for handling subquery expressions in Catalyst plans

use crate::dsl::ir::ComplexField;
use crate::dsl::languages::dataframe::conversion_error::ConversionError;
use serde_json::Value;

use super::df_utils::ConverterObject;
use crate::dsl::languages::dataframe::converter::process_node;

/// Process a ScalarSubquery node from a Catalyst plan
pub(crate) fn process_scalar_subquery(
    node: &Value,
    stream_index: &mut usize,
    project_count: &mut usize,
    conv_object: &ConverterObject,
) -> Result<ComplexField, Box<ConversionError>> {
    // Extract the subquery plan
    let subquery_plan = node
        .get("plan")
        .and_then(|p| p.as_array())
        .ok_or_else(|| Box::new(ConversionError::MissingField("plan".to_string())))?;

    if subquery_plan.is_empty() {
        return Err(Box::new(ConversionError::InvalidExpression));
    }

    // Process the subquery plan to generate an IR plan
    // We need to increment the project count to ensure unique naming for nested projections
    *project_count += 1;

    // Start processing from the root node of the subquery plan (index 0)
    let (subquery_ir_plan, _) = process_node(
        subquery_plan,
        0, 
        project_count,
        stream_index,
        conv_object,
    )?;

    // Create and return a ComplexField with the subquery
    Ok(ComplexField {
        column_ref: None,
        literal: None,
        aggregate: None,
        nested_expr: None,
        subquery: Some(subquery_ir_plan),
        subquery_vec: None,
    })
}