use serde_json::Value;
use std::sync::Arc;

use crate::dsl::ir::{ColumnRef, IrPlan};
use crate::dsl::languages::dataframe::conversion_error::ConversionError;

use super::df_project::process_project_agg;
use super::df_utils::ConverterObject; // Import the missing IrPlan type

pub(crate) fn process_aggregate(
    node: &Value,
    input_plan: Arc<IrPlan>,
    project_count: &mut usize,
    conv_object: &mut ConverterObject,
) -> Result<Arc<IrPlan>, Box<ConversionError>> {
    //Retrieve the grouping expressions array
    let grouping_expressions = node
        .get("groupingExpressions")
        .ok_or_else(|| {
            Box::new(ConversionError::MissingField(
                "groupingExpressions".to_string(),
            ))
        })?
        .as_array()
        .unwrap();

    // Retrieve the aggregate expressions array
    let aggregate_expressions = node
        .get("aggregateExpressions")
        .ok_or_else(|| {
            Box::new(ConversionError::MissingField(
                "aggregateExpressions".to_string(),
            ))
        })?
        .as_array()
        .unwrap();

    // Safety check for the aggregate expressions
    if aggregate_expressions.is_empty() {
        return Err(Box::new(ConversionError::InvalidGroupKeys(
            "Empty aggregate expressions".to_string(),
        )));
    }

    //If grouping expressions is empty, we have a projection with aggregates
    if grouping_expressions.is_empty() {
        process_project_agg(
            aggregate_expressions,
            input_plan,
            project_count,
            conv_object,
        )
    } else {
        //if grouping expressions is not empty, we have a group by with aggregates
        //we need to create a group plan and then parse the aggregate expressions
        //parse the grouping expressions
        let group_keys = parse_grouping_expressions(grouping_expressions, conv_object);

        // Define the group plan. We do not have a group condition, because of how the group by is defined in the Catalyst plan
        let group_plan = Arc::new(IrPlan::GroupBy {
            input: input_plan,
            keys: group_keys.unwrap(),
            group_condition: None,
        });
        //parse the aggregate expressions
        process_project_agg(
            aggregate_expressions,
            group_plan,
            project_count,
            conv_object,
        )
    }
}

fn parse_grouping_expressions(
    group_expressions: &[Value],
    conv_object: &mut ConverterObject,
) -> Result<Vec<ColumnRef>, Box<ConversionError>> {
    //group_expressions object is an array of arrays containing the column names on which the group by is performed
    //So, we need to iterate over each array inside the group_expressions array
    //and then iterate over each element inside the array and create a ColumnRef object for each element
    let mut group_keys = Vec::new();

    for group_expression in group_expressions {
        if let Some(group_expression_array) = group_expression.as_array() {
            for group_expression_item in group_expression_array {
                let column = conv_object.create_column_ref(group_expression_item);
                //if column is not null, push it to the group_keys vector
                //if column is null, return an error
                if let Ok(column) = column {
                    group_keys.push(column);
                } else {
                    return Err(Box::new(ConversionError::InvalidGroupKeys(
                        "Invalid column reference".to_string(),
                    )));
                }
            }
        }
    }
    Ok(group_keys)
}
