use crate::dsl::ir::validate::validate_ir_ast;
use crate::dsl::ir::IrPlan;
use crate::dsl::languages::dataframe::ast_builder::df_join::process_join_child;
use crate::dsl::languages::dataframe::conversion_error::ConversionError;
use serde_json::Value;
use std::sync::Arc;

use super::ast_builder::df_aggregate::process_aggregate;
use super::ast_builder::df_filter::process_filter;
use super::ast_builder::df_join::process_join;
use super::ast_builder::df_limit::process_limit;
use super::ast_builder::df_project::process_project;
use super::ast_builder::df_sort::process_sort;
use super::ast_builder::df_utils::ConverterObject;

/// Convert a Catalyst plan to Renoir IR AST
pub fn build_ir_ast_df(
    plan: &[Value],
    conv_object: &mut ConverterObject,
) -> Result<Arc<IrPlan>, Box<ConversionError>> {
    // The Catalyst plan is an array of nodes with the root node at index 0
    if plan.is_empty() {
        return Err(Box::new(ConversionError::EmptyPlan));
    }

    println!("Catalyst plan: {:?}", plan);
    let mut project_count:i64 = 0;

    // Start processing from the root node
    let final_ast = process_node(plan, 0, &mut project_count, conv_object)
        .unwrap()
        .0;

    println!("Final AST: {:?}", final_ast);

    // Validate the final AST
    Ok(validate_ir_ast(final_ast))
}

/// Process a node in the Catalyst plan
pub fn process_node(
    full_plan: &[Value],
    current_index: usize,
    project_count: &mut i64,
    conv_object: &mut ConverterObject,
) -> Result<(Arc<IrPlan>, usize), Box<ConversionError>> {
    let node = full_plan
        .get(current_index)
        .ok_or_else(|| {
            Box::new(ConversionError::InvalidNodeIndex(String::from(
                "Invalid node index",
            )))
        })
        .unwrap();
    // Extract the node class
    let class = node
        .get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))
        .unwrap();

    // Get the node type from the class name (last part after the dot)
    let node_type = class
        .split('.')
        .last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))
        .unwrap();

    // Process based on node type
    match node_type {
        "Project" => {
            // Increment the project count
            *project_count += 1;

            let current_project_count = *project_count;

            // Get the child node
            let child_idx = node
                .get("child")
                .and_then(|c| c.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("child".to_string())))
                .unwrap();

            let (input_plan, index) = process_node(
                full_plan,
                current_index + child_idx as usize + 1,
                project_count,
                conv_object,
            )?;

            // Process the project node
            Ok((
                process_project(node, input_plan, &current_project_count, conv_object)?,
                index,
            ))
        }
        "Filter" => {
            if *project_count == 0{
                *project_count += 1; // Increment project count for the first filter
            }
            let current_project_count = *project_count;
            // Get the child node
            let child_idx = node
                .get("child")
                .and_then(|c| c.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("child".to_string())))
                .unwrap();

            // Process the child node first
            let (input_plan, index) = process_node(
                full_plan,
                current_index + child_idx as usize + 1,
                project_count,
                conv_object,
            )?;
            // Process the filter node
            Ok((
                process_filter(node, input_plan, current_project_count, conv_object)?,
                index,
            ))
        }
        "Join" => {            
            let left_child_idx = node
                .get("left")
                .and_then(|c| c.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("left".to_string())))
                .unwrap();

            // Reset project count for each join child to properly track nested Projects
            let mut left_project_count: i64 = *project_count;
            let mut right_project_count: i64 = *project_count;

            let (left_child, index) = process_join_child(
                current_index + left_child_idx as usize + 1,
                full_plan,
                &mut left_project_count,
                conv_object,
            )?;

            let (right_child, final_idx) =
                process_join_child(index + 1, full_plan, &mut right_project_count, conv_object)?;

            Ok((
                process_join(node, left_child, right_child, conv_object)?,
                final_idx,
            ))
        }
        "Aggregate" => {
            // Increment the project count
            *project_count += 1;

            let current_project_count = *project_count;
            //Get the child node
            let child_idx = node
                .get("child")
                .and_then(|c| c.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("child".to_string())))
                .unwrap();

            //Process the child node first
            let (input_plan, index) = process_node(
                full_plan,
                current_index + child_idx as usize + 1,
                project_count,
                conv_object,
            )?;

            //Now process the aggregate node
            Ok((
                process_aggregate(node, input_plan, &current_project_count, conv_object)?,
                index,
            ))
        }
        "Sort" => {
            // Get the child node
            let child_idx = node
                .get("child")
                .and_then(|c| c.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("child".to_string())))
                .unwrap();

            // Process the child node first
            let (input_plan, index) = process_node(
                full_plan,
                current_index + child_idx as usize + 1,
                project_count,
                conv_object,
            )?;

            // Process the sort node
            Ok((process_sort(node, input_plan, conv_object)?, index))
        }
        "GlobalLimit" => {
            // Get the child node
            let child_idx = node
                .get("child")
                .and_then(|c| c.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("child".to_string())))
                .unwrap();

            // Process the child node first
            let (input_plan, index) = process_node(
                full_plan,
                current_index + child_idx as usize + 1,
                project_count,
                conv_object,
            )?;

            // Process the limit node
            Ok((process_limit(node, input_plan, conv_object)?, index))
        }
        "LocalLimit" => {
            // Skip LocalLimit and process its child directly as we already handle it in GlobalLimit
            let child_idx = node
                .get("child")
                .and_then(|c| c.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("child".to_string())))
                .unwrap();

            // Just pass through to the child
            process_node(
                full_plan,
                current_index + child_idx as usize + 1,
                project_count,
                conv_object,
            )
        }
        "LogicalRDD" | "LogicalRelation" => {
            *project_count += 1; // Increment project count for table scans
            // This is a base table scan
            Ok((
                process_logical_rdd(node, project_count, conv_object)?,
                current_index + 1,
            ))
        }
        _ => Err(Box::new(ConversionError::UnsupportedNodeType(
            node_type.to_string(),
        ))),
    }
}

/// Process a LogicalRDD node (table scan)
fn process_logical_rdd(
    node: &Value,
    project_count: &mut i64,
    conv_object: &mut ConverterObject,
) -> Result<Arc<IrPlan>, Box<ConversionError>> {
    // Extract table name from column expression IDs
    let mut table_name = String::from("unknown_table");

    // Check the output columns to find the associated table name
    if let Some(output) = node.get("output").and_then(|o| o.as_array()) {
        for column_array in output {
            if let Some(columns) = column_array.as_array() {
                if let Some(column) = columns.first() {
                    // Extract the expression ID
                    if let Some(expr_id_obj) = column.get("exprId") {
                        if let Some(id) = expr_id_obj.get("id").and_then(|id| id.as_u64()) {
                            let expr_id = id as usize;

                            // Look up the source name in our mapping
                            if let Some((_, source_name)) = conv_object.expr_to_source.get(&expr_id)
                            {
                                table_name = source_name.clone();
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    // Create the Table node
    let table_node = Arc::new(IrPlan::Table {
        table_name: table_name.clone(),
    });

    let stream_name = conv_object.increment_and_get_stream_name(*project_count);

    let plan = Arc::new(IrPlan::Scan {
        input: table_node,
        stream_name,
        alias: Some(table_name),
    });

    Ok(plan)
}
