use crate::dsl::ir::{IrPlan, JoinType};
use crate::dsl::languages::dataframe::conversion_error::ConversionError;
use serde_json::Value;
use std::sync::Arc;

use super::ast_builder::df_filter::process_filter;
use super::ast_builder::df_join::process_join;
use super::ast_builder::df_select::process_project;
use super::ast_builder::df_utils::ConverterObject;

/// Convert a Catalyst plan to Renoir IR AST
pub fn build_ir_ast_df(
    plan: &[Value],
    conv_object: ConverterObject,
) -> Result<Arc<IrPlan>, Box<ConversionError>> {
    // The Catalyst plan is an array of nodes with the root node at index 0
    let mut stream_index: usize = 0;
    if plan.is_empty() {
        return Err(Box::new(ConversionError::EmptyPlan));
    }

    println!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    println!("Plan: {:?}", plan);

    // Start processing from the root node
    process_node(&plan[0], 0, plan, &mut stream_index, &conv_object).0
}

/// Process a node in the Catalyst plan
fn process_node(
    node: &Value,
    current_index: usize,
    full_plan: &[Value],
    stream_index: &mut usize,
    conv_object: &ConverterObject,
) -> (Result<Arc<IrPlan>, Box<ConversionError>>, usize) {
    // Extract the node class
    let class = node
        .get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName)).unwrap();

    // Get the node type from the class name (last part after the dot)
    let node_type = class
        .split('.')
        .last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName)).unwrap();

    // Process based on node type
    match node_type {
        "Project" => {
            // Get the child node
            let child_idx = node
                .get("child")
                .and_then(|c| c.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("child".to_string()))).unwrap();

            let input_plan = process_node(
                &full_plan[current_index + child_idx as usize + 1],
                current_index + child_idx as usize + 1,
                full_plan,
                stream_index,
                conv_object,
            ).0;

            // Process the child node first

            // Process the project node
            (process_project(node, input_plan.unwrap(), current_index, conv_object), current_index)
        }
        "Filter" => {
            // Get the child node
            let child_idx = node
                .get("child")
                .and_then(|c| c.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("child".to_string()))).unwrap();

            // Process the child node first
            let input_plan = process_node(
                &full_plan[current_index + child_idx as usize + 1],
                current_index + child_idx as usize + 1,
                full_plan,
                stream_index,
                conv_object,
            ).0;
            // Process the filter node
            (process_filter(node, input_plan.unwrap(), conv_object), current_index)
        }
        "Join" => {
            

            let child_idx = node
                .get("left")
                .and_then(|c| c.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("left".to_string()))).unwrap();

            let (left_child, index) = process_node(
                &full_plan[current_index + child_idx as usize + 1],
                current_index + child_idx as usize + 1,
                full_plan,
                stream_index,
                conv_object,
            );

            let right_child = process_node(
                &full_plan[index + 1],
                index + 1,
                full_plan,
                stream_index,
                conv_object,
            ).0;

            (process_join(node, left_child.unwrap(), right_child.unwrap(), conv_object), current_index)

        }
        "LogicalRDD" | "LogicalRelation" => {
            // This is a base table scan
            (process_logical_rdd(node, stream_index, conv_object), current_index)
        }
        _ => (Err(Box::new(ConversionError::UnsupportedNodeType(
            node_type.to_string(),
        ))), current_index),
    }
}

/// Process a LogicalRDD node (table scan)
fn process_logical_rdd(
    node: &Value,
    stream_index: &mut usize,
    conv_object: &ConverterObject,
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
                        if let (Some(id), Some(jvm_id)) = (
                            expr_id_obj.get("id").and_then(|id| id.as_u64()),
                            expr_id_obj.get("jvmId").and_then(|j| j.as_str()),
                        ) {
                            let expr_id = format!("{}_{}", id, jvm_id);

                            // Look up the table name in our mapping
                            if let Some(table) = conv_object.expr_to_table.get(&expr_id) {
                                table_name = table.clone();
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

    let plan = Arc::new(IrPlan::Scan {
        input: table_node,
        stream_name: format!("stream_{}", stream_index),
        alias: Some(table_name),
    });

    *stream_index += 1; // Increment the stream index for the next node
                        // Create the Scan node
    Ok(plan)
}
