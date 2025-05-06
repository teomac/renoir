use crate::dsl::ir::IrPlan;
use crate::dsl::languages::dataframe::conversion_error::ConversionError;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use super::ast_builder::df_filter::process_filter;
use super::ast_builder::df_select::process_project;

/// Convert a Catalyst plan to Renoir IR AST
pub fn build_ir_ast_df(
    plan: &[Value],
    expr_to_table: HashMap<String, String>,
) -> Result<Arc<IrPlan>, Box<ConversionError>> {
    // The Catalyst plan is an array of nodes with the root node at index 0
    let mut stream_index: usize = 0;
    if plan.is_empty() {
        return Err(Box::new(ConversionError::EmptyPlan));
    }

    println!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    print!("Plan: {:?}", plan);

    // Start processing from the root node
    process_node(&plan[0], 0, plan, &expr_to_table, &mut stream_index)
}

/// Process a node in the Catalyst plan
fn process_node(
    node: &Value,
    current_index: usize,
    full_plan: &[Value],
    expr_to_table: &HashMap<String, String>,
    stream_index: &mut usize,
) -> Result<Arc<IrPlan>, Box<ConversionError>> {
    // Extract the node class
    let class = node
        .get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    // Get the node type from the class name (last part after the dot)
    let node_type = class
        .split('.')
        .last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;

    // Process based on node type
    match node_type {
        "Project" => {
            // Get the child node
            let child_idx = node
                .get("child")
                .and_then(|c| c.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("child".to_string())))?;

            let input_plan = process_node(
                &full_plan[current_index + child_idx as usize + 1],
                current_index + child_idx as usize + 1,
                full_plan,
                expr_to_table,
                stream_index,
            )?;

            // Process the child node first

            // Process the project node
            process_project(node, input_plan, expr_to_table)
        }
        "Filter" => {
            // Get the child node
            let child_idx = node
                .get("child")
                .and_then(|c| c.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("child".to_string())))?;

            // Process the child node first
            let input_plan = process_node(
                &full_plan[current_index + child_idx as usize + 1],
                current_index + child_idx as usize + 1,
                full_plan,
                expr_to_table,
                stream_index,
            )?;
            // Process the filter node
            process_filter(node, input_plan, expr_to_table)
        }
        "LogicalRDD" | "LogicalRelation" => {
            // This is a base table scan
            process_logical_rdd(node, expr_to_table, stream_index)
        }
        _ => Err(Box::new(ConversionError::UnsupportedNodeType(
            node_type.to_string(),
        ))),
    }
}

/// Process a LogicalRDD node (table scan)
fn process_logical_rdd(
    node: &Value,
    expr_to_table: &HashMap<String, String>,
    stream_index: &mut usize,
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
                            if let Some(table) = expr_to_table.get(&expr_id) {
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
