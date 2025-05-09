use crate::dsl::ir::IrPlan;
use crate::dsl::languages::dataframe::ast_builder::df_join::process_join_child;
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
    Ok(process_node(&plan[0], 0, plan, &mut stream_index, &conv_object).unwrap().0)
}

/// Process a node in the Catalyst plan
pub fn process_node(
    node: &Value,
    current_index: usize,
    full_plan: &[Value],
    stream_index: &mut usize,
    conv_object: &ConverterObject,
) -> Result<(Arc<IrPlan>, usize), Box<ConversionError>> {
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

    println!("Node type: {:?}", node_type);
    println!("Node: {:?}", node);

    // Process based on node type
    match node_type {
        "Project" => {
            // Get the child node
            let child_idx = node
                .get("child")
                .and_then(|c| c.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("child".to_string()))).unwrap();

            let (input_plan, index) = process_node(
                &full_plan[current_index + child_idx as usize + 1],
                current_index + child_idx as usize + 1,
                full_plan,
                stream_index,
                conv_object,
            )?;

            // Process the child node first

            // Process the project node
            Ok((process_project(node, input_plan, current_index, conv_object)?, index))
        }
        "Filter" => {
            // Get the child node
            let child_idx = node
                .get("child")
                .and_then(|c| c.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("child".to_string()))).unwrap();

            // Process the child node first
            let (input_plan, index) = process_node(
                &full_plan[current_index + child_idx as usize + 1],
                current_index + child_idx as usize + 1,
                full_plan,
                stream_index,
                conv_object,
            )?;
            // Process the filter node
            Ok((process_filter(node, input_plan, conv_object)?, index))
        }
        "Join" => {
            println!("Current index: {:?}", current_index);

            let left_child_idx = node
                .get("left")
                .and_then(|c| c.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("left".to_string()))).unwrap();
            println!("Child index: {:?}", left_child_idx);

            let (left_child, index) = process_join_child(
                current_index + left_child_idx as usize + 1,
                full_plan,
                stream_index,
                conv_object,
            )?;

            println!("Left child: {:?}", left_child);
            println!("Index: {:?}", index);

            let (right_child, final_idx) = process_join_child(
                index + 1,
                full_plan,
                stream_index,
                conv_object,
            )?;

            println!("Right child: {:?}", right_child);
            println!("Final index: {:?}", final_idx);

            Ok((process_join(node, left_child, right_child, conv_object)?, final_idx))

        }
        "LogicalRDD" | "LogicalRelation" => {
            // This is a base table scan
            Ok((process_logical_rdd(node, stream_index, conv_object)?, current_index + 1))
        }
        _ => Err(Box::new(ConversionError::UnsupportedNodeType(
            node_type.to_string(),
        ))),
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
                        println!("exprId: {:?}", expr_id_obj);
                        if let (Some(id), Some(jvm_id)) = (
                            expr_id_obj.get("id").and_then(|id| id.as_u64()),
                            expr_id_obj.get("jvmId").and_then(|j| j.as_str()),
                        ) {
                            let expr_id = format!("{}_{}", id, jvm_id);
                            println!("exprId: {:?}", expr_id);

                            // Look up the table name in our mapping
                            if let Some(table) = conv_object.expr_to_table.get(&expr_id) {
                                table_name = table.clone();
                                println!("Table name: {:?}", table_name);
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
