use std::sync::Arc;

use serde_json::Value;

use crate::dsl::{ir::{ColumnRef, IrPlan, JoinCondition, JoinType, ProjectionColumn}, languages::dataframe::{conversion_error::ConversionError, converter::process_node}};

use super::df_utils::ConverterObject;

pub fn process_join(node: &Value, left_child: Arc<IrPlan>, right_child: Arc<IrPlan>, conv_object: &ConverterObject) -> Result<Arc<IrPlan>, Box<ConversionError>> {
    let join_type = node.get("joinType")
                .ok_or_else(|| Box::new(ConversionError::MissingField("joinType".to_string()))).unwrap();

            let join_type_str = join_type.get("object")
                .and_then(|o| o.as_str())
                .ok_or_else(|| Box::new(ConversionError::InvalidJoinType))?
                .split('.')
                .last()
                .ok_or_else(|| Box::new(ConversionError::InvalidJoinType))?;

            let join_type_final = match join_type_str {
                "Inner$" => JoinType::Inner,
                "LeftOuter$" => JoinType::Left,
                "FullOuter$" => JoinType::Outer,
                _ => return Err(Box::new(ConversionError::UnsupportedJoinType(join_type_str.to_string()))),
            };

             // Extract the condition array
            let condition_array = node.get("condition")
            .and_then(|c| c.as_array())
            .ok_or_else(|| Box::new(ConversionError::MissingField("condition".to_string())))?;

            // The first element is usually the root condition expression
            if condition_array.is_empty() {
                return Err(Box::new(ConversionError::InvalidExpression));
            }

            let join_condition = process_join_condition(&condition_array, 0, conv_object)?;

            Ok(Arc::new(IrPlan::Join {
                left: left_child,
                right: right_child,
                join_type: join_type_final,
                condition: join_condition.0,
            }))
}

pub fn process_join_child(child_index: usize, full_plan: &[Value], stream_index: &mut usize, conv_object: &ConverterObject) -> Result<(Arc<IrPlan>, usize), Box<ConversionError>> {
    //get the child node
    let child_node = full_plan.get(child_index)
        .ok_or_else(|| Box::new(ConversionError::InvalidChildIndex))?;

    //process the child node using the process_node function
    let child_ir = process_node(child_node, child_index, full_plan, stream_index, conv_object)?;

    let processed_child_node = match &*child_ir.0 {
        //if the child node is a Project node, we need to create a Scan node with the project as input
        IrPlan::Project { columns, .. } => {
            // Create a Scan node with the project as input
            
            //first we retrieve the table name from the child node
            //iterate over the columns vector and get the first ColumnRef object
            let column_ref = columns.iter()
                .find_map(|col| match col.clone() {
                    ProjectionColumn::Column (col_ref, _)=> Some(col_ref.clone()),
                    _ => None,
                })
                .ok_or_else(|| Box::new(ConversionError::MissingField("ColumnRef".to_string())))?;

            let scan_node = IrPlan::Scan {
                input: child_ir.0,
                stream_name: {
                    let stream_name = format!("stream_{}", stream_index);
                    *stream_index += 1; // Increment the stream index for the next node
                    stream_name
                },
                alias: column_ref.table,
              
            };
            *stream_index += 1; // Increment the stream index for the next node
            Arc::new(scan_node)
        }
     _ => child_ir.0.clone()  //in any other case, we just return the child node
    };

    // Return the processed child node and the updated index
    Ok((processed_child_node, child_ir.1))
}


pub fn process_join_condition(condition_array: &[Value], idx: usize, conv_object: &ConverterObject) -> Result<(Vec<JoinCondition>, usize), Box<ConversionError>> {
    if idx >= condition_array.len() {
        return Err(Box::new(ConversionError::InvalidExpression));
    }
    
    let node = &condition_array[idx];
    
    // Get the class name
    let class = node.get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;
    
    let node_type = class.split('.').last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;


    println!("Processing node type: {}", node_type);
    
    match node_type {
        "And" => process_binary_operator(condition_array, idx, conv_object),
        "EqualTo" => process_comparison(condition_array, idx, conv_object),
        _ => Err(Box::new(ConversionError::UnsupportedExpressionType(node_type.to_string()))),
    }
}

pub fn process_attribute_reference(condition_array: &[Value], idx: usize, conv_object: &ConverterObject) -> Result<(ColumnRef, usize), Box<ConversionError>> {
    let node = &condition_array[idx];
    
    // Create a column reference using the utility function
    let column_ref = conv_object.create_column_ref(node)?;
    
    Ok((
            column_ref,
            idx + 1
    ))
}

pub fn process_binary_operator(condition_array: &[Value], idx: usize, conv_object: &ConverterObject) -> Result<(Vec<JoinCondition>, usize), Box<ConversionError>> {
    // Process left operand (always the next node)
    let (left_clause, next_idx) = process_comparison(condition_array, idx + 1, conv_object)?;
    
    // Process right operand (starts after the left branch is complete)
    let (right_clause, final_idx) = process_comparison(condition_array, next_idx, conv_object)?;
    
    

    Ok((
        [left_clause[0].clone(), right_clause[0].clone()].to_vec(),
        final_idx
    ))
}

pub fn process_comparison(condition_array: &[Value], idx: usize, conv_object: &ConverterObject) -> Result<(Vec<JoinCondition>, usize), Box<ConversionError>> {
    let node = &condition_array[idx];
    
    // Get indices for left and right expressions
    let left_idx = node.get("left").and_then(|l| l.as_u64())
        .ok_or_else(|| Box::new(ConversionError::MissingField("left".to_string())))? as usize;
    
    // Process left expression
    let (left_field, next_idx) = process_attribute_reference(condition_array, idx + left_idx +1, conv_object)?;
    
    // Process right expression
    let (right_field, final_idx) = process_attribute_reference(condition_array, next_idx, conv_object)?;
    

    Ok((
        [JoinCondition {
            left_col: left_field,
            right_col: right_field,
        }].to_vec(),
        final_idx
    ))
}