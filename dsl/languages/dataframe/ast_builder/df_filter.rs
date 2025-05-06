use crate::dsl::ir::{ColumnRef, ComparisonOp, ComplexField, Condition, FilterClause, FilterConditionType, IrLiteral, IrPlan, NullCondition, NullOp, BinaryOp};
use crate::dsl::languages::dataframe::conversion_error::ConversionError;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Process a Filter (WHERE) node from a Catalyst plan
pub fn process_filter(node: &Value, input_plan: Arc<IrPlan>, expr_to_table: &HashMap<String, String>) -> Result<Arc<IrPlan>, Box<ConversionError>> {
    // Extract the condition array
    let condition_array = node.get("condition")
        .and_then(|c| c.as_array())
        .ok_or_else(|| Box::new(ConversionError::MissingField("condition".to_string())))?;
    
    // The first element is usually the root condition expression
    if condition_array.is_empty() {
        return Err(Box::new(ConversionError::InvalidExpression));
    }
    
    // Process the condition
    let filter_clause = process_condition(&condition_array, 0, expr_to_table)?;
    
    // Create the Filter node
    Ok(Arc::new(IrPlan::Filter {
        input: input_plan,
        predicate: filter_clause,
    }))
}

/// Process a condition in the condition array, handling the decision tree traversal correctly
fn process_condition(condition_array: &[Value], idx: usize, expr_to_table: &HashMap<String, String>) -> Result<FilterClause, Box<ConversionError>> {
    if idx >= condition_array.len() {
        return Err(Box::new(ConversionError::InvalidExpression));
    }
    
    let condition = &condition_array[idx];
    
    // Extract the condition type
    let class = condition.get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;
    
    let condition_type = class.split('.').last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;
    
    match condition_type {
        "And" => {
            // Process AND condition (left AND right)
            // Next element is always the left operand
            let left_idx = idx + 1;
            
            // Process left operand and track how many elements were consumed
            let left_clause = process_condition(condition_array, left_idx, expr_to_table)?;
            
            // Determine position of right operand - this depends on how deep the left branch was
            // The right operand will be at the position after all the left branch elements
            let right_shift = determine_branch_size(condition_array, left_idx);
            let right_idx = left_idx + right_shift;
            
            if right_idx >= condition_array.len() {
                return Err(Box::new(ConversionError::InvalidExpression));
            }
            
            let right_clause = process_condition(condition_array, right_idx, expr_to_table)?;
            
            Ok(FilterClause::Expression {
                left: Box::new(left_clause),
                binary_op: BinaryOp::And,
                right: Box::new(right_clause),
            })
        },
        "Or" => {
            // Process OR condition (left OR right)
            // Similar to AND
            let left_idx = idx + 1;
            let left_clause = process_condition(condition_array, left_idx, expr_to_table)?;
            
            let right_shift = determine_branch_size(condition_array, left_idx);
            let right_idx = left_idx + right_shift;
            
            if right_idx >= condition_array.len() {
                return Err(Box::new(ConversionError::InvalidExpression));
            }
            
            let right_clause = process_condition(condition_array, right_idx, expr_to_table)?;
            
            Ok(FilterClause::Expression {
                left: Box::new(left_clause),
                binary_op: BinaryOp::Or,
                right: Box::new(right_clause),
            })
        },
        "Not" => {
            // Process NOT condition
            let child_idx = idx + 1;
            let child_condition = process_condition(condition_array, child_idx, expr_to_table)?;
            
            // Negate the condition
            match child_condition {
                FilterClause::Base(FilterConditionType::Comparison(Condition { left_field, operator, right_field })) => {
                    // Invert the comparison operator
                    let negated_operator = match operator {
                        ComparisonOp::Equal => ComparisonOp::NotEqual,
                        ComparisonOp::NotEqual => ComparisonOp::Equal,
                        ComparisonOp::GreaterThan => ComparisonOp::LessThanEquals,
                        ComparisonOp::LessThan => ComparisonOp::GreaterThanEquals,
                        ComparisonOp::GreaterThanEquals => ComparisonOp::LessThan,
                        ComparisonOp::LessThanEquals => ComparisonOp::GreaterThan,
                    };
                    
                    Ok(FilterClause::Base(FilterConditionType::Comparison(
                        Condition {
                            left_field,
                            operator: negated_operator,
                            right_field,
                        }
                    )))
                },
                FilterClause::Base(FilterConditionType::NullCheck(NullCondition { field, operator })) => {
                    // Invert the null operator
                    let negated_operator = match operator {
                        NullOp::IsNull => NullOp::IsNotNull,
                        NullOp::IsNotNull => NullOp::IsNull,
                    };
                    
                    Ok(FilterClause::Base(FilterConditionType::NullCheck(
                        NullCondition {
                            field,
                            operator: negated_operator,
                        }
                    )))
                },
                _ => Err(Box::new(ConversionError::UnsupportedExpressionType("Negation of complex condition".to_string())))
            }
        },
        "IsNotNull" => {
            // Process IS NOT NULL condition
            let child_idx = idx + 1;
            
            // The child should be the next node in the array
            if child_idx >= condition_array.len() {
                return Err(Box::new(ConversionError::InvalidExpression));
            }
            
            let child_node = &condition_array[child_idx];
            let field = process_expression(child_node, expr_to_table)?;
            
            Ok(FilterClause::Base(FilterConditionType::NullCheck(
                NullCondition {
                    field,
                    operator: NullOp::IsNotNull,
                }
            )))
        },
        "IsNull" => {
            // Process IS NULL condition
            let child_idx = idx + 1;
            
            if child_idx >= condition_array.len() {
                return Err(Box::new(ConversionError::InvalidExpression));
            }
            
            let child_node = &condition_array[child_idx];
            let field = process_expression(child_node, expr_to_table)?;
            
            Ok(FilterClause::Base(FilterConditionType::NullCheck(
                NullCondition {
                    field,
                    operator: NullOp::IsNull,
                }
            )))
        },
        "EqualTo" => {
            // Process equals condition - directly access referenced indices
            // For binary operations, left and right are absolute indices in the condition array
            let left_idx = condition.get("left")
                .and_then(|l| l.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("left".to_string())))?
                as usize;
            
            let right_idx = condition.get("right")
                .and_then(|r| r.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("right".to_string())))?
                as usize;
            
            // The indices point to elements after the current expression
            let left_node = &condition_array[idx + 1 + left_idx];
            let right_node = &condition_array[idx + 1 + right_idx];
            
            let left_field = process_expression(left_node, expr_to_table)?;
            let right_field = process_expression(right_node, expr_to_table)?;
            
            Ok(FilterClause::Base(FilterConditionType::Comparison(
                Condition {
                    left_field,
                    operator: ComparisonOp::Equal,
                    right_field,
                }
            )))
        },
        "GreaterThan" => {
            let left_idx = condition.get("left")
                .and_then(|l| l.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("left".to_string())))?
                as usize;
            
            let right_idx = condition.get("right")
                .and_then(|r| r.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("right".to_string())))?
                as usize;
            
            let left_node = &condition_array[idx + 1 + left_idx];
            let right_node = &condition_array[idx + 1 + right_idx];
            
            let left_field = process_expression(left_node, expr_to_table)?;
            let right_field = process_expression(right_node, expr_to_table)?;
            
            Ok(FilterClause::Base(FilterConditionType::Comparison(
                Condition {
                    left_field,
                    operator: ComparisonOp::GreaterThan,
                    right_field,
                }
            )))
        },
        "LessThan" => {
            let left_idx = condition.get("left")
                .and_then(|l| l.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("left".to_string())))?
                as usize;
            
            let right_idx = condition.get("right")
                .and_then(|r| r.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("right".to_string())))?
                as usize;
            
            let left_node = &condition_array[idx + 1 + left_idx];
            let right_node = &condition_array[idx + 1 + right_idx];
            
            let left_field = process_expression(left_node, expr_to_table)?;
            let right_field = process_expression(right_node, expr_to_table)?;
            
            Ok(FilterClause::Base(FilterConditionType::Comparison(
                Condition {
                    left_field,
                    operator: ComparisonOp::LessThan,
                    right_field,
                }
            )))
        },
        "GreaterThanOrEqual" => {
            let left_idx = condition.get("left")
                .and_then(|l| l.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("left".to_string())))?
                as usize;
            
            let right_idx = condition.get("right")
                .and_then(|r| r.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("right".to_string())))?
                as usize;
            
            let left_node = &condition_array[idx + 1 + left_idx];
            let right_node = &condition_array[idx + 1 + right_idx];
            
            let left_field = process_expression(left_node, expr_to_table)?;
            let right_field = process_expression(right_node, expr_to_table)?;
            
            Ok(FilterClause::Base(FilterConditionType::Comparison(
                Condition {
                    left_field,
                    operator: ComparisonOp::GreaterThanEquals,
                    right_field,
                }
            )))
        },
        "LessThanOrEqual" => {
            let left_idx = condition.get("left")
                .and_then(|l| l.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("left".to_string())))?
                as usize;
            
            let right_idx = condition.get("right")
                .and_then(|r| r.as_u64())
                .ok_or_else(|| Box::new(ConversionError::MissingField("right".to_string())))?
                as usize;
            
            let left_node = &condition_array[idx + 1 + left_idx];
            let right_node = &condition_array[idx + 1 + right_idx];
            
            let left_field = process_expression(left_node, expr_to_table)?;
            let right_field = process_expression(right_node, expr_to_table)?;
            
            Ok(FilterClause::Base(FilterConditionType::Comparison(
                Condition {
                    left_field,
                    operator: ComparisonOp::LessThanEquals,
                    right_field,
                }
            )))
        },
        "AttributeReference" => {
            // Direct reference to a column - handle as a boolean column
            let field = process_expression(condition, expr_to_table)?;
            
            // Create an implicit comparison with true
            let true_literal = ComplexField {
                column_ref: None,
                literal: Some(IrLiteral::Boolean(true)),
                aggregate: None,
                nested_expr: None,
                subquery: None, 
                subquery_vec: None,
            };
            
            Ok(FilterClause::Base(FilterConditionType::Comparison(
                Condition {
                    left_field: field,
                    operator: ComparisonOp::Equal,
                    right_field: true_literal,
                }
            )))
        },
        "Literal" => {
            // Direct literal - handle as a boolean value
            if let Some(value) = condition.get("value").and_then(|v| v.as_bool()) {
                Ok(FilterClause::Base(FilterConditionType::Boolean(value)))
            } else {
                Err(Box::new(ConversionError::InvalidExpression))
            }
        },
        _ => Err(Box::new(ConversionError::UnsupportedExpressionType(condition_type.to_string())))
    }
}

/// Helper function to determine the size of a branch starting at a given index
/// This helps calculate the position of the right operand after processing the left
fn determine_branch_size(condition_array: &[Value], start_idx: usize) -> usize {
    // Start with a size of 1 for the root node of this branch
    let mut size = 1;
    
    if start_idx >= condition_array.len() {
        return size;
    }
    
    let condition = &condition_array[start_idx];
    
    // Get the class of the condition
    let class = condition.get("class")
        .and_then(|c| c.as_str())
        .map(|c| c.split('.').last().unwrap_or(""))
        .unwrap_or("");
    
    match class {
        "And" | "Or" => {
            // For boolean operators, we need to account for both branches
            // First process the left branch
            let left_size = determine_branch_size(condition_array, start_idx + 1);
            // Then process the right branch
            let right_size = determine_branch_size(condition_array, start_idx + 1 + left_size);
            // Total size is current node + left branch + right branch
            size += left_size + right_size;
        },
        "Not" | "IsNotNull" | "IsNull" => {
            // For unary operators, we need to account for their child
            if let Some(branch_size) = condition_array.get(start_idx + 1).map(|_| determine_branch_size(condition_array, start_idx + 1)) {
                size += branch_size;
            }
        },
        "EqualTo" | "GreaterThan" | "LessThan" | "GreaterThanOrEqual" | "LessThanOrEqual" => {
            // For comparison operators, we need to account for left and right operands
            // These are typically fixed size (1 each for left and right)
            size += 2;
        },
        _ => {
            // For leaf nodes (literals, column references), size is just 1 (already counted)
        }
    }
    
    size
}

/// Process an expression to create a ComplexField
fn process_expression(expr: &Value, expr_to_table: &HashMap<String, String>) -> Result<ComplexField, Box<ConversionError>> {
    // Extract the expression type
    let class = expr.get("class")
        .and_then(|c| c.as_str())
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;
    
    let expr_type = class.split('.').last()
        .ok_or_else(|| Box::new(ConversionError::InvalidClassName))?;
    
    match expr_type {
        "AttributeReference" => {
            // This is a column reference
            let column_name = expr.get("name")
                .and_then(|n| n.as_str())
                .ok_or_else(|| Box::new(ConversionError::MissingField("name".to_string())))?
                .to_string();
            
            // Extract expression ID to look up table name
            let expr_id = match get_expr_id(expr) {
                Ok(id) => id,
                Err(_) => {
                    // Default to empty ID if not found
                    String::new()
                }
            };
            
            // Look up table name from mapping
            let table = expr_to_table.get(&expr_id).cloned();
            
            // Create column reference
            let column_ref = ColumnRef {
                table,
                column: column_name,
            };
            
            Ok(ComplexField {
                column_ref: Some(column_ref),
                literal: None,
                aggregate: None,
                nested_expr: None,
                subquery: None,
                subquery_vec: None,
            })
        },
        "Literal" => {
            // This is a literal value
            let value = expr.get("value")
                .ok_or_else(|| Box::new(ConversionError::MissingField("value".to_string())))?;  
            // Get the data type
            let data_type = expr.get("dataType")
                .and_then(|dt| dt.as_str())
                .ok_or_else(|| Box::new(ConversionError::MissingField("dataType".to_string())))?;
            
            // Convert the value based on data type
            let literal = match data_type {
                "long" | "int" => {
                    if let Some(i) = value.as_i64() {
                        IrLiteral::Integer(i)
                    } else {
                        return Err(Box::new(ConversionError::InvalidExpression));
                    }
                },
                "float" | "double" => {
                    if let Some(f) = value.as_f64() {
                        IrLiteral::Float(f)
                    } else {
                        return Err(Box::new(ConversionError::InvalidExpression));
                    }
                },
                "string" => {
                    if let Some(s) = value.as_str() {
                        IrLiteral::String(s.to_string())
                    } else {
                        return Err(Box::new(ConversionError::InvalidExpression));
                    }
                },
                "boolean" => {
                    if let Some(b) = value.as_bool() {
                        IrLiteral::Boolean(b)
                    } else {
                        return Err(Box::new(ConversionError::InvalidExpression));
                    }
                },
                _ => return Err(Box::new(ConversionError::UnsupportedExpressionType(data_type.to_string())))
            };
            
            Ok(ComplexField {
                column_ref: None,
                literal: Some(literal),
                aggregate: None,
                nested_expr: None,
                subquery: None,
                subquery_vec: None,
            })
        },
        // Handle other types of expressions if needed
        _ => Err(Box::new(ConversionError::UnsupportedExpressionType(expr_type.to_string())))
    }
}

/// Extract an expression ID as a string
fn get_expr_id(expr: &Value) -> Result<String, Box<ConversionError>> {
    let expr_id = expr.get("exprId")
        .ok_or_else(|| Box::new(ConversionError::MissingField("exprId".to_string())))?;
    
    let id = expr_id.get("id")
        .and_then(|id| id.as_u64())
        .ok_or_else(|| Box::new(ConversionError::MissingField("id".to_string())))?;
    
    let jvm_id = expr_id.get("jvmId")
        .and_then(|j| j.as_str())
        .ok_or_else(|| Box::new(ConversionError::MissingField("jvmId".to_string())))?;
    
    Ok(format!("{}_{}", id, jvm_id))
}