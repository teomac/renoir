use core::panic;
use std::{io, sync::Arc};

use crate::dsl::ir::ast_parser::ir_ast_structure::ComplexField;
use crate::dsl::ir::{ir_ast_to_renoir, InCondition};
use crate::dsl::{
    ir::{
        literal::LiteralParser, Condition, FilterClause, FilterConditionType, GroupBaseCondition,
        GroupClause, IrPlan, NullCondition, ProjectionColumn,
    },
    struct_object::object::QueryObject,
};

use super::old_subquery_csv;

pub fn old_manage_subqueries(
    ir_ast: &Arc<IrPlan>,
    output_path: &String,
    query_object: &mut QueryObject,
) -> io::Result<Arc<IrPlan>> {
    match &**ir_ast {
        IrPlan::Project {
            input,
            columns,
            distinct,
        } => {
            // First recursively process any subqueries in the input
            let processed_input = old_manage_subqueries(input, output_path, query_object)?;

            // Process columns to find and replace subqueries
            let processed_columns = columns
                .iter()
                .map(|col| {
                    match col {
                        ProjectionColumn::Subquery(subquery, alias) => {
                            // Recursively process nested subqueries within this subquery
                            let processed_subquery =
                                old_manage_subqueries(subquery, output_path, query_object)
                                    .expect("Failed to process nested subquery");

                            // Execute the processed subquery to get its result
                            let mut result = old_subquery_csv(
                                processed_subquery,
                                output_path,
                                query_object.tables_info.clone(),
                                query_object.table_to_csv.clone(),
                            );
                            // Convert result to StringLiteral with the same alias
                            // Clean up result, remove square brackets
                            result = result.trim().trim_start_matches('[').trim_end_matches(']').to_string();

                            // Clean up the result string - remove quotes and whitespace/newlines
                            result = result.trim().trim_matches('"').to_string();

                            println!("Result: {}", result);

                            ProjectionColumn::StringLiteral(result, alias.clone())
                        }
                        // Add handling for complex values that might contain subqueries
                        ProjectionColumn::ComplexValue(complex_field, alias) => {
                            match process_complex_field(complex_field, output_path, query_object) {
                                Ok(processed_field) => {
                                    ProjectionColumn::ComplexValue(processed_field, alias.clone())
                                }
                                Err(e) => panic!("Error processing complex field: {}", e),
                            }
                        }
                        // Preserve non-subquery columns as-is
                        _ => col.clone(),
                    }
                })
                .collect();

            // Return new Project node with processed input and columns
            Ok(Arc::new(IrPlan::Project {
                input: processed_input,
                columns: processed_columns,
                distinct: *distinct,
            }))
        }
        // Recursively process other node types
        IrPlan::Filter { input, predicate } => {
            // Process input first
            let processed_input = old_manage_subqueries(input, output_path, query_object)?;

            // Process the predicate to handle any subqueries
            let processed_predicate =
                process_filter_condition(predicate, output_path, query_object)?;

            Ok(Arc::new(IrPlan::Filter {
                input: processed_input,
                predicate: processed_predicate,
            }))
        }
        IrPlan::GroupBy {
            input,
            keys,
            group_condition,
        } => {
            let processed_input = old_manage_subqueries(input, output_path, query_object)?;

            // Process the group condition if it exists
            let processed_condition = if let Some(condition) = group_condition {
                Some(process_group_condition(
                    condition,
                    output_path,
                    query_object,
                )?)
            } else {
                None
            };

            Ok(Arc::new(IrPlan::GroupBy {
                input: processed_input,
                keys: keys.clone(),
                group_condition: processed_condition,
            }))
        }
        IrPlan::Join {
            left,
            right,
            condition,
            join_type,
        } => {
            let processed_left = old_manage_subqueries(left, output_path, query_object)?;
            let processed_right = old_manage_subqueries(right, output_path, query_object)?;
            Ok(Arc::new(IrPlan::Join {
                left: processed_left,
                right: processed_right,
                condition: condition.clone(),
                join_type: join_type.clone(),
            }))
        }
        IrPlan::OrderBy { input, items } => {
            let processed_input = old_manage_subqueries(input, output_path, query_object)?;
            Ok(Arc::new(IrPlan::OrderBy {
                input: processed_input,
                items: items.clone(),
            }))
        }
        IrPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let processed_input = old_manage_subqueries(input, output_path, query_object)?;
            Ok(Arc::new(IrPlan::Limit {
                input: processed_input,
                limit: *limit,
                offset: *offset,
            }))
        }
        IrPlan::Scan {
            input,
            stream_name,
            alias,
        } => {
            if !matches!(
                &**input,
                IrPlan::Project {
                    input: _,
                    columns: _,
                    distinct: _
                }
            ) {
                Ok(ir_ast.clone())
            } else {
                let processed_input = manage_nested_join(input, stream_name, alias, query_object);
                Ok(Arc::new(IrPlan::Scan {
                    input: processed_input,
                    stream_name: stream_name.clone(),
                    alias: alias.clone(),
                }))
            }
        }
        // Base case - table nodes have no nested queries to process
        IrPlan::Table { .. } => Ok(ir_ast.clone()),
    }
}

fn process_complex_field(
    field: &ComplexField,
    output_path: &String,
    query_object: &mut QueryObject,
) -> io::Result<ComplexField> {
    match field {
        ComplexField {
            subquery: Some(subquery),
            ..
        } => {
            // Process the subquery itself first in case it contains nested subqueries
            let processed_subquery = old_manage_subqueries(subquery, output_path, query_object)?;

            // Execute the subquery to get result
            let result = old_subquery_csv(
                processed_subquery,
                output_path,
                query_object.tables_info.clone(),
                query_object.table_to_csv.clone(),
            );

            // Parse the result vector
            let values: Vec<&str> = result
                .trim()
                .trim_start_matches('[')
                .trim_end_matches(']')
                .split(',')
                .map(|s| s.trim().trim_matches('"'))
                .collect();

            // Validate that subquery returns exactly one value
            if values.len() > 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Subquery must return exactly one value when used in SELECT clause",
                ));
            }

            // Get the single value or empty string if no results
            let result_str = values.first().copied().unwrap_or("").to_string();

            // Convert the result string to appropriate IrLiteral
            let literal = LiteralParser::parse(&result_str)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

            // Return new ComplexField with just the literal
            Ok(ComplexField {
                column_ref: None,
                literal: Some(literal),
                aggregate: None,
                nested_expr: None,
                subquery: None,
                subquery_vec: None,
            })
        }
        ComplexField {
            nested_expr: Some(box_expr),
            ..
        } => {
            let (left, op, right) = &**box_expr;

            // Recursively process both sides of the expression
            let processed_left = process_complex_field(left, output_path, query_object)?;
            let processed_right = process_complex_field(right, output_path, query_object)?;

            Ok(ComplexField {
                column_ref: None,
                literal: None,
                aggregate: None,
                nested_expr: Some(Box::new((processed_left, op.clone(), processed_right))),
                subquery: None,
                subquery_vec: None,
            })
        }
        // Other cases just return clone of original field since they can't contain subqueries
        _ => Ok(field.clone()),
    }
}

fn process_filter_condition(
    condition: &FilterClause,
    output_path: &String,
    query_object: &mut QueryObject,
) -> io::Result<FilterClause> {
    match condition {
        FilterClause::Base(base_condition) => {
            match base_condition {
                FilterConditionType::Comparison(comparison) => {
                    // Process both left and right fields for subqueries
                    let processed_left =
                        process_complex_field(&comparison.left_field, output_path, query_object)?;
                    let processed_right =
                        process_complex_field(&comparison.right_field, output_path, query_object)?;

                    Ok(FilterClause::Base(FilterConditionType::Comparison(
                        Condition {
                            left_field: processed_left,
                            operator: comparison.operator.clone(),
                            right_field: processed_right,
                        },
                    )))
                }
                FilterConditionType::NullCheck(null_check) => {
                    // Process the field for subqueries
                    let processed_field =
                        process_complex_field(&null_check.field, output_path, query_object)?;

                    Ok(FilterClause::Base(FilterConditionType::NullCheck(
                        NullCondition {
                            field: processed_field,
                            operator: null_check.operator.clone(),
                        },
                    )))
                }
                FilterConditionType::Exists(exists_subquery, is_negated) => {
                    // Process the nested subqueries first
                    let processed_subquery =
                        old_manage_subqueries(exists_subquery, output_path, query_object)?;

                    // Execute the subquery
                    let result = old_subquery_csv(
                        processed_subquery,
                        output_path,
                        query_object.tables_info.clone(),
                        query_object.table_to_csv.clone(),
                    );

                    // Check if subquery returned any results
                    let has_results = result.trim() != "[]";
                    let bool_result = if *is_negated {
                        !has_results
                    } else {
                        has_results
                    };

                    Ok(FilterClause::Base(FilterConditionType::Boolean(
                        bool_result,
                    )))
                }
                FilterConditionType::Boolean(boolean) => {
                    Ok(FilterClause::Base(FilterConditionType::Boolean(*boolean)))
                }
                FilterConditionType::ExistsVec(vec, negated ) =>{
                    Ok(FilterClause::Base(FilterConditionType::ExistsVec(vec.clone(), *negated)))
                }
                FilterConditionType::In(in_condition) => {
                    match in_condition {
                        InCondition::InSubquery {
                            field,
                            subquery,
                            negated,
                        } => {
                            // Process the subquery
                            let processed_subquery =
                                old_manage_subqueries(subquery, output_path, query_object)?;

                            // Execute the subquery
                            let result = old_subquery_csv(
                                processed_subquery,
                                output_path,
                                query_object.tables_info.clone(),
                                query_object.table_to_csv.clone(),
                            );

                            let has_results = !result.trim().is_empty();
                            let mut ir_literals = Vec::new();
                            if has_results {
                                // convert the result to a vector of irLiterals
                                let mut values: Vec<&str> = result
                                    .trim()
                                    .trim_start_matches('[')
                                    .trim_end_matches(']')
                                    .split(',')
                                    .map(|s| s.trim().trim_matches('"'))
                                    .collect();
                                values.sort();
                                values.dedup();

                                for value in values {
                                    let literal = LiteralParser::parse(value).map_err(|e| {
                                        io::Error::new(io::ErrorKind::Other, e.to_string())
                                    })?;
                                    ir_literals.push(literal);
                                }
                            }

                            Ok(FilterClause::Base(FilterConditionType::In(
                                InCondition::In {
                                    field: field.clone(),
                                    values: ir_literals,
                                    negated: *negated,
                                },
                            )))
                        }
                        InCondition::In { .. } => panic!("We should not get here"),
                        InCondition::InVec { .. } => panic!("We should not get here"),
                    }
                }
            }
        }
        FilterClause::Expression {
            left,
            binary_op,
            right,
        } => {
            // Recursively process both sides of the expression
            let processed_left = process_filter_condition(left, output_path, query_object)?;
            let processed_right = process_filter_condition(right, output_path, query_object)?;

            Ok(FilterClause::Expression {
                left: Box::new(processed_left),
                binary_op: binary_op.clone(),
                right: Box::new(processed_right),
            })
        }
    }
}

fn process_group_condition(
    condition: &GroupClause,
    output_path: &String,
    query_object: &mut QueryObject,
) -> io::Result<GroupClause> {
    match condition {
        GroupClause::Base(base_condition) => {
            match base_condition {
                GroupBaseCondition::Comparison(comparison) => {
                    // Process both left and right fields for subqueries
                    let processed_left =
                        process_complex_field(&comparison.left_field, output_path, query_object)?;
                    let processed_right =
                        process_complex_field(&comparison.right_field, output_path, query_object)?;

                    Ok(GroupClause::Base(GroupBaseCondition::Comparison(
                        Condition {
                            left_field: processed_left,
                            operator: comparison.operator.clone(),
                            right_field: processed_right,
                        },
                    )))
                }
                GroupBaseCondition::NullCheck(null_check) => {
                    // Process the field for subqueries
                    let processed_field =
                        process_complex_field(&null_check.field, output_path, query_object)?;

                    Ok(GroupClause::Base(GroupBaseCondition::NullCheck(
                        NullCondition {
                            field: processed_field,
                            operator: null_check.operator.clone(),
                        },
                    )))
                }
                GroupBaseCondition::Exists(ir_plan, is_negated) => {
                    // Process the subquery
                    let processed_subquery = old_manage_subqueries(ir_plan, output_path, query_object)?;

                    // Execute the subquery
                    let result = old_subquery_csv(
                        processed_subquery,
                        output_path,
                        query_object.tables_info.clone(),
                        query_object.table_to_csv.clone(),
                    );

                    // Check if subquery returned any results
                    let has_results = !result.trim().is_empty();
                    let bool_result = if *is_negated {
                        !has_results
                    } else {
                        has_results
                    };

                    Ok(GroupClause::Base(GroupBaseCondition::Boolean(bool_result)))
                }
                GroupBaseCondition::Boolean(boolean) => {
                    Ok(GroupClause::Base(GroupBaseCondition::Boolean(*boolean)))
                }
                GroupBaseCondition::ExistsVec(vec, negated) => {
                    Ok(GroupClause::Base(GroupBaseCondition::ExistsVec(vec.clone(), *negated)))
                }
                GroupBaseCondition::In(in_condition) => {
                    match in_condition {
                        InCondition::InSubquery {
                            field,
                            subquery,
                            negated,
                        } => {
                            // Process the subquery
                            let processed_subquery =
                                old_manage_subqueries(subquery, output_path, query_object)?;

                            // Execute the subquery
                            let result = old_subquery_csv(
                                processed_subquery,
                                output_path,
                                query_object.tables_info.clone(),
                                query_object.table_to_csv.clone(),
                            );

                            let has_results = !result.trim().is_empty();
                            let mut ir_literals = Vec::new();
                            if has_results {
                                // convert the result to a vector of irLiterals
                                let mut values: Vec<&str> = result
                                    .trim()
                                    .trim_start_matches('[')
                                    .trim_end_matches(']')
                                    .split(',')
                                    .map(|s| s.trim().trim_matches('"'))
                                    .collect();
                                values.sort();
                                values.dedup();

                                for value in values {
                                    let literal = LiteralParser::parse(value).map_err(|e| {
                                        io::Error::new(io::ErrorKind::Other, e.to_string())
                                    })?;
                                    ir_literals.push(literal);
                                }
                            }

                            Ok(GroupClause::Base(GroupBaseCondition::In(InCondition::In {
                                field: field.clone(),
                                values: ir_literals,
                                negated: *negated,
                            })))
                        }
                        InCondition::In { .. } => panic!("We should not get here"),
                        InCondition::InVec { .. } => panic!("We should not get here"),
                    }
                }
            }
        }
        GroupClause::Expression { left, op, right } => {
            // Recursively process both sides of the expression
            let processed_left = process_group_condition(left, output_path, query_object)?;
            let processed_right = process_group_condition(right, output_path, query_object)?;

            Ok(GroupClause::Expression {
                left: Box::new(processed_left),
                op: op.clone(),
                right: Box::new(processed_right),
            })
        }
    }
}

fn manage_nested_join(
    input: &Arc<IrPlan>,
    stream_name: &String,
    alias: &Option<String>,
    query_object: &mut QueryObject,
) -> Arc<IrPlan> {
    // process all the subqueries in the nested joins
    let processed_input = old_manage_subqueries(input, &query_object.output_path.clone(), query_object)
        .expect("Failed to process nested join subqueries");

    let mut object = query_object.clone().populate(&processed_input);
    let _ = ir_ast_to_renoir(&mut object);
    let binding = object.clone();
    let stream = binding.get_stream(stream_name);

    query_object
        .streams
        .insert(stream_name.clone(), stream.clone());

    query_object
        .table_to_struct_name
        .insert(alias.clone().unwrap(), stream.final_struct_name.last().unwrap().to_string());
    query_object
        .alias_to_stream
        .insert(alias.clone().unwrap(), stream_name.to_string());
    query_object
        .tables_info
        .insert(alias.clone().unwrap(), stream.final_struct.clone());
    query_object.structs.insert(
        stream.final_struct_name.last().unwrap().clone(),
        stream.final_struct.clone(),
    );

    Arc::new(IrPlan::Table {
        table_name: alias.clone().unwrap(),
    })
}