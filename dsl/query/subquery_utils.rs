use crate::dsl::ir::ast_builder::ir_ast_structure::ComplexField;
use crate::dsl::ir::{ExistsCondition, InCondition};
use crate::dsl::query::subquery_process::*;
use crate::dsl::{
    ir::{
        Condition, FilterClause, FilterConditionType, GroupBaseCondition, GroupClause, IrPlan,
        NullCondition, ProjectionColumn,
    },
    struct_object::object::QueryObject,
};
use core::panic;
use std::{io, sync::Arc};

// Processes all the IR AST nodes in order to find and handle any subqueries in the main query.
// This function is called recursively to ensure that all nested subqueries are processed correctly.
//
// # Arguments
//
// * `ir_ast` - An `Arc<IrPlan>` that holds the intermediate representation of the query.
// * `query_object` - A mutable reference to a `QueryObject` that holds the query information.
//
// # Returns
//
// * `io::Result<Arc<IrPlan>>` - Returns an `Ok` variant with the processed IR AST if successful,
//   or an `Err` variant with an `io::Error` if an error occurs.
pub(crate) fn manage_subqueries(
    ir_ast: &Arc<IrPlan>,
    query_object: &mut QueryObject,
) -> io::Result<Arc<IrPlan>> {
    match &**ir_ast {
        IrPlan::Project {
            input,
            columns,
            distinct,
        } => {
            // First recursively process any subqueries in the input
            let processed_input = manage_subqueries(input, query_object)?;

            // Process columns to find and replace subqueries
            let processed_columns = columns
                .iter()
                .map(|col| {
                    match col {
                        ProjectionColumn::Subquery(subquery, alias) => {
                            // Recursively process nested subqueries within this subquery
                            let processed_subquery = manage_subqueries(subquery, query_object)
                                .expect("Failed to process nested subquery");

                            let (result, _, fields) = subquery_result(
                                processed_subquery,
                                &query_object.output_path,
                                query_object.tables_info.clone(),
                                query_object.table_to_csv.clone(),
                                true,
                            );

                            let temp_fields = query_object.get_mut_fields();
                            temp_fields.fill(fields.structs.clone(), fields.streams.clone());

                            ProjectionColumn::SubqueryVec(result, alias.clone())
                        }
                        // Add handling for complex values that might contain subqueries
                        ProjectionColumn::ComplexValue(complex_field, alias) => {
                            match process_complex_field(complex_field, query_object) {
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
            let processed_input = manage_subqueries(input, query_object)?;

            // Process the predicate to handle any subqueries
            let processed_predicate = process_filter_condition(predicate, query_object)?;

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
            let processed_input = manage_subqueries(input, query_object)?;

            // Process the group condition if it exists
            let processed_condition = if let Some(condition) = group_condition {
                Some(process_group_condition(condition, query_object)?)
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
            let processed_left = manage_subqueries(left, query_object)?;
            let processed_right = manage_subqueries(right, query_object)?;
            Ok(Arc::new(IrPlan::Join {
                left: processed_left,
                right: processed_right,
                condition: condition.clone(),
                join_type: join_type.clone(),
            }))
        }
        IrPlan::OrderBy { input, items } => {
            let processed_input = manage_subqueries(input, query_object)?;
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
            let processed_input = manage_subqueries(input, query_object)?;
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
            if matches!(&**input, IrPlan::Table { table_name: _ }) {
                Ok(ir_ast.clone())
            } else {
                let processed_input = process_scan(input, query_object);
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

//Processes complex fields in a recursive manner
fn process_complex_field(
    field: &ComplexField,
    query_object: &mut QueryObject,
) -> io::Result<ComplexField> {
    match field {
        ComplexField {
            subquery: Some(subquery),
            ..
        } => {
            // Process the subquery itself first in case it contains nested subqueries
            let processed_subquery = manage_subqueries(subquery, query_object)?;

            // Execute the subquery to get result
            let (result, result_type, fields) = subquery_result(
                processed_subquery,
                &query_object.output_path,
                query_object.tables_info.clone(),
                query_object.table_to_csv.clone(),
                true,
            );

            let temp_fields = query_object.get_mut_fields();
            temp_fields.fill(fields.structs.clone(), fields.streams.clone());

            // Return new ComplexField with just the literal
            Ok(ComplexField {
                column_ref: None,
                literal: None,
                aggregate: None,
                nested_expr: None,
                subquery: None,
                subquery_vec: Some((result, result_type)),
            })
        }
        ComplexField {
            nested_expr: Some(box_expr),
            ..
        } => {
            let (left, op, right, is_par) = &**box_expr;

            // Recursively process both sides of the expression
            let processed_left = process_complex_field(left, query_object)?;
            let processed_right = process_complex_field(right, query_object)?;

            Ok(ComplexField {
                column_ref: None,
                literal: None,
                aggregate: None,
                nested_expr: Some(Box::new((
                    processed_left,
                    op.clone(),
                    processed_right,
                    *is_par,
                ))),
                subquery: None,
                subquery_vec: None,
            })
        }
        // Other cases just return clone of original field since they can't contain subqueries
        _ => Ok(field.clone()),
    }
}

// Processes filter conditions, including subqueries and complex expressions
fn process_filter_condition(
    condition: &FilterClause,
    query_object: &mut QueryObject,
) -> io::Result<FilterClause> {
    match condition {
        FilterClause::Base(base_condition) => {
            match base_condition {
                FilterConditionType::Comparison(comparison) => {
                    // Process both left and right fields for subqueries
                    let processed_left =
                        process_complex_field(&comparison.left_field, query_object)?;
                    let processed_right =
                        process_complex_field(&comparison.right_field, query_object)?;

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
                    let processed_field = process_complex_field(&null_check.field, query_object)?;

                    Ok(FilterClause::Base(FilterConditionType::NullCheck(
                        NullCondition {
                            field: processed_field,
                            operator: null_check.operator.clone(),
                        },
                    )))
                }
                FilterConditionType::Exists(exists) => {
                    match exists {
                        ExistsCondition::Subquery { subquery, negated } => {
                            // Process the nested subqueries first
                            let processed_subquery = manage_subqueries(subquery, query_object)?;

                            // Execute the subquery
                            let (result, _, fields) = subquery_result(
                                processed_subquery,
                                &query_object.output_path,
                                query_object.tables_info.clone(),
                                query_object.table_to_csv.clone(),
                                false,
                            );

                            let temp_fields = query_object.get_mut_fields();
                            temp_fields.fill(fields.structs.clone(), fields.streams.clone());

                            Ok(FilterClause::Base(FilterConditionType::Exists(
                                ExistsCondition::Vec {
                                    vector_name: result.clone(),
                                    negated: *negated,
                                },
                            )))
                        }
                        ExistsCondition::Vec {
                            vector_name,
                            negated,
                        } => Ok(FilterClause::Base(FilterConditionType::Exists(
                            ExistsCondition::Vec {
                                vector_name: vector_name.clone(),
                                negated: *negated,
                            },
                        ))),
                    }
                }
                FilterConditionType::Boolean(boolean) => {
                    Ok(FilterClause::Base(FilterConditionType::Boolean(*boolean)))
                }
                FilterConditionType::In(in_condition) => {
                    match in_condition {
                        InCondition::Subquery {
                            field,
                            subquery,
                            negated,
                        } => {
                            //two possible cases:
                            //first, the field is a subquery

                            if field.subquery.is_some() {
                                let in_subquery = field.subquery.clone().unwrap();
                                // Process the in subquery
                                let processed_in_subquery =
                                    manage_subqueries(&in_subquery, query_object)?;

                                // Process the subquery
                                let processed_subquery = manage_subqueries(subquery, query_object)?;

                                let tables_info = query_object.tables_info.clone();
                                let table_to_csv = query_object.table_to_csv.clone();

                                // Execute the in subquery
                                let (in_result, in_result_type, in_fields) = subquery_result(
                                    processed_in_subquery,
                                    &query_object.output_path,
                                    tables_info.clone(),
                                    table_to_csv.clone(),
                                    true,
                                );

                                let temp_fields = query_object.get_mut_fields();
                                temp_fields
                                    .fill(in_fields.structs.clone(), in_fields.streams.clone());

                                // Execute the subquery
                                let (result, result_type, fields) = subquery_result(
                                    processed_subquery,
                                    &query_object.output_path,
                                    tables_info,
                                    table_to_csv,
                                    false,
                                );

                                let temp_fields = query_object.get_mut_fields();
                                temp_fields.fill(fields.structs.clone(), fields.streams.clone());

                                Ok(FilterClause::Base(FilterConditionType::In(
                                    InCondition::Vec {
                                        field: ComplexField {
                                            subquery_vec: Some((in_result, in_result_type)),
                                            subquery: None,
                                            column_ref: None,
                                            literal: None,
                                            aggregate: None,
                                            nested_expr: None,
                                        },
                                        vector_name: result,
                                        vector_type: result_type,
                                        negated: *negated,
                                    },
                                )))
                            } else {
                                //second: the field is either a column_ref or a complex_expr
                                //process complex_expr
                                let processed_field = process_complex_field(field, query_object)?;
                                // Process the subquery
                                let processed_subquery = manage_subqueries(subquery, query_object)?;

                                // Execute the subquery
                                let (result, result_type, fields) = subquery_result(
                                    processed_subquery,
                                    &query_object.output_path,
                                    query_object.tables_info.clone(),
                                    query_object.table_to_csv.clone(),
                                    false,
                                );

                                let temp_fields = query_object.get_mut_fields();
                                temp_fields.fill(fields.structs.clone(), fields.streams.clone());

                                Ok(FilterClause::Base(FilterConditionType::In(
                                    InCondition::Vec {
                                        field: processed_field,
                                        vector_name: result,
                                        vector_type: result_type,
                                        negated: *negated,
                                    },
                                )))
                            }
                        }
                        InCondition::Vec {
                            field,
                            vector_name,
                            vector_type,
                            negated,
                        } => {
                            // Process the field for subqueries
                            let processed_field = process_complex_field(field, query_object)?;

                            Ok(FilterClause::Base(FilterConditionType::In(
                                InCondition::Vec {
                                    field: processed_field,
                                    vector_name: vector_name.clone(),
                                    vector_type: vector_type.clone(),
                                    negated: *negated,
                                },
                            )))
                        }
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
            let processed_left = process_filter_condition(left, query_object)?;
            let processed_right = process_filter_condition(right, query_object)?;

            Ok(FilterClause::Expression {
                left: Box::new(processed_left),
                binary_op: binary_op.clone(),
                right: Box::new(processed_right),
            })
        }
    }
}

// Processes group conditions, including subqueries and complex expressions
fn process_group_condition(
    condition: &GroupClause,
    query_object: &mut QueryObject,
) -> io::Result<GroupClause> {
    match condition {
        GroupClause::Base(base_condition) => {
            match base_condition {
                GroupBaseCondition::Comparison(comparison) => {
                    // Process both left and right fields for subqueries
                    let processed_left =
                        process_complex_field(&comparison.left_field, query_object)?;
                    let processed_right =
                        process_complex_field(&comparison.right_field, query_object)?;

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
                    let processed_field = process_complex_field(&null_check.field, query_object)?;

                    Ok(GroupClause::Base(GroupBaseCondition::NullCheck(
                        NullCondition {
                            field: processed_field,
                            operator: null_check.operator.clone(),
                        },
                    )))
                }
                GroupBaseCondition::Exists(exists) => {
                    match exists {
                        ExistsCondition::Subquery { subquery, negated } => {
                            // Process the subquery
                            let processed_subquery = manage_subqueries(subquery, query_object)?;

                            // Execute the subquery
                            let (result, _, fields) = subquery_result(
                                processed_subquery,
                                &query_object.output_path,
                                query_object.tables_info.clone(),
                                query_object.table_to_csv.clone(),
                                false,
                            );

                            let temp_fields = query_object.get_mut_fields();
                            temp_fields.fill(fields.structs.clone(), fields.streams.clone());

                            Ok(GroupClause::Base(GroupBaseCondition::Exists(
                                ExistsCondition::Vec {
                                    vector_name: result.clone(),
                                    negated: *negated,
                                },
                            )))
                        }
                        ExistsCondition::Vec {
                            vector_name,
                            negated,
                        } => Ok(GroupClause::Base(GroupBaseCondition::Exists(
                            ExistsCondition::Vec {
                                vector_name: vector_name.clone(),
                                negated: *negated,
                            },
                        ))),
                    }
                }
                GroupBaseCondition::Boolean(boolean) => {
                    Ok(GroupClause::Base(GroupBaseCondition::Boolean(*boolean)))
                }
                GroupBaseCondition::In(in_condition) => {
                    match in_condition {
                        InCondition::Subquery {
                            field,
                            subquery,
                            negated,
                        } => {
                            //two cases:
                            //first, field is a subquery
                            if field.subquery.is_some() {
                                let in_subquery = field.subquery.clone().unwrap();

                                // Process the in subquery
                                let processed_in_subquery =
                                    manage_subqueries(&in_subquery, query_object)?;

                                // Process the subquery
                                let processed_subquery = manage_subqueries(subquery, query_object)?;

                                let tables_info = query_object.tables_info.clone();
                                let table_to_csv = query_object.table_to_csv.clone();

                                // Execute the in subquery
                                let (in_result, in_result_type, in_fields) = subquery_result(
                                    processed_in_subquery,
                                    &query_object.output_path,
                                    tables_info.clone(),
                                    table_to_csv.clone(),
                                    true,
                                );

                                let temp_fields = query_object.get_mut_fields();
                                temp_fields
                                    .fill(in_fields.structs.clone(), in_fields.streams.clone());

                                // Execute the subquery
                                let (result, result_type, fields) = subquery_result(
                                    processed_subquery,
                                    &query_object.output_path,
                                    tables_info,
                                    table_to_csv,
                                    false,
                                );

                                let temp_fields = query_object.get_mut_fields();
                                temp_fields.fill(fields.structs.clone(), fields.streams.clone());

                                Ok(GroupClause::Base(GroupBaseCondition::In(
                                    InCondition::Vec {
                                        field: ComplexField {
                                            subquery_vec: Some((in_result, in_result_type)),
                                            subquery: None,
                                            column_ref: None,
                                            literal: None,
                                            aggregate: None,
                                            nested_expr: None,
                                        },
                                        vector_name: result,
                                        vector_type: result_type,
                                        negated: *negated,
                                    },
                                )))
                            } else {
                                //second, field is a complex_expr, a column_ref or an aggregate
                                //Process complex_expr
                                let processed_field = process_complex_field(field, query_object)?;
                                // Process the subquery
                                let processed_subquery = manage_subqueries(subquery, query_object)?;

                                // Execute the subquery
                                let (result, result_type, fields) = subquery_result(
                                    processed_subquery,
                                    &query_object.output_path,
                                    query_object.tables_info.clone(),
                                    query_object.table_to_csv.clone(),
                                    false,
                                );

                                let temp_fields = query_object.get_mut_fields();
                                temp_fields.fill(fields.structs.clone(), fields.streams.clone());

                                Ok(GroupClause::Base(GroupBaseCondition::In(
                                    InCondition::Vec {
                                        field: processed_field,
                                        vector_name: result,
                                        vector_type: result_type,
                                        negated: *negated,
                                    },
                                )))
                            }
                        }
                        InCondition::Vec {
                            field,
                            vector_name,
                            vector_type,
                            negated,
                        } => {
                            // Process the field for subqueries
                            let processed_field = process_complex_field(field, query_object)?;

                            Ok(GroupClause::Base(GroupBaseCondition::In(
                                InCondition::Vec {
                                    field: processed_field,
                                    vector_name: vector_name.clone(),
                                    vector_type: vector_type.clone(),
                                    negated: *negated,
                                },
                            )))
                        }
                    }
                }
            }
        }
        GroupClause::Expression { left, op, right } => {
            // Recursively process both sides of the expression
            let processed_left = process_group_condition(left, query_object)?;
            let processed_right = process_group_condition(right, query_object)?;

            Ok(GroupClause::Expression {
                left: Box::new(processed_left),
                op: op.clone(),
                right: Box::new(processed_right),
            })
        }
    }
}

// Processes scan nodes, including subqueries and complex expressions
fn process_scan(input: &Arc<IrPlan>, query_object: &mut QueryObject) -> Arc<IrPlan> {
    //retrieve the output path
    let output_path = query_object.output_path.clone();
    // process all the subqueries in the nested joins
    let processed_input =
        manage_subqueries(input, query_object).expect("Failed to process nested join subqueries");

    let sub_fields = subquery_renoir(
        processed_input,
        &query_object.output_path,
        query_object.tables_info.clone(),
        query_object.table_to_csv.clone(),
    );

    let (stream_name, stream_info) = sub_fields.streams.first().unwrap();

    query_object
        .streams
        .insert(stream_name.clone(), stream_info.clone());
    query_object
        .tables_info
        .insert(stream_name.clone(), stream_info.final_struct.get(stream_info.final_struct.keys().last().unwrap()).unwrap().clone());
    query_object.table_to_struct_name.insert(
        stream_name.clone(),
        stream_info.final_struct.keys().last().unwrap().to_string(),
    );
    query_object.structs.insert(
        stream_info.final_struct.keys().last().unwrap().clone(),
        stream_info.final_struct.get(stream_info.final_struct.keys().last().unwrap()).unwrap().clone(),
    );

    let fields = query_object.get_mut_fields();
    fields.output_path = output_path;
    fields.fill(sub_fields.structs, sub_fields.streams.clone());

    Arc::new(IrPlan::Table {
        table_name: stream_name.to_string(),
    })
}
