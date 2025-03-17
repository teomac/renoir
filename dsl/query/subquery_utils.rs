use std::{io, sync::Arc};

use crate::dsl::{ir::{literal::LiteralParser, IrPlan, ProjectionColumn}, struct_object::object::QueryObject};
use crate::dsl::ir::ast_parser::ir_ast_structure::ComplexField;

use super::subquery_csv;

pub fn manage_subqueries(ir_ast: &Arc<IrPlan>, output_path: &String, query_object: &QueryObject) -> io::Result<Arc<IrPlan>> {

    match &**ir_ast {
        IrPlan::Project { input, columns, distinct } => {
            // First recursively process any subqueries in the input
            let processed_input = manage_subqueries(input, &output_path, query_object)?;
            
            // Process columns to find and replace subqueries
            let processed_columns = columns.iter().map(|col| {
                match col {
                    ProjectionColumn::Subquery(subquery, alias) => {
                        // Recursively process nested subqueries within this subquery
                        let processed_subquery = manage_subqueries(subquery, &output_path, query_object)
                            .expect("Failed to process nested subquery");
                            
                        // Execute the processed subquery to get its result
                        let mut result = subquery_csv(
                            processed_subquery,
                            &output_path,
                            query_object.tables_info.clone(),
                            query_object.table_to_csv.clone()
                        );
                        // Convert result to StringLiteral with the same alias
                        // Clean up the result string - remove quotes and whitespace/newlines
            result = result.trim().trim_matches('"').to_string();

                        ProjectionColumn::StringLiteral(result, alias.clone())
                    },
                     // Add handling for complex values that might contain subqueries
                     ProjectionColumn::ComplexValue(complex_field, alias) => {
                        match process_complex_field(complex_field, output_path, query_object) {
                            Ok(processed_field) => ProjectionColumn::ComplexValue(processed_field, alias.clone()),
                            Err(e) => panic!("Error processing complex field: {}", e)
                        }
                    },
                    // Preserve non-subquery columns as-is
                    _ => col.clone()
                }
            }).collect();

            // Return new Project node with processed input and columns
            Ok(Arc::new(IrPlan::Project {
                input: processed_input,
                columns: processed_columns,
                distinct: *distinct
            }))
        },
        // Recursively process other node types
        IrPlan::Filter { input, predicate } => {
            // Process input first
            let processed_input = manage_subqueries(input, &output_path, query_object)?;

            // Process predicate to find and replace subqueries
            //as for now, we only focus on subqueries that are in comparison expressions
            //NO expressions like IN or EXISTS for now







            Ok(Arc::new(IrPlan::Filter {
                input: processed_input,
                predicate: predicate.clone()
            }))
        },
        IrPlan::GroupBy { input, keys, group_condition } => {
            let processed_input = manage_subqueries(input, &output_path, query_object)?;
            Ok(Arc::new(IrPlan::GroupBy {
                input: processed_input,
                keys: keys.clone(),
                group_condition: group_condition.clone()
            }))
        },
        IrPlan::Join { left, right, condition, join_type } => {
            let processed_left = manage_subqueries(left, &output_path, query_object)?;
            let processed_right = manage_subqueries(right, &output_path, query_object)?;
            Ok(Arc::new(IrPlan::Join {
                left: processed_left,
                right: processed_right,
                condition: condition.clone(),
                join_type: join_type.clone()
            }))
        },
        IrPlan::OrderBy { input, items } => {
            let processed_input = manage_subqueries(input, &output_path, query_object)?;
            Ok(Arc::new(IrPlan::OrderBy {
                input: processed_input,
                items: items.clone()
            }))
        },
        IrPlan::Limit { input, limit, offset } => {
            let processed_input = manage_subqueries(input, &output_path, query_object)?;
            Ok(Arc::new(IrPlan::Limit {
                input: processed_input,
                limit: *limit,
                offset: *offset
            }))
        },
        // Base case - Scan nodes have no nested queries to process
        IrPlan::Scan { .. } => Ok(ir_ast.clone())
    }
}


fn process_complex_field(field: &ComplexField, output_path: &String, query_object: &QueryObject) -> io::Result<ComplexField> {
    match field {
        ComplexField {
            subquery: Some(subquery),
            ..
        } => {
            // Process the subquery itself first in case it contains nested subqueries
            let processed_subquery = manage_subqueries(subquery, output_path, query_object)?;
            
            // Execute the subquery to get result
            let mut result = subquery_csv(
                processed_subquery,
                output_path,
                query_object.tables_info.clone(),
                query_object.table_to_csv.clone()
            );

            // Clean up the result string - remove quotes and whitespace/newlines
            result = result.trim().trim_matches('"').to_string();

        // Convert the result string to appropriate IrLiteral
        let literal = LiteralParser::parse(&result)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;


            // Return new ComplexField with just the literal
            Ok(ComplexField {
                column_ref: None,
                literal: Some(literal),
                aggregate: None,
                nested_expr: None,
                subquery: None,
            })
        },
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
            })
        },
        // Other cases just return clone of original field since they can't contain subqueries
        _ => Ok(field.clone())
    }
}