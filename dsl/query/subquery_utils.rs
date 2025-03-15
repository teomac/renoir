use std::{io, sync::Arc};

use crate::dsl::{ir::{self, IrPlan, ProjectionColumn}, struct_object::object::QueryObject};

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
                        let result = subquery_csv(
                            processed_subquery,
                            &output_path,
                            query_object.tables_info.clone(),
                            query_object.table_to_csv.clone()
                        );
                        // Convert result to StringLiteral with the same alias
                        ProjectionColumn::StringLiteral(result, alias.clone())
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
            let processed_input = manage_subqueries(input, &output_path, query_object)?;
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