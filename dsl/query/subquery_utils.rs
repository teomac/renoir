use std::{io, sync::Arc};

use crate::dsl::{ir::{self, IrPlan, ProjectionColumn}, struct_object::object::QueryObject};

use super::subquery_csv;

pub fn manage_subqueries(ir_ast: &Arc<IrPlan>, output_path: &String, query_object: &QueryObject) -> io::Result<Arc<IrPlan>> {

    let mut ir_ast_cloned = ir_ast.clone();

    // iterate through the ir ast to find subqueries.
    // for now we only focus on subqueries in the project node.
    // the method will be called recursively to handle subqueries in subqueries.


    match *ir_ast {
        IrPlan::Project { input, columns, distinct } => {
            for col in columns.iter() {
                match col {
                    &ProjectionColumn::Subquery(subquery, alias) => {
                        // in this case we need to call the subquery_csv method to retrieve the result of the subquery
                        let result = subquery_csv(subquery, output_path, query_object.tables_info.clone(), query_object.table_to_csv.clone());

                        // we need to update the ir_ast_cloned with the result of the subquery
                    },
                    _ => (),
                }
            }
        },
        _ => (),
    }

    // return the updated ir_ast_cloned

    Ok(ir_ast_cloned)

}