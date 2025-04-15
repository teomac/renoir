pub mod ast_parser;
pub mod into_renoir;

pub use ast_parser::*;
pub use into_renoir::*;
use std::sync::Arc;

use crate::dsl::struct_object::object::QueryObject;

use self::r_distinct_order::process_distinct_order;

pub fn query_ir_to_ast(query_str: &str) -> Arc<IrPlan> {
    IrParser::parse_query(query_str).expect("Failed to parse query")
}

pub fn ir_ast_to_renoir(
    query_object: &mut QueryObject,
) -> Result<String, Box<dyn std::error::Error>> {
    let ir_ast = query_object.ir_ast.clone().unwrap();
    let result = IrToRenoir::convert(&ir_ast, query_object);
    process_distinct_order(result.as_ref().unwrap(), query_object);

    Ok(result.unwrap())
}
