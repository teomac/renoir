pub mod ast_builder;
pub(crate) mod into_renoir;

pub use ast_builder::*;
pub use into_renoir::*;
use std::sync::Arc;

use self::r_distinct_order::process_distinct_order;
use crate::dsl::struct_object::object::QueryObject;

/// Converts an IR query string into an IR AST.
pub(crate) fn query_ir_to_ast(query_str: &str) -> Arc<IrPlan> {
    IrParser::parse_query(query_str).expect("Failed to parse query")
}

/// Converts an IR AST into Renoir code.
pub(crate) fn ir_ast_to_renoir(query_object: &mut QueryObject) {
    let ir_ast = query_object.ir_ast.clone().unwrap();
    let result = IrToRenoir::convert(&ir_ast, query_object);
    if result.is_err() {
        panic!("Error converting IR AST to Renoir");
    }
    process_distinct_order(result.as_ref().unwrap(), query_object);
}
