pub mod df_builder;
pub mod df_parser;

use crate::dsl::ir::ast_parser::ir_ast_structure::IrPlan;
use std::sync::Arc;

pub fn dataframe_to_ir(query_str: &str) -> Result<Arc<IrPlan>, Box<dyn std::error::Error>> {
    println!("Input DataFrame query: {}", query_str);

    let ir_ast = df_parser::DataFrameParser::parse_query(query_str)?;
    println!("Generated Ir AST: {:?}", ir_ast);

    Ok(ir_ast)
}
