pub mod ast_parser;
pub mod into_renoir;

pub use ast_parser::*;
pub use into_renoir::*;

use crate::dsl::struct_object::object::QueryObject;

pub fn query_ir_to_ast(query_str: &str) -> IrAST {
    //println!("Input Ir query: {}", query_str);

    let ast = IrParser::parse_query(query_str).expect("Failed to parse query");
    //println!("Ir AST: {:?}", ast);

    ast
}

pub fn ir_ast_to_renoir(query_object: &mut QueryObject) -> Result<(), Box<dyn std::error::Error>> {
    let ir_ast = query_object.ir_ast.clone().unwrap();
    let result = IrToRenoir::convert(&ir_ast, query_object);

    result
}
