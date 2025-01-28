pub mod ast_parser;
pub mod into_renoir;

pub use ast_parser::*;
pub use into_renoir::*;

use crate::dsl::struct_object::object::QueryObject;

pub fn query_aqua_to_ast(query_str: &str) -> AquaAST {
    println!("Input Aqua query: {}", query_str);
    
    let ast = AquaParser::parse_query(query_str).expect("Failed to parse query");
    println!("Aqua AST: {:?}", ast);

    ast
}

pub fn aqua_ast_to_renoir(ast: &AquaAST, query_object: &mut QueryObject) -> String {
    let renoir_string = AquaToRenoir::convert(ast, query_object);
    println!("Generated Renoir string:\n{}", renoir_string);

    renoir_string
}
