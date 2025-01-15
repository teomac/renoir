pub mod ast_parser;
pub mod into_renoir;

use std::collections::HashMap;
pub use ast_parser::*;
pub use into_renoir::*;

pub fn query_aqua_to_renoir(query_str: &str, hash_map: &HashMap<String, String>) -> String {
    println!("Input Aqua query: {}", query_str);
    
    let ast = AquaParser::parse_query(query_str).expect("Failed to parse query");
    println!("Aqua AST: {:?}", ast);
    
    let renoir_string = AquaToRenoir::convert(&ast, hash_map);
    println!("Generated Renoir string:\n{}", renoir_string);

    renoir_string
}