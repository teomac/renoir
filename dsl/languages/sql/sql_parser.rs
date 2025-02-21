use super::ast_parser::*;
use crate::dsl::languages::sql::into_ir::ir_query_gen::SqlToIr;

pub fn sql_to_ir(query_str: &str) -> String {
    println!("Input SQL query: {}", query_str);
    
    let sql_ast = SqlParser::parse_query(query_str).expect("Failed to parse query");
    //println!("SQL AST: {:?}", sql_ast);
    
    let ir_string = SqlToIr::convert(&sql_ast);
    println!("Generated Ir string:\n{}", ir_string);

    ir_string
}