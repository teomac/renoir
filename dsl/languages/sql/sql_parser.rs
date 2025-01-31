use super::ast_parser::*;
use crate::dsl::languages::sql::into_aqua::aqua_query_gen::SqlToAqua;

pub fn sql_to_aqua(query_str: &str) -> String {
    //println!("Input SQL query: {}", query_str);
    
    let sql_ast = SqlParser::parse_query(query_str).expect("Failed to parse query");
    //println!("SQL AST: {:?}", sql_ast);
    
    let aqua_string = SqlToAqua::convert(&sql_ast);
    //println!("Generated Aqua string:\n{}", aqua_string);

    aqua_string
}