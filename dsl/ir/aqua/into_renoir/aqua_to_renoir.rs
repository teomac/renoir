use crate::dsl::{ir::aqua::{ast_parser::ir_ast_structure::AquaAST, into_renoir::{
    r_condition::process_where_clause, 
    r_source::*}, 
r_sink::process_select_clauses
}, struct_object::object::QueryObject};

use super::r_group::process_group_by;

pub struct AquaToRenoir;

impl AquaToRenoir {
    pub fn convert(ast: &AquaAST, query_object: &mut QueryObject) -> String {

        println!("Aqua AST: {:#?}", ast);


        let mut final_string = String::new();

        let from_clause = &ast.from; 
        final_string.push_str(&format!(
            "{}",
            process_from_clause(&from_clause, query_object)
        ));

        if let Some(where_clause) = &ast.filter {
            final_string.push_str(&format!(
                ".filter(|x| {})",
                process_where_clause(&where_clause, &query_object)
            ));
        }

        if let Some(ref group_by) = ast.group_by {
            final_string.push_str(&format!(
                "{}",
                process_group_by(&group_by, query_object)
            ));
        }

        // Process all select clauses together
        final_string.push_str(&process_select_clauses(&ast.select, query_object));
        
        
    
        //println!("Final string: {}", final_string);
        final_string
    }

    
} 