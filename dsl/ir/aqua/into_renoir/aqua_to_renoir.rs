use crate::dsl::{ir::aqua::{ast_parser::ast_structure::AquaAST, into_renoir::{r_condition::process_where_clause, r_source::*}, r_sink::process_select_clauses, ColumnRef}, struct_object::object::QueryObject};

use super::r_utils::convert_column_ref;

pub struct AquaToRenoir;

impl AquaToRenoir {
    pub fn convert(ast: &AquaAST, query_object: &mut QueryObject) -> String {


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
                ".group_by(|x| ({}))",
                process_group_by_keys(&group_by.columns, query_object)
            ));

            // Add HAVING clause if present
            //if let Some(ref having) = group_by.group_condition {
                //TODO: Implement HAVING clause
            //}

            final_string.push_str(".drop_key()");
        }

        // Process all select clauses together
        final_string.push_str(&process_select_clauses(&ast.select, query_object));
        
        
    
        //println!("Final string: {}", final_string);
        final_string
    }

    
} 

fn process_group_by_keys(columns: &Vec<ColumnRef>, query_object: &QueryObject) -> String {
    columns.iter()
        .map(|col| convert_column_ref(col, query_object))
        .collect::<Vec<_>>()
        .join(", ")
}