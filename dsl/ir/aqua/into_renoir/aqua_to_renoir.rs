use std::collections::HashMap;
use crate::dsl::ir::aqua::ast_parser::ast_structure::AquaAST;
use super::operator_chain::OperatorChain;

pub struct AquaToRenoir;

impl AquaToRenoir {
    pub fn convert(ast: &AquaAST, hash_map: &HashMap<String, String>) -> String {
        let mut parts = Vec::new();
        
        // FROM clause
        let mut from_str = match &ast.from.scan.alias {
            Some(alias) => format!("from {} as {} in input1", ast.from.scan.stream_name, alias),
            None => format!("from {} in input1", ast.from.scan.stream_name),
        };
        
        // Add JOIN if present
        if let Some(join) = &ast.from.join {
            from_str.push_str(&format!(" {}", 
                OperatorChain::process_join(join, hash_map)
            ));
        }
        parts.push(from_str);

        // WHERE clause (if present)
        if let Some(where_clause) = &ast.filter {
            parts.push(format!("where {}",
                OperatorChain::process_where_clause(where_clause, hash_map)
            ));
        }

        // SELECT clause
        parts.push(OperatorChain::process_select(&ast.select, hash_map));

        parts.join("\n")
    }
}