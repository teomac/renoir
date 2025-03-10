use crate::dsl::{
    ir::{
        ast_parser::ir_ast_structure::IrAST,
        into_renoir::{r_condition::process_filter_clause, 
            r_source::*, 
            r_sink::base::r_sink_base::process_projections,
            r_group::r_group_keys::process_group_by}
    },
    struct_object::object::QueryObject,
};

pub struct IrToRenoir;

impl IrToRenoir {
    pub fn convert(ast: &IrAST, query_object: &mut QueryObject) -> Result<(), Box<dyn std::error::Error>> {

        let mut result_vec = Vec::new();

        let from_clause = &ast.from;
        
        result_vec.push(process_from_clause(&from_clause, query_object)?);

        if let Some(where_clause) = &ast.filter {
           
                result_vec.push(process_filter_clause(&where_clause, query_object)?);
            
        }

        if let Some(ref group_by) = ast.group_by {
            //process group by and conditions. Inside this function, there will also be processing of select clauses
            result_vec.push(process_group_by(&group_by, query_object)?);
        } else {
            // Process all select clauses together
            result_vec.push(process_projections(&ast.select.select, query_object)?);
        }

        
        Ok(())
    }
}
