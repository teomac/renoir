use crate::dsl::{
    ir::{
        ast_parser::ir_ast_structure::IrAST,
        into_renoir::{r_condition::process_filter_clause, r_group::r_group_keys::process_group_by, r_sink::base::r_sink_base::process_projections, r_source::*, r_limit::*, r_order::*}, ir_ast_structure::Operation
    },
    struct_object::object::QueryObject,
};

pub struct IrToRenoir;

impl IrToRenoir {
    pub fn convert(ast: &IrAST, query_object: &mut QueryObject) -> Result<(), Box<dyn std::error::Error>> {

        let mut result_vec = Vec::new();
        
        for operation in ast.operations.iter() {
                if operation.from.is_some()  {
                    result_vec.push(process_from_clause(&operation.from.clone().unwrap(), query_object)?);
                } else if  operation.select.is_some() {
                    result_vec.push(process_projections(&operation.select.clone().unwrap().select, query_object)?);
                } else if operation.filter.is_some() {
                    result_vec.push(process_filter_clause(&operation.filter.clone().unwrap(), query_object)?);
                } else if operation.group_by.is_some() {
                    result_vec.push(process_group_by(&operation.group_by.clone().unwrap(), query_object)?);
                } else if operation.order_by.is_some() {
                    //todo
                } else if operation.limit.is_some() {
                    //todo
                } else {
                    panic!("Unknown operation in IR AST");
                }
                
            
        }
        
        Ok(())
    }
}
