use pest::Parser;
use pest_derive::Parser;
use std::sync::Arc;

use crate::dsl::ir::ast_parser::error::IrParseError;
use crate::dsl::ir::ast_parser::ir_ast_structure::*;

use super::df_builder::DataFrameASTBuilder;

#[derive(Parser)]
#[grammar = "dsl/languages/dataframe/df_grammar.pest"]
pub struct DataFrameParser;

impl DataFrameParser {
    pub fn parse_query(input: &str) -> Result<Arc<IrPlan>, Box<IrParseError>> {
        let pairs = Self::parse(Rule::query, input).map_err(|e| {
            Box::new(IrParseError::InvalidInput(format!(
                "Failed to parse query: {}",
                e
            )))
        })?;

        let ast = DataFrameASTBuilder::build_ast_from_pairs(pairs)?;

        Ok(ast)
    }
}
