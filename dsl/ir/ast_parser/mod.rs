pub mod builder;
pub mod condition;
pub mod error;
pub mod group;
pub mod ir_ast_structure;
pub mod limit;
pub mod literal;
pub mod order;
pub mod projection;
pub mod source;

pub use ir_ast_structure::*;

use crate::dsl::ir::ast_parser::builder::IrASTBuilder;
use crate::dsl::ir::ast_parser::error::IrParseError;
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "dsl/ir/ir_grammar.pest"]
pub struct IrParser;

impl IrParser {
    pub fn parse_query(input: &str) -> Result<IrAST, IrParseError> {
        let pairs = Self::parse(Rule::query, input).map_err(|e| IrParseError::PestError(e))?;

        let ast = IrASTBuilder::build_ast_from_pairs(pairs)?;
        Ok(ast)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_query() {
        let input = "from stream1 in input1 select field1";
        assert!(IrParser::parse_query(input).is_ok());
    }

    #[test]
    fn test_query_with_condition() {
        let input = "from stream1 in input1 where field1 > 10 select field2";
        assert!(IrParser::parse_query(input).is_ok());
    }

    #[test]
    fn test_query_with_join() {
        let input = "from stream1 in input1 join stream2 in input2 on stream1.id == stream2.id select stream1.value";
        assert!(IrParser::parse_query(input).is_ok());
    }

    #[test]
    fn test_query_with_aggregate() {
        let input = "from stream1 in input1 select max(value)";
        assert!(IrParser::parse_query(input).is_ok());
    }

    #[test]
    fn test_invalid_query() {
        let input = "invalid query syntax";
        assert!(IrParser::parse_query(input).is_err());
    }
}
