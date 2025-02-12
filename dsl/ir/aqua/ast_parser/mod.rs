pub mod builder;
pub mod error;
pub mod condition;
pub mod sink;
pub mod source;
pub mod literal;
pub mod ir_ast_structure;
pub mod group;

pub use ir_ast_structure::{
    AquaAST, 
    Condition, 
    FromClause, 
    SelectClause, 
    WhereClause, 
    ComparisonOp,
    ColumnRef,
    AggregateFunction,
    AggregateType,
    AquaLiteral,
    BinaryOp,
};

use pest::Parser;
use pest_derive::Parser;
use crate::dsl::ir::aqua::ast_parser::error::AquaParseError;
use crate::dsl::ir::aqua::ast_parser::builder::AquaASTBuilder;


#[derive(Parser)]
#[grammar = "dsl/ir/aqua/ir_grammar.pest"] 
pub struct AquaParser;

impl AquaParser {
    pub fn parse_query(input: &str) -> Result<AquaAST, AquaParseError> {
        let pairs = Self::parse(Rule::query, input)
            .map_err(|e| AquaParseError::PestError(e))?;
        
        let ast = AquaASTBuilder::build_ast_from_pairs(pairs)?;
        //AquaASTBuilder::validate_ast(&ast)?;
        
        Ok(ast)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_query() {
        let input = "from stream1 in input1 select field1";
        assert!(AquaParser::parse_query(input).is_ok());
    }

    #[test]
    fn test_query_with_condition() {
        let input = "from stream1 in input1 where field1 > 10 select field2";
        assert!(AquaParser::parse_query(input).is_ok());
    }

    #[test]
    fn test_query_with_join() {
        let input = "from stream1 in input1 join stream2 in input2 on stream1.id == stream2.id select stream1.value";
        assert!(AquaParser::parse_query(input).is_ok());
    }

    #[test]
    fn test_query_with_aggregate() {
        let input = "from stream1 in input1 select max(value)";
        assert!(AquaParser::parse_query(input).is_ok());
    }

    #[test]
    fn test_invalid_query() {
        let input = "invalid query syntax";
        assert!(AquaParser::parse_query(input).is_err());
    }
}