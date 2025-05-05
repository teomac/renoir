pub(crate) mod builder;
pub(crate) mod condition;
pub(crate) mod error;
pub(crate) mod group;
pub mod ir_ast_structure;
pub(crate) mod limit;
pub(crate) mod literal;
pub(crate) mod order;
pub(crate) mod projection;
pub(crate) mod source;


pub use ir_ast_structure::*;
use pest::iterators::Pair;

use crate::dsl::ir::ast_builder::builder::IrASTBuilder;
use crate::dsl::ir::ast_builder::error::IrParseError;
use pest::Parser;
use pest_derive::Parser;

use std::sync::Arc;

#[derive(Parser)]
#[grammar = "dsl/ir/ir_grammar.pest"]
pub struct IrParser;

impl IrParser {
    /// Parses an IR string into an IR AST.
    pub(crate) fn parse_query(input: &str) -> Result<Arc<IrPlan>, Box<IrParseError>> {
        let pairs = Self::parse(Rule::query, input).map_err(|e| Box::new(IrParseError::from(e)))?;

        let ast = IrASTBuilder::build_ast_from_pairs(pairs)?;

        Ok(ast)
    }

    /// Parses a subquery expression into an IR AST.
    pub(crate) fn parse_subquery(pair: Pair<Rule>) -> Result<Arc<IrPlan>, Box<IrParseError>> {
        if pair.as_rule() != Rule::subquery {
            return Err(Box::new(IrParseError::InvalidInput(format!(
                "Expected subquery expression, got {:?}",
                pair.as_rule()
            ))));
        }

        let subquery_text = pair.as_str();

        // Remove the outer parentheses
        let inner_ir = if subquery_text.starts_with("(") && subquery_text.ends_with(")") {
            &subquery_text[1..subquery_text.len() - 1]
        } else {
            subquery_text
        };

        IrParser::parse_query(inner_ir)
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
