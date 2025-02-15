use super::ir_ast_structure::*;
use super::error::AquaParseError;
use super::group::GroupParser;
use super::{condition::ConditionParser, sink::SinkParser, source::SourceParser};
use crate::dsl::ir::aqua::ast_parser::Rule;
use pest::iterators::Pairs;

pub struct AquaASTBuilder;

impl AquaASTBuilder {
    pub fn build_ast_from_pairs(pairs: Pairs<Rule>) -> Result<AquaAST, AquaParseError> {
        let mut from = None;
        let mut select = Vec::new();
        let mut filter = None;
        let mut group_by = None;

        // Process each clause in the query
        for pair in pairs {
            match pair.as_rule() {
                Rule::query => {
                    for clause in pair.into_inner() {
                        match clause.as_rule() {
                            Rule::from_clause => {
                                from = Some(SourceParser::parse(clause)?);
                            }
                            Rule::select_clause => {
                                select = SinkParser::parse(clause)?;
                            }
                            Rule::where_clause => {
                                filter = Some(ConditionParser::parse(clause)?);
                            }
                            Rule::group_clause => {
                                group_by = Some(GroupParser::parse(clause)?);
                            }
                            Rule::EOI => {}
                            _ => {
                                return Err(AquaParseError::InvalidInput(format!(
                                    "Unexpected clause: {:?}",
                                    clause.as_rule()
                                )))
                            }
                        }
                    }
                }
                _ => return Err(AquaParseError::InvalidInput("Expected query".to_string())),
            }
        }

        // Create and validate the AST
        let ast = AquaAST {
            from: from
                .ok_or_else(|| AquaParseError::InvalidInput("Missing FROM clause".to_string()))?,
            select,
            filter,
            group_by,
        };

        Ok(ast)
    }

    pub fn validate_ast(ast: &AquaAST) -> Result<(), AquaParseError> {

        //Validate the WHERE clause
        // column types must be coherent with the condition.

        //validate HAVING clause
        // column types must be coherent with the condition.

        //validate the SELECT clause
        // column types must be coherent with the operation.
       
        Ok(())
    }
}
