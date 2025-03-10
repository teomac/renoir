use super::error::IrParseError;
use super::group::GroupParser;
use super::ir_ast_structure::*;
use super::limit::LimitParser;
use super::order::OrderParser;
use super::{condition::ConditionParser, sink::SinkParser, source::SourceParser};
use crate::dsl::ir::ast_parser::Rule;
use pest::iterators::Pairs;

pub struct IrASTBuilder;

impl IrASTBuilder {
    pub fn build_ast_from_pairs(pairs: Pairs<Rule>) -> Result<IrAST, IrParseError> {
        let mut from = None;
        let mut select = None;
        let mut filter = None;
        let mut group_by = None;
        let mut order_by = None;
        let mut limit = None;

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
                                select = Some(SinkParser::parse(clause)?);
                            }
                            Rule::where_clause => {
                                filter = Some(ConditionParser::parse(clause)?);
                            }
                            Rule::group_clause => {
                                group_by = Some(GroupParser::parse(clause)?);
                            }
                            Rule::order_clause => {
                                order_by = Some(OrderParser::parse(clause)?);
                            }
                            Rule::limit_expr => {
                                limit = Some(LimitParser::parse(clause)?);
                            }
                            Rule::EOI => {}
                            _ => {
                                return Err(IrParseError::InvalidInput(format!(
                                    "Unexpected clause: {:?}",
                                    clause.as_rule()
                                )))
                            }
                        }
                    }
                }
                _ => return Err(IrParseError::InvalidInput("Expected query".to_string())),
            }
        }

        // Create and validate the AST
        let ast = IrAST {
            from: from
                .ok_or_else(|| IrParseError::InvalidInput("Missing FROM clause".to_string()))?,
            select: select
                .ok_or_else(|| IrParseError::InvalidInput("Missing SELECT clause".to_string()))?,
            filter,
            group_by,
            order_by,
            limit,
        };

        Ok(ast)
    }
}
