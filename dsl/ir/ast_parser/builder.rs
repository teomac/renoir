use super::ir_ast_structure::*;
use super::error::IrParseError;
use super::group::GroupParser;
use super::limit::LimitParser;
use super::order::OrderParser;
use super::{condition::ConditionParser, sink::SinkParser, source::SourceParser};
use crate::dsl::ir::ast_parser::Rule;
use pest::iterators::Pairs;

pub struct IrASTBuilder;

impl IrASTBuilder {
    pub fn build_ast_from_pairs(pairs: Pairs<Rule>) -> Result<IrAST, IrParseError> {
        let mut from = None;
        let mut select = Vec::new();
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
                                select = SinkParser::parse(clause)?;
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
            select,
            filter,
            group_by,
            order_by,
            limit,
        };

        Ok(ast)
    }

    pub fn validate_ast(_ast: &IrAST) -> Result<(), IrParseError> {

        //Validate the WHERE clause
        // column types must be coherent with the condition.

        //validate HAVING clause
        // column types must be coherent with the condition.

        //validate the SELECT clause
        // column types must be coherent with the operation. 
        //if we have a GROUP BY, the single columns in the SELECT clause must be in the GROUP BY clause.
       
        Ok(())
    }
}
