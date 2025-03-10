use super::error::IrParseError;
use super::group::GroupParser;
use super::ir_ast_structure::*;
use super::limit::LimitParser;
use super::order::OrderParser;
use super::{condition::ConditionParser, sink::SinkParser, source::SourceParser};
use crate::dsl::ir::ast_parser::Rule;
use crate::dsl::languages::sql::ast_parser::from;
use pest::iterators::Pairs;

pub struct IrASTBuilder;

impl IrASTBuilder {
    pub fn build_ast_from_pairs(pairs: Pairs<Rule>) -> Result<IrAST, IrParseError> {
        let mut ast: IrAST = IrAST {
            operations: Vec::new(),
        };

        // Process each clause in the query
        for pair in pairs {
            match pair.as_rule() {
                Rule::query => {
                    for clause in pair.into_inner() {
                        match clause.as_rule() {
                            Rule::from_clause => {
                                let from_clause = Some(SourceParser::parse(clause)?);
                                let mut op = Operation::new();
                                op.from = from_clause;
                                ast.operations.push(op);
                            }
                            Rule::select_clause => {
                                let select = Some(SinkParser::parse(clause)?);
                                let mut op = Operation::new();
                                op.select = select;
                                ast.operations.push(op);
                            }
                            Rule::where_clause => {
                                let filter = Some(ConditionParser::parse(clause)?);
                                let mut op = Operation::new();
                                op.filter = filter;
                                ast.operations.push(op);
                            }
                            Rule::group_clause => {
                                let group_by = Some(GroupParser::parse(clause)?);
                                let mut op = Operation::new();
                                op.group_by = group_by;
                                ast.operations.push(op);
                            }
                            Rule::order_clause => {
                                let order_by = Some(OrderParser::parse(clause)?);
                                let mut op = Operation::new();
                                op.order_by = order_by;
                                ast.operations.push(op);
                            }
                            Rule::limit_expr => {
                                let limit = Some(LimitParser::parse(clause)?);
                                let mut op = Operation::new();
                                op.limit = limit;
                                ast.operations.push(op);
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

        Ok(ast)
    }
}
