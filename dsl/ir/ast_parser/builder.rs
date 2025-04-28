use super::error::IrParseError;
use super::group::GroupParser;
use super::ir_ast_structure::*;
use super::limit::LimitParser;
use super::order::OrderParser;
use super::{condition::ConditionParser, projection::ProjectionParser, source::SourceParser};
use crate::dsl::ir::ast_parser::Rule;
use pest::iterators::Pairs;
use std::sync::Arc;

pub struct IrASTBuilder;

impl IrASTBuilder {
    pub(crate) fn build_ast_from_pairs(pairs: Pairs<Rule>) -> Result<Arc<IrPlan>, Box<IrParseError>> {
        let mut current_plan: Option<Arc<IrPlan>> = None;

        // Process each clause in the query
        for pair in pairs {
            match pair.as_rule() {
                Rule::query => {
                    for clause in pair.into_inner() {
                        match clause.as_rule() {
                            Rule::scan_clause => {
                                // Parse scan and joins from scan clause
                                let scan_plan = SourceParser::parse(clause)?;
                                current_plan = Some(scan_plan);
                            }
                            Rule::filter_clause => {
                                // Build filter on top of current plan
                                let filter_predicate = ConditionParser::parse(clause)?;
                                if let Some(input) = current_plan {
                                    current_plan =
                                        Some(Arc::new(IrPlan::filter(input, filter_predicate)));
                                } else {
                                    return Err(Box::new(IrParseError::InvalidInput(
                                        "Filter clause before scan clause".to_string(),
                                    )));
                                }
                            }
                            Rule::group_clause => {
                                // Build group by on top of current plan
                                let group = GroupParser::parse(clause)?;
                                if let Some(input) = current_plan {
                                    current_plan =
                                        Some(Arc::new(IrPlan::group_by(input, group.0, group.1)));
                                } else {
                                    return Err(Box::new(IrParseError::InvalidInput(
                                        "Group clause before scan clause".to_string(),
                                    )));
                                }
                            }
                            Rule::projection_clause => {
                                // Build projection on top of current plan
                                let project = ProjectionParser::parse(clause)?;
                                if let Some(input) = current_plan {
                                    current_plan = Some(Arc::new(IrPlan::project(
                                        input, project.0, project.1,
                                    )));
                                } else {
                                    return Err(Box::new(IrParseError::InvalidInput(
                                        "Projection clause before scan clause".to_string(),
                                    )));
                                }
                            }
                            Rule::order_clause => {
                                // Build order by on top of current plan
                                let order = OrderParser::parse(clause)?;
                                if let Some(input) = current_plan {
                                    current_plan = Some(Arc::new(IrPlan::order_by(input, order)));
                                } else {
                                    return Err(Box::new(IrParseError::InvalidInput(
                                        "Order clause before scan clause".to_string(),
                                    )));
                                }
                            }
                            Rule::limit_expr => {
                                // Build limit on top of current plan
                                let limit = LimitParser::parse(clause)?;
                                if let Some(input) = current_plan {
                                    current_plan =
                                        Some(Arc::new(IrPlan::limit(input, limit.0, limit.1)));
                                } else {
                                    return Err(Box::new(IrParseError::InvalidInput(
                                        "Limit clause before scan clause".to_string(),
                                    )));
                                }
                            }
                            Rule::EOI => {}
                            _ => {
                                return Err(Box::new(IrParseError::InvalidInput(format!(
                                    "Unexpected clause: {:?}",
                                    clause.as_rule()
                                ))))
                            }
                        }
                    }
                }
                _ => {
                    return Err(Box::new(IrParseError::InvalidInput(
                        "Expected query".to_string(),
                    )))
                }
            }
        }

        // Ensure we built a complete plan
        Ok(current_plan.ok_or_else(|| IrParseError::InvalidInput("Empty query".to_string()))?)
    }
}
