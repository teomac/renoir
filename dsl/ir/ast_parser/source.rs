use super::error::IrParseError;
use super::{ir_ast_structure::*, IrParser};
use crate::dsl::ir::ast_parser::Rule;
use pest::iterators::Pair;
use std::sync::Arc;
pub struct SourceParser;

impl SourceParser {
    pub fn parse(pair: Pair<Rule>) -> Result<Arc<IrPlan>, IrParseError> {
        let has_join = pair.as_str().contains("join");


        let mut inner = pair.into_inner();

        // Skip 'scan' keyword if present
        if inner.peek().map_or(false, |p| p.as_str() == "from") {
            inner.next();
        }

        // Parse the initial scan
        let scan_expr = inner
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Missing source expression".to_string()))?;
        let mut current_plan = Arc::new(Self::parse_scan(scan_expr, has_join)?);

        // Process any joins
        while let Some(pair) = inner.next() {
            // Look for join type first
            let mut join_type = JoinType::Inner; // Default join type

            if pair.as_rule() == Rule::join_type {
                join_type = match pair.as_str() {
                    "inner" => JoinType::Inner,
                    "left" => JoinType::Left,
                    "outer" => JoinType::Outer,
                    _ => {
                        return Err(IrParseError::InvalidInput(format!(
                            "Invalid join type: {}",
                            pair.as_str()
                        )))
                    }
                };
                // Get next token which should be 'join'
                inner.next();
            } else if pair.as_str() != "join" {
                return Err(IrParseError::InvalidInput(format!(
                    "Expected join keyword, got {}",
                    pair.as_str()
                )));
            }

            // Parse the join scan
            let join_scan_expr = inner
                .next()
                .ok_or_else(|| IrParseError::InvalidInput("Missing join stream".to_string()))?;
            let right_plan = Arc::new(Self::parse_scan(join_scan_expr, has_join)?);

            // Expect and skip 'on' keyword
            if inner.next().map_or(true, |p| p.as_str() != "on") {
                return Err(IrParseError::InvalidInput("Missing ON in join clause".to_string()));
            }

            // Parse join condition
            let condition_pair = inner
                .next()
                .ok_or_else(|| IrParseError::InvalidInput("Missing join condition".to_string()))?;
            let conditions = JoinCondition::parse(condition_pair)?;

            // Create new join plan
            current_plan = Arc::new(IrPlan::Join {
                left: current_plan,
                right: right_plan,
                condition: conditions,
                join_type,
            });
        }

        Ok(current_plan)
    }

    fn parse_scan(pair: Pair<Rule>, has_join: bool) -> Result<IrPlan, IrParseError> {
        let mut inner = pair.into_inner();
        let table_name = inner
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Missing stream name".to_string()))?;

        let mut alias = None;
        let mut stream_input = None;

        while let Some(pair) = inner.next() {
            match pair.as_rule() {
                Rule::identifier => {
                    if alias.is_none() {
                        alias = Some(pair.as_str().to_string());
                    }
                }
                Rule::stream_input => {
                    stream_input = Some(pair.as_str().to_string());
                }
                _ => {} // Skip other tokens
            }
        }

        // Input source is required
        if stream_input.is_none() {
            return Err(IrParseError::InvalidInput(
                "Missing input source for stream".to_string(),
            ));
        }

        

        match table_name.as_rule() {
            Rule::identifier => {
                if alias.is_none() && has_join{
                    alias = Some(table_name.as_str().to_string().clone());
                }

                return Ok(IrPlan::Scan {
                    stream_name: stream_input.unwrap(),
                    alias,
                    input: IrPlan::Table { table_name: table_name.as_str().to_string() }.into(),
                });
            }
            Rule::subquery => {
                let subquery = IrParser::parse_subquery(table_name)?;
                return Ok(IrPlan::Scan {
                    stream_name: stream_input.unwrap(),
                    alias,
                    input: subquery,
                });
            }
            _ => {
                return Err(IrParseError::InvalidInput(
                    "Invalid input source for stream".to_string(),
                ));
            }

        }
    }

    fn parse_qualified_column(pair: Pair<Rule>) -> Result<ColumnRef, IrParseError> {
        if pair.as_rule() != Rule::qualified_column {
            return Err(IrParseError::InvalidInput(
                "Join condition must use qualified column references".to_string(),
            ));
        }

        let mut inner = pair.into_inner();
        let stream = inner
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Missing stream name".to_string()))?
            .as_str()
            .to_string();
        let field = inner
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Missing field name".to_string()))?
            .as_str()
            .to_string();

        Ok(ColumnRef {
            table: Some(stream),
            column: field,
        })
    }
}

impl JoinCondition {
    fn parse(pair: Pair<Rule>) -> Result<Vec<JoinCondition>, IrParseError> {
        let mut conditions = Vec::new();
        let mut pairs = pair.into_inner().peekable();

        while let Some(left_pair) = pairs.next() {
            let right_pair = pairs.next().ok_or_else(|| {
                IrParseError::InvalidInput("Missing right side of join condition".to_string())
            })?;

            conditions.push(JoinCondition {
                left_col: SourceParser::parse_qualified_column(left_pair)?,
                right_col: SourceParser::parse_qualified_column(right_pair)?,
            });

            // Skip the AND if present
            if pairs.peek().map_or(false, |p| p.as_str() == "AND") {
                pairs.next();
            }
        }

        Ok(conditions)
    }
}
