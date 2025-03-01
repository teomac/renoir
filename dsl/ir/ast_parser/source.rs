use super::error::IrParseError;
use super::ir_ast_structure::*;
use crate::dsl::ir::ast_parser::Rule;
use pest::iterators::Pair;

pub struct SourceParser;

impl SourceParser {
    pub fn parse(pair: Pair<Rule>) -> Result<FromClause, IrParseError> {
        let mut inner = pair.into_inner();

        // Skip 'from' if present
        if inner.peek().map_or(false, |p| p.as_str() == "from") {
            inner.next();
        }

        let scan_expr = inner
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Missing source expression".to_string()))?;
        let scan = Self::parse_scan(scan_expr)?;

        let mut joins = Vec::new();

        while inner.peek().is_some() {
            // Check if the next token is a join_type or "join"
            let mut join_type = JoinType::Inner; // Default join type

            // Look for join type first
            if let Some(token) = inner.peek() {
                if token.as_rule() == Rule::join_type {
                    let join_type_token = inner.next().unwrap();
                    join_type = match join_type_token.as_str() {
                        "inner" => JoinType::Inner,
                        "left" => JoinType::Left,
                        "outer" => JoinType::Outer,
                        _ => {
                            return Err(IrParseError::InvalidInput(format!(
                                "Invalid join type: {}",
                                join_type_token.as_str()
                            )))
                        }
                    };
                }
            }

            // Now look for "join" keyword
            if inner.peek().map_or(false, |p| p.as_str() == "join") {
                // Consume 'join' token
                inner.next();

                // Parse the join scan
                let join_scan_expr = inner
                    .next()
                    .ok_or_else(|| IrParseError::InvalidInput("Missing join stream".to_string()))?;
                let join_scan = Self::parse_scan(join_scan_expr)?;

                // Expect and skip 'on' keyword
                if inner.next().map_or(true, |p| p.as_str() != "on") {
                    return Err(IrParseError::InvalidInput(
                        "Missing ON in join clause".to_string(),
                    ));
                }

                // Parse join condition
                let condition_pair = inner.next().ok_or_else(|| {
                    IrParseError::InvalidInput("Missing join condition".to_string())
                })?;
                let condition = JoinCondition::parse(condition_pair)?;

                joins.push(JoinClause {
                    join_type,
                    join_scan,
                    condition,
                });
            }
        }
        if joins.is_empty() {
            Ok(FromClause { scan, joins: None })
        } else {
            Ok(FromClause {
                scan,
                joins: Some(joins),
            })
        }
    }

    fn parse_scan(pair: Pair<Rule>) -> Result<ScanClause, IrParseError> {
        let mut inner = pair.into_inner();
        let mut stream_name = None;
        let mut alias = None;
        let mut input_source = None;

        while let Some(pair) = inner.next() {
            match pair.as_rule() {
                Rule::identifier => {
                    if stream_name.is_none() {
                        stream_name = Some(pair.as_str().to_string());
                    } else {
                        alias = Some(pair.as_str().to_string());
                    }
                }
                Rule::stream_input => {
                    input_source = Some(pair.as_str().to_string());
                }
                _ => {} // Skip other tokens
            }
        }

        Ok(ScanClause {
            stream_name: stream_name
                .ok_or_else(|| IrParseError::InvalidInput("Missing stream name".to_string()))?,
            alias,
            input_source: input_source
                .ok_or_else(|| IrParseError::InvalidInput("Missing input source".to_string()))?,
        })
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
    fn parse(pair: Pair<Rule>) -> Result<Self, IrParseError> {
        let mut conditions = Vec::new();
        let mut pairs = pair.into_inner().peekable();

        while let Some(left_pair) = pairs.next() {
            let right_pair = pairs.next().ok_or_else(|| {
                IrParseError::InvalidInput("Missing right side of join condition".to_string())
            })?;

            conditions.push(JoinPair {
                left_col: SourceParser::parse_qualified_column(left_pair)?,
                right_col: SourceParser::parse_qualified_column(right_pair)?,
            });

            // Skip the AND if present
            if pairs.peek().map_or(false, |p| p.as_str() == "AND") {
                pairs.next();
            }
        }

        Ok(JoinCondition { conditions })
    }
}
