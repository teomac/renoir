use super::ir_ast_structure::*;
use super::error::AquaParseError;
use crate::dsl::ir::aqua::ast_parser::Rule;
use pest::iterators::Pair;

pub struct SourceParser;

impl SourceParser {
    pub fn parse(pair: Pair<Rule>) -> Result<FromClause, AquaParseError> {
        let mut inner = pair.into_inner();

        // Skip 'from' if present
        if inner.peek().map_or(false, |p| p.as_str() == "from") {
            inner.next();
        }

        let scan_expr = inner
            .next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing source expression".to_string()))?;
        let scan = Self::parse_scan(scan_expr)?;

        let mut joins = Vec::new();

        while let Some(token) = inner.next() {
            if token.as_str() == "join" {
                // Parse the join scan
                let join_scan_expr = inner.next().ok_or_else(|| {
                    AquaParseError::InvalidInput("Missing join stream".to_string())
                })?;
                let join_scan = Self::parse_scan(join_scan_expr)?;

                // Expect and skip 'on' keyword
                if inner.next().map_or(true, |p| p.as_str() != "on") {
                    return Err(AquaParseError::InvalidInput(
                        "Missing ON in join clause".to_string(),
                    ));
                }

                // Parse join condition
                let condition_pair = inner.next().ok_or_else(|| {
                    AquaParseError::InvalidInput("Missing join condition".to_string())
                })?;
                let condition = JoinCondition::parse(condition_pair)?;

                joins.push(JoinClause {
                    scan: join_scan,
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

    fn parse_scan(pair: Pair<Rule>) -> Result<ScanClause, AquaParseError> {
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
                .ok_or_else(|| AquaParseError::InvalidInput("Missing stream name".to_string()))?,
            alias,
            input_source: input_source
                .ok_or_else(|| AquaParseError::InvalidInput("Missing input source".to_string()))?,
        })
    }

    fn parse_qualified_column(pair: Pair<Rule>) -> Result<ColumnRef, AquaParseError> {
        if pair.as_rule() != Rule::qualified_column {
            return Err(AquaParseError::InvalidInput(
                "Join condition must use qualified column references".to_string(),
            ));
        }

        let mut inner = pair.into_inner();
        let stream = inner
            .next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing stream name".to_string()))?
            .as_str()
            .to_string();
        let field = inner
            .next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing field name".to_string()))?
            .as_str()
            .to_string();

        Ok(ColumnRef {
            table: Some(stream),
            column: field,
        })
    }
}

impl JoinCondition {
    fn parse(pair: Pair<Rule>) -> Result<Self, AquaParseError> {
        let mut conditions = Vec::new();
        let mut pairs = pair.into_inner().peekable();
        
        while let Some(left_pair) = pairs.next() {
            let right_pair = pairs.next()
                .ok_or_else(|| AquaParseError::InvalidInput("Missing right side of join condition".to_string()))?;

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
