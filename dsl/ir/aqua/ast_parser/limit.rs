use pest::iterators::Pair;
use super::ir_ast_structure::*;
use super::error::AquaParseError;
use crate::dsl::ir::aqua::ast_parser::Rule;

pub struct LimitParser;

impl LimitParser {
    pub fn parse(pair: Pair<Rule>) -> Result<LimitClause, AquaParseError> {
        let mut inner = pair.into_inner();
        
        // Parse LIMIT clause
        let limit_clause = inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing LIMIT clause".to_string()))?;
        let limit = Self::parse_limit(limit_clause)?;

        // Parse optional OFFSET clause
        let offset = if let Some(offset_clause) = inner.next() {
            Some(Self::parse_offset(offset_clause)?)
        } else {
            None
        };

        Ok(LimitClause {
            limit,
            offset,
        })
    }

    fn parse_limit(pair: Pair<Rule>) -> Result<i64, AquaParseError> {
        let mut inner = pair.into_inner();
        inner.next(); // Skip limit keyword
        let number = inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing limit value".to_string()))?;
        
        number.as_str().parse::<i64>()
            .map_err(|_| AquaParseError::InvalidInput("Invalid limit value".to_string()))
    }

    fn parse_offset(pair: Pair<Rule>) -> Result<i64, AquaParseError> {
        let mut inner = pair.into_inner();
        inner.next(); // Skip offset keyword
        let number = inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing offset value".to_string()))?;
        
        number.as_str().parse::<i64>()
            .map_err(|_| AquaParseError::InvalidInput("Invalid offset value".to_string()))
    }
}