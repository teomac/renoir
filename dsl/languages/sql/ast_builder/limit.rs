use super::{error::SqlParseError, sql_ast_structure::LimitClause};
use crate::dsl::languages::sql::ast_builder::Rule;
use pest::iterators::Pair;

pub struct LimitParser;

impl LimitParser {
    pub(crate) fn parse(pair: Pair<Rule>) -> Result<LimitClause, Box<SqlParseError>> {
        let mut inner = pair.into_inner();

        // Parse LIMIT clause
        let limit_clause = inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing LIMIT clause".to_string()))?;
        let limit = Self::parse_limit(limit_clause)?;

        // Parse optional OFFSET clause
        let offset = if let Some(offset_clause) = inner.next() {
            Some(Self::parse_offset(offset_clause)?)
        } else {
            None
        };

        Ok(LimitClause { limit, offset })
    }

    fn parse_limit(pair: Pair<Rule>) -> Result<i64, Box<SqlParseError>> {
        let mut inner = pair.into_inner();
        inner.next(); // Skip LIMIT keyword
        let number = inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing limit value".to_string()))?;

        Ok(number
            .as_str()
            .parse::<i64>()
            .map_err(|_| SqlParseError::InvalidInput("Invalid limit value".to_string()))?)
    }

    fn parse_offset(pair: Pair<Rule>) -> Result<i64, Box<SqlParseError>> {
        let mut inner = pair.into_inner();
        inner.next(); // Skip OFFSET keyword
        let number = inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing offset value".to_string()))?;

        Ok(number
            .as_str()
            .parse::<i64>()
            .map_err(|_| SqlParseError::InvalidInput("Invalid offset value".to_string()))?)
    }
}
