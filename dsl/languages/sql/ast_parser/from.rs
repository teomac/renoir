use super::error::SqlParseError;
use super::sql_ast_structure::*;
use crate::dsl::languages::sql::ast_parser::Rule;
use pest::iterators::Pair;

pub struct FromParser;

impl FromParser {
    pub fn parse(pair: Pair<Rule>) -> Result<FromClause, SqlParseError> {
        let mut inner = pair.into_inner();
        inner.next(); // Skip FROM keyword

        let scan_expr = inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing scan expression".to_string()))?;
        let scan = Self::parse_scan(scan_expr)?;

        let mut joins = Vec::new();

        while let Some(join_expr) = inner.next() {
            if join_expr.as_rule() == Rule::join_expr {
                joins.push(Self::parse_join(join_expr)?);
            }
        }

        Ok(FromClause {
            scan,
            joins: Some(joins),
        })
    }

    fn parse_scan(pair: Pair<Rule>) -> Result<ScanClause, SqlParseError> {
        let mut inner = pair.into_inner();

        let variable = inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing table name".to_string()))?
            .as_str()
            .to_string();

        let mut alias = None;
        while let Some(next_token) = inner.next() {
            match next_token.as_rule() {
                Rule::as_keyword => {
                    if let Some(alias_token) = inner.next() {
                        alias = Some(alias_token.as_str().to_string());
                    }
                }
                Rule::variable => {
                    alias = Some(next_token.as_str().to_string());
                }
                _ => {}
            }
        }

        Ok(ScanClause { variable, alias })
    }

    fn parse_join(pair: Pair<Rule>) -> Result<JoinClause, SqlParseError> {
        let mut inner = pair.into_inner();

        // Default join type
        let mut join_type = JoinType::Inner;

        let first = inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing JOIN data".to_string()))?;

        // Check if the first token is a join_kind
        if first.as_rule() == Rule::join_kind {
            // Parse the join type from the join_kind rule
            let kind_str = first.as_str().to_uppercase();

            if kind_str.contains("INNER") {
                join_type = JoinType::Inner;
            } else if kind_str.contains("LEFT") {
                join_type = JoinType::Left;
            } else if kind_str == "OUTER" {
                join_type = JoinType::Outer;
            } else {
                return Err(SqlParseError::InvalidInput(format!(
                    "Unknown join type: {}",
                    kind_str
                )));
            }

            // Get the JOIN keyword
            let join_keyword = inner
                .next()
                .ok_or_else(|| SqlParseError::InvalidInput("Missing JOIN keyword".to_string()))?;

            if join_keyword.as_rule() != Rule::join {
                return Err(SqlParseError::InvalidInput(format!(
                    "Expected JOIN keyword, got {:?}",
                    join_keyword.as_rule()
                )));
            }
        } else if first.as_rule() != Rule::join {
            // If it's not a join_kind or JOIN keyword, error
            return Err(SqlParseError::InvalidInput(format!(
                "Expected JOIN keyword or join type, got {:?}",
                first.as_rule()
            )));
        }

        let scan_expr = inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing join table".to_string()))?;
        let join_scan = Self::parse_scan(scan_expr)?;

        inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing ON keyword".to_string()))?;

        let join_condition = inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing join condition".to_string()))?;

        let mut conditions = Vec::new();
        let mut condition_pairs = join_condition.into_inner().peekable();

        while let Some(left_col) = condition_pairs.next() {
            // Parse each condition pair
            let right_col = condition_pairs.next().ok_or_else(|| {
                SqlParseError::InvalidInput("Missing right part of join condition".to_string())
            })?;

            let left_parts = left_col.into_inner().collect::<Vec<_>>();
            let right_parts = right_col.into_inner().collect::<Vec<_>>();

            let left_var = format!("{}.{}", left_parts[0].as_str(), left_parts[1].as_str());
            let right_var = format!("{}.{}", right_parts[0].as_str(), right_parts[1].as_str());

            conditions.push(JoinCondition {
                left_var,
                right_var,
            });

            // Skip the AND operator if present
            if condition_pairs
                .peek()
                .map_or(false, |p| p.as_str().to_uppercase() == "AND")
            {
                condition_pairs.next();
            }
        }

        if conditions.is_empty() {
            return Err(SqlParseError::InvalidInput(
                "No valid join conditions found".to_string(),
            ));
        }

        Ok(JoinClause {
            join_type,
            join_scan,
            join_expr: JoinExpr { conditions },
        })
    }
}
