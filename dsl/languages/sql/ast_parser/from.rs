use super::error::SqlParseError;
use super::sql_ast_structure::*;
use super::builder::SqlASTBuilder;
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
        
        // Parse scan source which can now be a table or a subquery
        let scan = match scan_expr.as_rule() {
            Rule::scan_expr => {
                let scan_clause = Self::parse_scan(scan_expr)?;
                FromSource::Table(scan_clause)
            },
            Rule::subquery_expr => {
                // Extract and parse the subquery
                let subquery = Self::parse_subquery(scan_expr)?;
                
                // Check for an alias after the subquery
                let alias = if let Some(next_token) = inner.peek() {
                    if next_token.as_rule() == Rule::as_keyword {
                        inner.next(); // Skip AS
                        if let Some(alias_token) = inner.next() {
                            Some(alias_token.as_str().to_string())
                        } else {
                            None
                        }
                    } else if next_token.as_rule() == Rule::variable {
                        Some(inner.next().unwrap().as_str().to_string())
                    } else {
                        None
                    }
                } else {
                    None
                };
                
                FromSource::Subquery(Box::new(subquery), alias)
            },
            _ => return Err(SqlParseError::InvalidInput(
                format!("Expected scan expression or subquery, got {:?}", scan_expr.as_rule())
            ))
        };

        let mut joins = Vec::new();

        while let Some(join_expr) = inner.next() {
            if join_expr.as_rule() == Rule::join_expr {
                joins.push(Self::parse_join(join_expr)?);
            }
        }

        Ok(FromClause {
            scan,
            joins: if joins.is_empty() { None } else { Some(joins) },
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

    // New: Parse subquery in FROM clause
    fn parse_subquery(pair: Pair<Rule>) -> Result<SqlAST, SqlParseError> {
        // Extract the subquery part
        let subquery = pair.into_inner()
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Empty subquery".to_string()))?;
        
        // Use the builder to parse the subquery
        SqlASTBuilder::build_ast_from_pairs(subquery.into_inner())
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

        // Parse the join source (table or subquery)
        let join_source_pair = inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing join table or subquery".to_string()))?;
        
        // Process join source which can now be a table or a subquery
        let join_scan = match join_source_pair.as_rule() {
            Rule::scan_expr => {
                let scan_clause = Self::parse_scan(join_source_pair)?;
                FromSource::Table(scan_clause)
            },
            Rule::subquery_expr => {
                // Extract and parse the subquery
                let subquery = Self::parse_subquery(join_source_pair)?;
                
                // Check for an alias after the subquery
                let alias = if let Some(next_token) = inner.peek() {
                    if next_token.as_rule() == Rule::as_keyword {
                        inner.next(); // Skip AS
                        if let Some(alias_token) = inner.next() {
                            Some(alias_token.as_str().to_string())
                        } else {
                            None
                        }
                    } else if next_token.as_rule() == Rule::variable {
                        Some(inner.next().unwrap().as_str().to_string())
                    } else {
                        None
                    }
                } else {
                    None
                };
                
                FromSource::Subquery(Box::new(subquery), alias)
            },
            _ => return Err(SqlParseError::InvalidInput(
                format!("Expected scan expression or subquery, got {:?}", join_source_pair.as_rule())
            ))
        };

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