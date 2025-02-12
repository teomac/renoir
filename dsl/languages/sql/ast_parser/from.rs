use super::sql_ast_structure::*;
use super::error::SqlParseError;
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

        inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing JOIN keyword".to_string()))?;

        let scan_expr = inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing join table".to_string()))?;
        let join_scan = Self::parse_scan(scan_expr)?;

        // save right table join name
        let right_table_name = join_scan.variable.clone();

        inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing ON keyword".to_string()))?;

        let join_condition = inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing join condition".to_string()))?;

        let mut condition_parts = join_condition.into_inner();

        // Parse both table.column references
        let left_col = condition_parts.next().ok_or_else(|| {
            SqlParseError::InvalidInput("Missing left join condition".to_string())
        })?;
        let left_parts = left_col.into_inner().collect::<Vec<_>>();
        let left_var = format!("{}.{}", left_parts[0].as_str(), left_parts[1].as_str());

        let right_col = condition_parts.next().ok_or_else(|| {
            SqlParseError::InvalidInput("Missing right join condition".to_string())
        })?;
        let right_parts = right_col.into_inner().collect::<Vec<_>>();
        let right_var = format!("{}.{}", right_parts[0].as_str(), right_parts[1].as_str());

        // Check if the right table name is the same as the join alias or the table name
        if right_parts[0].as_str() != right_table_name {
            if join_scan.alias.is_none() {
                // case 1: no alias and right table name is different --> swap left and right tables
                return Ok(JoinClause {
                    join_type: JoinType::Inner,
                    join_scan,
                    join_expr: JoinExpr {
                        left_var: right_var,
                        right_var: left_var,
                    },
                });
            // case 2: alias is present and right table name is different
            } else {
                if right_parts[0].as_str() != join_scan.alias.as_ref().unwrap() {
                    // case 3: alias is present and alias is different --> swap left and right tables
                    return Ok(JoinClause {
                        join_type: JoinType::Inner,
                        join_scan,
                        join_expr: JoinExpr {
                            left_var: right_var,
                            right_var: left_var,
                        },
                    });
                }
                // case 4: alias is the same: do nothing
                else {
                    return Ok(JoinClause {
                        join_type: JoinType::Inner,
                        join_scan,
                        join_expr: JoinExpr {
                            left_var: left_var,
                            right_var: right_var,
                        },
                    });
                }
            }
        } else {
            // case 5: right table name is the same as the join alias or the table name
            return Ok(JoinClause {
                join_type: JoinType::Inner,
                join_scan,
                join_expr: JoinExpr {
                    left_var: left_var,
                    right_var: right_var,
                },
            });
        }
    }
}
