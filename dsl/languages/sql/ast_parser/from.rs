use pest::iterators::Pair;
use super::ast_structure::*;
use super::error::SqlParseError;
use crate::dsl::languages::sql::ast_parser::Rule;

pub struct FromParser;

impl FromParser {
    pub fn parse(pair: Pair<Rule>) -> Result<FromClause, SqlParseError> {
        let mut inner = pair.into_inner();
        inner.next(); // Skip FROM keyword
        
        let scan_expr = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing scan expression".to_string()))?;
        let scan = Self::parse_scan(scan_expr)?;
        
        let join = if let Some(join_expr) = inner.next() {
            if join_expr.as_rule() == Rule::join_expr {
                Some(Self::parse_join(join_expr)?)
            } else {
                None
            }
        } else {
            None
        };

        Ok(FromClause { scan, join })
    }

    fn parse_scan(pair: Pair<Rule>) -> Result<ScanClause, SqlParseError> {
        let mut inner = pair.into_inner();
        
        let variable = inner.next()
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
        
        inner.next().ok_or_else(|| SqlParseError::InvalidInput("Missing JOIN keyword".to_string()))?;

        let scan_expr = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing join table".to_string()))?;
        let join_scan = Self::parse_scan(scan_expr)?;

        inner.next().ok_or_else(|| SqlParseError::InvalidInput("Missing ON keyword".to_string()))?;

        let join_condition = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing join condition".to_string()))?;

            let mut condition_parts = join_condition.into_inner();
        
            // Parse both table.column references
            let left_col = condition_parts.next()
                .ok_or_else(|| SqlParseError::InvalidInput("Missing left join condition".to_string()))?;
            let left_parts = left_col.into_inner().collect::<Vec<_>>();
            let left_var = format!("{}.{}", 
                left_parts[0].as_str(), 
                left_parts[1].as_str()
            );
    
            let right_col = condition_parts.next()
                .ok_or_else(|| SqlParseError::InvalidInput("Missing right join condition".to_string()))?;
            let right_parts = right_col.into_inner().collect::<Vec<_>>();
            let right_var = format!("{}.{}", 
                right_parts[0].as_str(), 
                right_parts[1].as_str()
            );

        Ok(JoinClause {
            join_type: JoinType::Inner,
            join_scan,
            join_expr: JoinExpr {
                left_var: left_var,
                right_var: right_var,
            }
        })
    }
}