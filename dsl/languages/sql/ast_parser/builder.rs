use pest::iterators::Pairs;
use super::ast_structure::*;
use super::error::SqlParseError;
use super::{
    select::SelectParser,
    from::FromParser,
    condition::ConditionParser,
};
use crate::dsl::languages::sql::ast_parser::Rule;

pub struct SqlASTBuilder;

impl SqlASTBuilder {
    pub fn build_ast_from_pairs(pairs: Pairs<Rule>) -> Result<SqlAST, SqlParseError> {
        for pair in pairs {
            match pair.as_rule() {
                Rule::query => {
                    let mut inner = pair.into_inner();
                    inner.next(); // Skip SELECT keyword
                    
                    let select_part = inner.next()
                        .ok_or_else(|| SqlParseError::InvalidInput("Missing SELECT clause".to_string()))?;
                    let from_part = inner.next()
                        .ok_or_else(|| SqlParseError::InvalidInput("Missing FROM clause".to_string()))?;
                    
                    let where_part = inner.next();

                    return Ok(SqlAST {
                        select: SelectParser::parse(select_part)?,
                        from: FromParser::parse(from_part)?,
                        filter: if let Some(where_expr) = where_part {
                            if where_expr.as_rule() == Rule::where_expr {
                                Some(ConditionParser::parse(where_expr)?)
                            } else {
                                None
                            }
                        } else {
                            None
                        },
                    });
                }
                _ => return Err(SqlParseError::InvalidInput("Expected query".to_string())),
            }
        }
        Err(SqlParseError::InvalidInput("No valid query found".to_string()))
    }
}