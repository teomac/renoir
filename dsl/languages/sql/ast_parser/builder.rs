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

                    // Parse the select part differently based on if it's asterisk or column_list
                    let select_clauses = match select_part.as_rule() {
                        Rule::asterisk => {
                            vec![SelectClause {
                                selection: SelectParser::parse(select_part)?,
                                alias: None,
                            }]
                        },
                        Rule::column_list => {
                            // Parse each column in the list
                            select_part.into_inner()
                                .map(|col_pair| {
                                    
                                    
                                    // Get the initial selection from the first item (column_item)
                                    let mut inner_pairs = col_pair.clone().into_inner();
                                    let _column_item = inner_pairs.next()
                                        .ok_or_else(|| SqlParseError::InvalidInput("Missing column item".to_string()))?;
                                    let selection = SelectParser::parse(col_pair.clone())?;
                                    
                                    // Check for alias - look for as_keyword followed by variable
                                    let alias = if inner_pairs.any(|p| p.as_rule() == Rule::as_keyword) {
                                        // If we found AS keyword, take the next token as the alias
                                        col_pair.into_inner()
                                            .last() // Get the last token which should be the alias variable
                                            .map(|alias_pair| alias_pair.as_str().to_string())
                                    } else {
                                        None
                                    };
                        
                                    Ok(SelectClause {
                                        selection,
                                        alias,
                                    })
                                })
                                .collect::<Result<Vec<_>, _>>()?
                        },
                        _ => return Err(SqlParseError::InvalidInput("Invalid SELECT clause".to_string())),
                    };

                    let from_part = inner.next()
                        .ok_or_else(|| SqlParseError::InvalidInput("Missing FROM clause".to_string()))?;
                    
                    let where_part = inner.next();

                    return Ok(SqlAST {
                        select: select_clauses,
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