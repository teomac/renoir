use pest::iterators::Pair;
use super::ast_structure::*;
use super::error::SqlParseError;
use crate::dsl::languages::sql::ast_parser::Rule;

pub struct GroupByParser;

impl GroupByParser {
    pub fn parse(pair: Pair<Rule>) -> Result<GroupByClause, SqlParseError> {
        let mut inner = pair.into_inner();
        
        // Skip 'GROUP BY' keywords if present
        while inner.peek().map_or(false, |p| p.as_str() == "GROUP BY") {
            inner.next();
        }
        
        // Get the group by list
        let group_by_list = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing GROUP BY columns".to_string()))?;
        println!("groupbylist: {:?}", group_by_list);
            
        let mut columns = Vec::new();
        let mut having = None;
        
        // Process group by items first
        for item in group_by_list.into_inner() {
            columns.push(Self::parse_column_ref(item)?);
        }
        
        // Check for HAVING clause
        while let Some(next_token) = inner.next() {
            match next_token.as_rule() {
                Rule::having_keyword => {
                    if let Some(having_conditions) = inner.next() {
                        having = Some(Self::parse_having_conditions(having_conditions)?);
                    }
                }
                _ => {}
            }
        }
        
        if columns.is_empty() {
            return Err(SqlParseError::InvalidInput("Empty GROUP BY clause".to_string()));
        }
        
        Ok(GroupByClause { 
            columns,
            having,
        })
    }
    
    fn parse_column_ref(pair: Pair<Rule>) -> Result<ColumnRef, SqlParseError> {
        match pair.as_rule() {
            Rule::table_column => {
                let mut inner = pair.into_inner();
                let table = inner.next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing table name".to_string()))?
                    .as_str()
                    .to_string();
                let column = inner.next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing column name".to_string()))?
                    .as_str()
                    .to_string();
                Ok(ColumnRef {
                    table: Some(table),
                    column,
                })
            }
            Rule::variable => {
                Ok(ColumnRef {
                    table: None,
                    column: pair.as_str().to_string(),
                })
            }
            _ => Err(SqlParseError::InvalidInput(
                format!("Expected column reference, got {:?}", pair.as_rule())
            )),
        }
    }

    fn parse_having_conditions(pair: Pair<Rule>) -> Result<WhereClause, SqlParseError> {
        use super::condition::ConditionParser;
        let where_conditions = pair.into_inner().next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing HAVING conditions".to_string()))?;
            
        ConditionParser::parse_conditions(where_conditions)
    }
}