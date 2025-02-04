use pest::iterators::Pair;
use super::ast_structure::*;
use super::error::AquaParseError;
use super::condition::ConditionParser;
use crate::dsl::ir::aqua::ast_parser::Rule;

pub struct GroupParser;

impl GroupParser {
    pub fn parse(pair: Pair<Rule>) -> Result<GroupByClause, AquaParseError> {
        let mut inner = pair.into_inner();
        
        let mut columns = Vec::new();
        let mut having = None;
        
        // Process GROUP BY columns
        while let Some(item) = inner.next() {
            match item.as_rule() {
                Rule::qualified_column | Rule::identifier => {
                    columns.push(Self::parse_column_ref(item)?);
                },
                Rule::having_clause => {
                    // Parse HAVING conditions
                    if let Some(having_conditions) = item.into_inner().next() {
                        having = Some(ConditionParser::parse_conditions(having_conditions)?);
                    }
                },
                _ => {
                    // Skip other tokens (like keywords)
                    continue;
                }
            }
        }
        
        if columns.is_empty() {
            return Err(AquaParseError::InvalidInput("Empty GROUP BY clause".to_string()));
        }
        
        Ok(GroupByClause { 
            columns,
            having,
        })
    }
    
    fn parse_column_ref(pair: Pair<Rule>) -> Result<ColumnRef, AquaParseError> {
        match pair.as_rule() {
            Rule::qualified_column => {
                let mut inner = pair.into_inner();
                let table = inner.next()
                    .ok_or_else(|| AquaParseError::InvalidInput("Missing table name".to_string()))?
                    .as_str()
                    .to_string();
                let column = inner.next()
                    .ok_or_else(|| AquaParseError::InvalidInput("Missing column name".to_string()))?
                    .as_str()
                    .to_string();
                Ok(ColumnRef {
                    table: Some(table),
                    column,
                })
            }
            Rule::identifier => {
                Ok(ColumnRef {
                    table: None,
                    column: pair.as_str().to_string(),
                })
            }
            _ => Err(AquaParseError::InvalidInput(
                format!("Expected column reference, got {:?}", pair.as_rule())
            )),
        }
    }
}