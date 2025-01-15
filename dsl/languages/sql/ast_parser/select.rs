use pest::iterators::Pair;
use super::ast_structure::*;
use super::error::SqlParseError;
use super::literal::LiteralParser;
use crate::dsl::languages::sql::ast_parser::Rule;

pub struct SelectParser;

impl SelectParser {
    pub fn parse(pair: Pair<Rule>) -> Result<SelectClause, SqlParseError> {
        let selection = match pair.as_rule() {
            Rule::variable | Rule::table_column => {
                let col_ref = Self::parse_column_ref(pair)?;
                SelectType::Simple(col_ref)
            },
            Rule::aggregate_expr => {
                Self::parse_aggregate(pair)?
            },
            Rule::select_expr => {
                Self::parse_complex_expression(pair)?
            },
            _ => return Err(SqlParseError::InvalidInput("Invalid SELECT clause".to_string())),
        };

        Ok(SelectClause { selection })
    }

    //function to parse column references
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
            _ => Err(SqlParseError::InvalidInput(format!("Expected column reference, got {:?}", pair.as_rule()))),
        }
    }

    fn parse_aggregate(pair: Pair<Rule>) -> Result<SelectType, SqlParseError> {
        let mut agg = pair.into_inner();
        let func = match agg.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing aggregate function".to_string()))?
            .as_str()
            .to_uppercase()
            .as_str() 
        {
            "MAX" => AggregateFunction::Max,
            _ => return Err(SqlParseError::InvalidInput("Unknown aggregate function".to_string())),
        };
        
        let var_pair = agg.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing aggregate column".to_string()))?;
        let col_ref = Self::parse_column_ref(var_pair)?;
            
        Ok(SelectType::Aggregate(func, col_ref))
    }

    fn parse_complex_expression(pair: Pair<Rule>) -> Result<SelectType, SqlParseError> {
        let mut complex = pair.into_inner();
        let var_pair = complex.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing first operand".to_string()))?;
        let col_ref = Self::parse_column_ref(var_pair)?;
            
        let op = complex.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing operator".to_string()))?
            .as_str()
            .to_string();
            
        let val_str = complex.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing second operand".to_string()))?
            .as_str();
            
        let literal = LiteralParser::parse(val_str)?;
        Ok(SelectType::ComplexValue(col_ref, op, literal))
    }
}