use pest::iterators::Pair;
use super::ast_structure::*;
use super::error::SqlParseError;
use crate::dsl::languages::sql::ast_parser::Rule;

pub struct ConditionParser;

impl ConditionParser {
    pub fn parse(pair: Pair<Rule>) -> Result<WhereClause, SqlParseError> {
        let mut inner = pair.into_inner();
        inner.next(); // Skip WHERE keyword
        
        let conditions = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing where conditions".to_string()))?;
        
        Self::parse_conditions(conditions)
    }

    pub fn parse_conditions(conditions_pair: Pair<Rule>) -> Result<WhereClause, SqlParseError> {
        let mut pairs = conditions_pair.into_inner().peekable();
        
        let first_condition = pairs.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing condition".to_string()))?;
        let mut current = WhereClause {
            condition: Self::parse_single_condition(first_condition)?,
            binary_op: None,
            next: None
        };
        
        let mut last = &mut current;
        
        while let Some(op_pair) = pairs.next() {
            if let Some(condition_pair) = pairs.next() {
                let op = match op_pair.as_str().to_uppercase().as_str() {
                    "AND" => BinaryOp::And,
                    "OR" => BinaryOp::Or,
                    _ => return Err(SqlParseError::InvalidInput("Invalid binary operator".to_string())),
                };
                
                last.binary_op = Some(op);
                last.next = Some(Box::new(WhereClause {
                    condition: Self::parse_single_condition(condition_pair)?,
                    binary_op: None,
                    next: None,
                }));
                
                if let Some(ref mut next) = last.next {
                    last = next;
                }
            }
        }
        
        Ok(current)
    }

    fn parse_single_condition(condition_pair: Pair<Rule>) -> Result<Condition, SqlParseError> {
        let mut inner = condition_pair.into_inner();

        //parse left field
        let left_field_pair = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing variable in condition".to_string()))?;

        let left_field = Self::parse_where_field(left_field_pair)?;
            
        let operator = match inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing operator in left field".to_string()))?
            .as_str() 
        {
            ">" => ComparisonOp::GreaterThan,
            "<" => ComparisonOp::LessThan,
            ">=" => ComparisonOp::GreaterOrEqualThan,
            "<=" => ComparisonOp::LessOrEqualThan,
            "=" => ComparisonOp::Equal,
            "!=" | "<>" => ComparisonOp::NotEqual,
            op => return Err(SqlParseError::InvalidInput(format!("Invalid operator: {}", op))),
        };

        //parse right field

        let right_field_pair = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing value or variable in right field".to_string()))?;

        let right_field = Self::parse_where_field(right_field_pair)?;

        Ok(Condition {
            left_field,
            operator,
            right_field,
        })
    }

    // New helper function to parse column references
    fn parse_where_field(pair: Pair<Rule>) -> Result<WhereField, SqlParseError> {
        match pair.as_rule() {
            Rule::number => {
                //first we try to parse as int
                let value = pair.as_str().parse::<i64>()
                    .map(SqlLiteral::Integer)
                    .unwrap_or_else(|_| {
                        //if it fails, we try to parse as float
                        pair.as_str().parse::<f64>()
                            .map(SqlLiteral::Float)
                            .unwrap_or_else(|_| {
                                //parse as boolean
                                match pair.as_str() {
                                    "true" => SqlLiteral::Boolean(true),
                                    "false" => SqlLiteral::Boolean(false),
                                    _ => {
                                        //if it fails, we return as string
                                        SqlLiteral::String(pair.as_str().to_string())
                                    }
                                }
                            })
                    });
                Ok(WhereField{
                    column: None,
                    value: Some(value),
                })

            }
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
                Ok(WhereField{
                    column: Some(ColumnRef {
                        table: Some(table),
                        column,
                    }),
                    value: None,
                })
            }
            Rule::variable => {
                Ok(WhereField{
                    column: Some(ColumnRef {
                        table: None,
                        column: pair.as_str().to_string(),
                    }),
                    value: None,
                })
            }
            _ => Err(SqlParseError::InvalidInput(format!("Expected column reference, got {:?}", pair.as_rule()))),
        }
    }
}