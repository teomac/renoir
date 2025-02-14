use pest::iterators::Pair;
use super::sql_ast_structure::*;
use super::error::SqlParseError;
use crate::dsl::languages::sql::ast_parser::Rule;

pub struct ConditionParser;

impl ConditionParser {
    pub fn parse(pair: Pair<Rule>) -> Result<WhereClause, SqlParseError> {
        let mut inner = pair.into_inner();
        inner.next(); // Skip WHERE keyword

        let conditions = inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing where conditions".to_string()))?;

        Self::parse_conditions(conditions)
    }

    pub fn parse_conditions(conditions_pair: Pair<Rule>) -> Result<WhereClause, SqlParseError> {
        let mut pairs = conditions_pair.into_inner().peekable();
        
        // Parse first condition
        let first_condition = pairs.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing condition".to_string()))?;
    
        // Parse the first condition using parse_single_condition
        let mut current = Self::parse_single_condition(first_condition)?;
        
        let mut last = &mut current;
        
        while let Some(op_pair) = pairs.next() {
            if let Some(condition_pair) = pairs.next() {
                let op = match op_pair.as_str().to_uppercase().as_str() {
                    "AND" => BinaryOp::And,
                    "OR" => BinaryOp::Or,
                    _ => return Err(SqlParseError::InvalidInput(
                        format!("Invalid binary operator: {}", op_pair.as_str())
                    )),
                };
                
                // Parse the next condition and create a new WhereClause
                let next_condition = Self::parse_single_condition(condition_pair)?;
                
                last.binary_op = Some(op);
                last.next = Some(Box::new(next_condition));
                
                if let Some(ref mut next) = last.next {
                    last = next;
                }
            }
        }
        
        Ok(current)
    }

    // Before the existing function
    fn parse_single_condition(condition_pair: Pair<Rule>) -> Result<WhereClause, SqlParseError> {
        let mut inner = condition_pair.into_inner();

        let left_field_pair = inner.next().ok_or_else(|| {
            SqlParseError::InvalidInput("Missing variable in condition".to_string())
        })?;

        let left_field = Self::parse_where_field(left_field_pair)?;

        // Check if the next token is a null operator
        let next_token = inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing operator".to_string()))?;

        match next_token.as_rule() {
            Rule::null_operator => {
                // Handle IS NULL / IS NOT NULL
                let operator = match next_token.as_str().to_uppercase().as_str() {
                    "IS NULL" => NullOp::IsNull,
                    "IS NOT NULL" => NullOp::IsNotNull,
                    _ => {
                        return Err(SqlParseError::InvalidInput(format!(
                            "Invalid null operator: {}",
                            next_token.as_str()
                        )))
                    }
                };

                Ok(WhereClause {
                    condition: WhereConditionType::NullCheck(WhereNullCondition {
                        field: left_field,
                        operator,
                    }),
                    binary_op: None,
                    next: None,
                })
            }
            Rule::operator => {
                // Existing comparison operator logic
                let operator = match next_token.as_str() {
                    ">" => ComparisonOp::GreaterThan,
                    "<" => ComparisonOp::LessThan,
                    ">=" => ComparisonOp::GreaterOrEqualThan,
                    "<=" => ComparisonOp::LessOrEqualThan,
                    "=" => ComparisonOp::Equal,
                    "!=" | "<>" => ComparisonOp::NotEqual,
                    op => {
                        return Err(SqlParseError::InvalidInput(format!(
                            "Invalid operator: {}",
                            op
                        )))
                    }
                };

                let right_field_pair = inner.next().ok_or_else(|| {
                    SqlParseError::InvalidInput(
                        "Missing value or variable in right field".to_string(),
                    )
                })?;

                let right_field = Self::parse_where_field(right_field_pair)?;

                Ok(WhereClause {
                    condition: WhereConditionType::Comparison(WhereCondition {
                        left_field,
                        operator,
                        right_field,
                    }),
                    binary_op: None,
                    next: None,
                })
            }
            _ => Err(SqlParseError::InvalidInput("Expected operator".to_string())),
        }
    }

    // New helper function to parse column references
    fn parse_where_field(pair: Pair<Rule>) -> Result<WhereField, SqlParseError> {
        match pair.as_rule() {
            Rule::number => {
                //first we try to parse as int
                let value = pair
                    .as_str()
                    .parse::<i64>()
                    .map(SqlLiteral::Integer)
                    .unwrap_or_else(|_| {
                        //if it fails, we try to parse as float
                        pair.as_str()
                            .parse::<f64>()
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
                Ok(WhereField {
                    column: None,
                    value: Some(value),
                })
            }
            Rule::table_column => {
                let mut inner = pair.into_inner();
                let table = inner
                    .next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing table name".to_string()))?
                    .as_str()
                    .to_string();
                let column = inner
                    .next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing column name".to_string()))?
                    .as_str()
                    .to_string();
                Ok(WhereField {
                    column: Some(ColumnRef {
                        table: Some(table),
                        column,
                    }),
                    value: None,
                })
            }
            Rule::variable => Ok(WhereField {
                column: Some(ColumnRef {
                    table: None,
                    column: pair.as_str().to_string(),
                }),
                value: None,
            }),
            _ => Err(SqlParseError::InvalidInput(format!(
                "Expected column reference, got {:?}",
                pair.as_rule()
            ))),
        }
    }
}
