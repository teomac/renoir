use pest::iterators::Pair;
use super::sql_ast_structure::*;
use super::error::SqlParseError;
use crate::dsl::languages::sql::ast_parser::Rule;

pub struct ConditionParser;

impl ConditionParser {
    pub fn parse(pair: Pair<Rule>) -> Result<WhereClause, SqlParseError> {
        let mut inner = pair.into_inner();
        inner.next(); // Skip WHERE keyword
        
        let conditions = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing where conditions".to_string()))?;
            
        Self::parse_where_conditions(conditions)
    }

    fn parse_where_conditions(pair: Pair<Rule>) -> Result<WhereClause, SqlParseError> {
        let mut pairs = pair.into_inner().peekable();
        
        // Get the first condition or term
        let first = pairs.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Expected condition".to_string()))?;
        
        let mut left = match first.as_rule() {
            Rule::where_term => Self::parse_where_term(first)?,
            Rule::condition => Self::parse_condition(first)?,
            _ => return Err(SqlParseError::InvalidInput(format!("Unexpected rule: {:?}", first.as_rule())))
        };

        // If there are more terms, they must be binary operations
        while let Some(op) = pairs.next() {
            let op = match op.as_str().to_uppercase().as_str() {
                "AND" => BinaryOp::And,
                "OR" => BinaryOp::Or,
                _ => return Err(SqlParseError::InvalidInput(format!("Invalid binary operator: {}", op.as_str())))
            };

            let right_term = pairs.next()
                .ok_or_else(|| SqlParseError::InvalidInput("Expected right term after operator".to_string()))?;

            let right = match right_term.as_rule() {
                Rule::where_term => Self::parse_where_term(right_term)?,
                Rule::condition => Self::parse_condition(right_term)?,
                _ => return Err(SqlParseError::InvalidInput(format!("Unexpected rule: {:?}", right_term.as_rule())))
            };

            left = WhereClause::Expression {
                left: Box::new(left),
                op,
                right: Box::new(right)
            };
        }

        Ok(left)
    }

    fn parse_where_term(pair: Pair<Rule>) -> Result<WhereClause, SqlParseError> {
        let mut inner = pair.into_inner();
        
        // Get first element
        let first = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Empty where term".to_string()))?;
        
        match first.as_rule() {
            Rule::l_paren => {
                // After l_paren we expect where_conditions
                let conditions = inner.next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Empty parentheses".to_string()))?;
                Self::parse_where_conditions(conditions)
            },
            Rule::condition => Self::parse_condition(first),
            _ => Err(SqlParseError::InvalidInput(format!("Invalid where term: {:?}", first.as_rule())))
        }
    }

    fn parse_condition(pair: Pair<Rule>) -> Result<WhereClause, SqlParseError> {
        let mut inner = pair.into_inner();
        
        let left = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing left side of condition".to_string()))?;
            
        let operator = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing operator".to_string()))?;

        match operator.as_rule() {
            Rule::null_operator => {
                let op = match operator.as_str().to_uppercase().as_str() {
                    "IS NULL" => NullOp::IsNull,
                    "IS NOT NULL" => NullOp::IsNotNull,
                    _ => return Err(SqlParseError::InvalidInput(format!("Invalid null operator: {}", operator.as_str())))
                };
                
                Ok(WhereClause::Base(WhereBaseCondition::NullCheck(
                    WhereNullCondition {
                        field: Self::parse_where_field(left)?,
                        operator: op
                    }
                )))
            },
            Rule::operator => {
                let right = inner.next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing right side of condition".to_string()))?;

                let op = match operator.as_str() {
                    ">" => ComparisonOp::GreaterThan,
                    "<" => ComparisonOp::LessThan,
                    ">=" => ComparisonOp::GreaterOrEqualThan,
                    "<=" => ComparisonOp::LessOrEqualThan,
                    "=" => ComparisonOp::Equal,
                    "!=" | "<>" => ComparisonOp::NotEqual,
                    _ => return Err(SqlParseError::InvalidInput(format!("Invalid operator: {}", operator.as_str())))
                };

                Ok(WhereClause::Base(WhereBaseCondition::Comparison(
                    WhereCondition {
                        left_field: Self::parse_where_field(left)?,
                        operator: op,
                        right_field: Self::parse_where_field(right)?
                    }
                )))
            },
            _ => Err(SqlParseError::InvalidInput("Expected operator".to_string()))
        }
    }


    fn parse_arithmetic_expr(pair: Pair<Rule>) -> Result<ArithmeticExpr, SqlParseError> {
        match pair.as_rule() {
            Rule::arithmetic_expr => {
                let mut pairs = pair.into_inner().peekable();
                
                // Parse first term
                let first_term = pairs.next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing first term".to_string()))?;
                let mut left = Self::parse_arithmetic_term(first_term)?;
                
                // Process any subsequent operations
                while let Some(op) = pairs.next() {
                    if let Some(next_term) = pairs.next() {
                        let right = Self::parse_arithmetic_term(next_term)?;
                        left = ArithmeticExpr::BinaryOp(
                            Box::new(left),
                            op.as_str().to_string(),
                            Box::new(right)
                        );
                    }
                }
                
                Ok(left)
            },
            _ => Err(SqlParseError::InvalidInput(format!("Expected arithmetic expression, got {:?}", pair.as_rule())))
        }
    }

    fn parse_arithmetic_term(pair: Pair<Rule>) -> Result<ArithmeticExpr, SqlParseError> {
        match pair.as_rule() {
            Rule::arithmetic_term => {
                let mut inner = pair.into_inner();
                let first = inner.next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Empty arithmetic primary".to_string()))?;
    
                match first.as_rule() {
                    Rule::l_paren => {
                        // For parenthesized expressions, get the inner expression
                        let expr = inner.next()
                            .ok_or_else(|| SqlParseError::InvalidInput("Empty parentheses".to_string()))?;
                        
                        // Skip the right parenthesis
                        inner.next();
                        
                        Self::parse_arithmetic_expr(expr)
                    },
                    _ => Self::parse_arithmetic_factor(first)
                }
            },
            _ => Err(SqlParseError::InvalidInput(format!("Expected arithmetic primary, got {:?}", pair.as_rule())))
        }
    }

    fn parse_arithmetic_factor(pair: Pair<Rule>) -> Result<ArithmeticExpr, SqlParseError> {
        let factor = pair.into_inner().next()
            .ok_or_else(|| SqlParseError::InvalidInput("Empty arithmetic factor".to_string()))?;
        
        match factor.as_rule() {
            Rule::number => {
                // Parse number as SqlLiteral
                let value = if let Ok(int_val) = factor.as_str().parse::<i64>() {
                    SqlLiteral::Integer(int_val)
                } else if let Ok(float_val) = factor.as_str().parse::<f64>() {
                    SqlLiteral::Float(float_val)
                } else {
                    return Err(SqlParseError::InvalidInput("Invalid number format".to_string()));
                };
                Ok(ArithmeticExpr::Literal(value))
            },
            Rule::string_literal => {
                let inner_str = factor.as_str();
                let clean_str = inner_str[1..inner_str.len()-1].to_string();
                Ok(ArithmeticExpr::Literal(SqlLiteral::String(clean_str)))
            },
            Rule::table_column => {
                let mut inner = factor.into_inner();
                let table = inner.next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing table name".to_string()))?
                    .as_str()
                    .to_string();
                let column = inner.next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing column name".to_string()))?
                    .as_str()
                    .to_string();
                Ok(ArithmeticExpr::Column(ColumnRef {
                    table: Some(table),
                    column,
                }))
            },
            Rule::variable => {
                Ok(ArithmeticExpr::Column(ColumnRef {
                    table: None,
                    column: factor.as_str().to_string(),
                }))
            },
            Rule::aggregate_expr => {
                let mut agg = factor.into_inner();
                let func = match agg.next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing aggregate function".to_string()))?
                    .as_str()
                    .to_uppercase()
                    .as_str()
                {
                    "MAX" => AggregateFunction::Max,
                    "MIN" => AggregateFunction::Min,
                    "SUM" => AggregateFunction::Sum,
                    "COUNT" => AggregateFunction::Count,
                    "AVG" => AggregateFunction::Avg,
                    _ => return Err(SqlParseError::InvalidInput("Unknown aggregate function".to_string())),
                };
                
                let col_ref = Self::parse_column_ref(agg.next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing aggregate column".to_string()))?)?;
                
                Ok(ArithmeticExpr::Aggregate(func, col_ref))
            },
            _ => Err(SqlParseError::InvalidInput(format!("Invalid arithmetic factor: {:?}", factor.as_rule())))
        }
    }

    fn parse_column_ref(pair: Pair<Rule>) -> Result<ColumnRef, SqlParseError> {
        match pair.as_rule() {
            Rule::asterisk => Ok(ColumnRef {
                table: None,
                column: "*".to_string(),
            }),
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


    fn parse_where_field(pair: Pair<Rule>) -> Result<WhereField, SqlParseError> {
        match pair.as_rule() {
            Rule::arithmetic_expr => {
                Ok(WhereField {
                    column: None,
                    value: None,
                    arithmetic: Some(Self::parse_arithmetic_expr(pair)?),
                })
            }
            Rule::string_literal => {
                // Remove the single quotes and store the inner content
                let inner_str = pair.as_str();
                let clean_str = inner_str[1..inner_str.len()-1].to_string();
                Ok(WhereField {
                    column: None,
                    value: Some(SqlLiteral::String(clean_str)),
                    arithmetic: None,
                })
            }
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
                    arithmetic: None,
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
                    arithmetic: None,
                })
            }
            Rule::variable => Ok(WhereField {
                column: Some(ColumnRef {
                    table: None,
                    column: pair.as_str().to_string(),
                }),
                value: None,
                arithmetic: None,
            }),
            _ => Err(SqlParseError::InvalidInput(format!(
                "Expected where field, got {:?}",
                pair.as_rule()
            ))),
        }
    }
}