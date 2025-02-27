use pest::iterators::Pair;
use super::sql_ast_structure::*;
use super::error::SqlParseError;
use crate::dsl::languages::sql::ast_parser::Rule;

pub struct GroupByParser;

impl GroupByParser {
    pub fn parse(pair: Pair<Rule>) -> Result<GroupByClause, SqlParseError> {
        let mut inner = pair.into_inner();
        
        inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing GROUP BY keyword".to_string()))?;
        
        let group_by_list = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing GROUP BY columns".to_string()))?;
            
        let mut columns = Vec::new();
        let mut having = None;
        
        // Process group by items first
        for item in group_by_list.into_inner() {
            columns.push(Self::parse_column_ref(item)?);
        }

        if columns.is_empty() {
            return Err(SqlParseError::InvalidInput("Empty GROUP BY clause".to_string()));
        }
        
        // Check for HAVING clause with group by columns
        while let Some(next_token) = inner.next() {
            match next_token.as_rule() {
                Rule::having_keyword => {
                    if let Some(having_expr) = inner.next() {
                        having = Some(Self::parse_having_expr(having_expr, &columns)?);
                    }
                }
                _ => {}
            }
        }
        
        Ok(GroupByClause { 
            columns,
            having,
        })
    }


    
    //function to parse column reference
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

    //////////////////////////////////////////////////////////////////////////////////
    /// 
    /// 


    // New method to parse having expressions
    fn parse_having_expr(pair: Pair<Rule>, group_by_cols: &[ColumnRef]) -> Result<HavingClause, SqlParseError> {
        let mut pairs = pair.into_inner().peekable();
        
        // Get the first term
        let first = pairs.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Expected having condition".to_string()))?;
        
        let mut left = match first.as_rule() {
            Rule::having_term => Self::parse_having_term(first, group_by_cols)?,
            Rule::condition => Self::parse_having_condition(first, group_by_cols)?,
            _ => return Err(SqlParseError::InvalidInput(format!("Unexpected rule in having: {:?}", first.as_rule())))
        };

        // Process binary operations if present
        while let Some(op) = pairs.next() {
            let op = match op.as_str().to_uppercase().as_str() {
                "AND" => BinaryOp::And,
                "OR" => BinaryOp::Or,
                _ => return Err(SqlParseError::InvalidInput(format!("Invalid binary operator in having: {}", op.as_str())))
            };

            let right_term = pairs.next()
                .ok_or_else(|| SqlParseError::InvalidInput("Expected right term after operator in having".to_string()))?;

            let right = match right_term.as_rule() {
                Rule::having_term => Self::parse_having_term(right_term, group_by_cols)?,
                Rule::condition => Self::parse_having_condition(right_term, group_by_cols)?,
                _ => return Err(SqlParseError::InvalidInput(format!("Unexpected rule in having: {:?}", right_term.as_rule())))
            };

            left = HavingClause::Expression {
                left: Box::new(left),
                op,
                right: Box::new(right)
            };
        }

        Ok(left)
    }

    // New method to parse having terms (including parenthesized expressions)
    fn parse_having_term(pair: Pair<Rule>, group_by_cols: &[ColumnRef]) -> Result<HavingClause, SqlParseError> {
        let mut inner = pair.into_inner();
        
        let first = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Empty having term".to_string()))?;
        
        match first.as_rule() {
            Rule::l_paren => {
                let conditions = inner.next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Empty parentheses in having".to_string()))?;
                Self::parse_having_expr(conditions, group_by_cols)
            },
            Rule::condition => Self::parse_having_condition(first, group_by_cols),
            _ => Err(SqlParseError::InvalidInput(format!("Invalid having term: {:?}", first.as_rule())))
        }
    }

    // Modified parse_having_condition to include aggregate validation
    fn parse_having_condition(pair: Pair<Rule>, group_by_cols: &[ColumnRef]) -> Result<HavingClause, SqlParseError> {
        let mut inner = pair.into_inner();
        
        let left = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing left side of having condition".to_string()))?;
            
        let operator = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing operator in having".to_string()))?;

        match operator.as_rule() {
            Rule::null_operator => {
                let field = Self::parse_having_field(left)?;
                
                // Check if the field is in the GROUP BY clause
            if let Some(col_ref) = &field.column {
                let is_in_group_by = group_by_cols.iter().any(|group_col| {
                    group_col.column == col_ref.column && 
                    (group_col.table == col_ref.table || col_ref.table.is_none())
                });
                
                // Only enforce aggregation rule if it's not in the GROUP BY
                if !is_in_group_by && field.aggregate.is_none() {
                    return Err(SqlParseError::InvalidInput(
                        format!("Column {} must be aggregated or in GROUP BY clause", col_ref.to_string())
                    ));
                }
            }
                let op = match operator.as_str().to_uppercase().as_str() {
                    "IS NULL" => NullOp::IsNull,
                    "IS NOT NULL" => NullOp::IsNotNull,
                    _ => return Err(SqlParseError::InvalidInput(format!("Invalid null operator in having: {}", operator.as_str())))
                };
                
                Ok(HavingClause::Base(HavingBaseCondition::NullCheck(
                    HavingNullCondition {
                        field,
                        operator: op
                    }
                )))
            },
            Rule::operator => {
                let right = inner.next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing right side of having condition".to_string()))?;

                let left_field = Self::parse_having_field(left)?;
                let right_field = Self::parse_having_field(right)?;

                // Validate arithmetic expressions if present
                if let Some(ref arithmetic) = left_field.arithmetic {
                    Self::validate_having_arithmetic(arithmetic, group_by_cols)?;
                }
                if let Some(ref arithmetic) = right_field.arithmetic {
                    Self::validate_having_arithmetic(arithmetic, group_by_cols)?;
                }

                // Also validate non-arithmetic fields
                if left_field.arithmetic.is_none() && right_field.arithmetic.is_none() {
                    if let (Some(left_col), Some(right_col)) = (&left_field.column, &right_field.column) {
                        // Check if either column is in GROUP BY
                        let left_in_group_by = group_by_cols.iter().any(|group_col| {
                            group_col.column == left_col.column && 
                            group_col.table == left_col.table
                        });
                        let right_in_group_by = group_by_cols.iter().any(|group_col| {
                            group_col.column == right_col.column && 
                            group_col.table == right_col.table
                        });
                        
                        if !left_in_group_by && !right_in_group_by && 
                           left_field.aggregate.is_none() && right_field.aggregate.is_none() {
                            return Err(SqlParseError::InvalidInput(
                                format!("Either {} or {} must be aggregated or in GROUP BY clause", 
                                    left_col.to_string(), right_col.to_string())
                            ));
                        }
                    }
                }

                let op = match operator.as_str() {
                    ">" => ComparisonOp::GreaterThan,
                    "<" => ComparisonOp::LessThan,
                    ">=" => ComparisonOp::GreaterOrEqualThan,
                    "<=" => ComparisonOp::LessOrEqualThan,
                    "=" => ComparisonOp::Equal,
                    "!=" | "<>" => ComparisonOp::NotEqual,
                    _ => return Err(SqlParseError::InvalidInput(format!("Invalid operator in having: {}", operator.as_str())))
                };

                Ok(HavingClause::Base(HavingBaseCondition::Comparison(
                    HavingCondition {
                        left_field,
                        operator: op,
                        right_field
                    }
                )))
            },
            _ => Err(SqlParseError::InvalidInput("Expected operator in having condition".to_string()))
        }
    }

    // Update parse_having_field to handle arithmetic expressions
    fn parse_having_field(pair: Pair<Rule>) -> Result<HavingField, SqlParseError> {
        match pair.as_rule() {
            Rule::arithmetic_expr => {
                Ok(HavingField {
                    column: None,
                    value: None,
                    aggregate: None,
                    arithmetic: Some(Self::parse_arithmetic_expr(pair)?),
                })
            }
            Rule::boolean => {  // Add this case
                let value = pair.as_str().parse::<bool>()
                    .map_err(|_| SqlParseError::InvalidInput("Invalid boolean value".to_string()))?;
                Ok(HavingField {
                    column: None,
                    value: Some(SqlLiteral::Boolean(value)),
                    aggregate: None,
                    arithmetic: None,
                })
            },
            Rule::string_literal => {
                let inner_str = pair.as_str();
                let clean_str = inner_str[1..inner_str.len()-1].to_string();
                Ok(HavingField {
                    column: None,
                    value: Some(SqlLiteral::String(clean_str)),
                    aggregate: None,
                    arithmetic: None,
                })
            }
            Rule::number => {
                let value = pair.as_str().parse::<i64>()
                    .map(SqlLiteral::Integer)
                    .unwrap_or_else(|_| {
                        pair.as_str().parse::<f64>()
                            .map(SqlLiteral::Float)
                            .unwrap_or_else(|_| SqlLiteral::String(pair.as_str().to_string()))
                    });
                Ok(HavingField {
                    column: None,
                    value: Some(value),
                    aggregate: None,
                    arithmetic: None,
                })
            }
            Rule::aggregate_expr => {
                let mut agg = pair.into_inner();
                let aggregate = match agg.next().unwrap().as_str() {
                    "SUM" => AggregateFunction::Sum,
                    "AVG" => AggregateFunction::Avg,
                    "COUNT" => AggregateFunction::Count,
                    "MIN" => AggregateFunction::Min,
                    "MAX" => AggregateFunction::Max,
                    _ => return Err(SqlParseError::InvalidInput("Invalid aggregate function".to_string())),
                };
                let column = Self::parse_column_ref(agg.next().unwrap())?;
                Ok(HavingField {
                    column: None,
                    value: None,
                    aggregate: Some((aggregate, column)),
                    arithmetic: None,
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
                Ok(HavingField {
                    column: Some(ColumnRef {
                        table: Some(table),
                        column,
                    }),
                    value: None,
                    aggregate: None,
                    arithmetic: None,
                })
            }
            Rule::variable => {
                Ok(HavingField {
                    column: Some(ColumnRef {
                        table: None,
                        column: pair.as_str().to_string(),
                    }),
                    value: None,
                    aggregate: None,
                    arithmetic: None,
                })
            }
            _ => Err(SqlParseError::InvalidInput(
                format!("Expected having field, got {:?}", pair.as_rule())
            )),
        }
    }
 // Add method to parse arithmetic expressions in HAVING clause
 fn parse_arithmetic_expr(pair: Pair<Rule>) -> Result<ArithmeticExpr, SqlParseError> {
    match pair.as_rule() {
        Rule::arithmetic_expr => {
            let mut pairs = pair.into_inner().peekable();
            
            let first_term = pairs.next()
                .ok_or_else(|| SqlParseError::InvalidInput("Missing first term".to_string()))?;
            let mut left = Self::parse_arithmetic_term(first_term)?;
            
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
        _ => Err(SqlParseError::InvalidInput(
            format!("Expected arithmetic expression, got {:?}", pair.as_rule())
        ))
    }
}

// Add methods for parsing arithmetic terms and factors
fn parse_arithmetic_term(pair: Pair<Rule>) -> Result<ArithmeticExpr, SqlParseError> {
    match pair.as_rule() {
        Rule::arithmetic_term => {
            let mut inner = pair.into_inner();
            let first = inner.next()
                .ok_or_else(|| SqlParseError::InvalidInput("Empty arithmetic term".to_string()))?;

            match first.as_rule() {
                Rule::l_paren => {
                    let expr = inner.next()
                        .ok_or_else(|| SqlParseError::InvalidInput("Empty parentheses".to_string()))?;
                    Self::parse_arithmetic_expr(expr)
                },
                _ => Self::parse_arithmetic_factor(first)
            }
        },
        _ => Err(SqlParseError::InvalidInput(
            format!("Expected arithmetic term, got {:?}", pair.as_rule())
        ))
    }
}

    // Update parse_arithmetic_factor to include aggregate functions
    fn parse_arithmetic_factor(pair: Pair<Rule>) -> Result<ArithmeticExpr, SqlParseError> {
        let factor = pair.into_inner().next()
            .ok_or_else(|| SqlParseError::InvalidInput("Empty arithmetic factor".to_string()))?;
        
        match factor.as_rule() {
            Rule::number => {
                let value = if let Ok(int_val) = factor.as_str().parse::<i64>() {
                    SqlLiteral::Integer(int_val)
                } else if let Ok(float_val) = factor.as_str().parse::<f64>() {
                    SqlLiteral::Float(float_val)
                } else {
                    return Err(SqlParseError::InvalidInput("Invalid number format".to_string()));
                };
                Ok(ArithmeticExpr::Literal(value))
            },
            Rule::boolean => {  // Add this case
                let value = factor.as_str().parse::<bool>()
                    .map_err(|_| SqlParseError::InvalidInput("Invalid boolean value".to_string()))?;
                Ok(ArithmeticExpr::Literal(SqlLiteral::Boolean(value)))
            },
            Rule::string_literal => {
                // Extract the string value without quotes
                let inner_str = factor.as_str();
                let clean_str = inner_str[1..inner_str.len()-1].to_string();
                Ok(ArithmeticExpr::Literal(SqlLiteral::String(clean_str)))
            },
            Rule::aggregate_expr => {
                let mut agg = factor.into_inner();
                let aggregate = match agg.next()
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
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing column in aggregate".to_string()))?)?;

                Ok(ArithmeticExpr::Aggregate(aggregate, col_ref))
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
            _ => Err(SqlParseError::InvalidInput(
                format!("Invalid arithmetic factor: {:?}", factor.as_rule())
            ))
        }
    }

     // Add validation for arithmetic expressions in having conditions
     fn validate_having_arithmetic(expr: &ArithmeticExpr, group_by_cols: &[ColumnRef]) -> Result<(), SqlParseError> {
        match expr {
            ArithmeticExpr::Column(col_ref) => {
                // Check if this column is in GROUP BY
                if group_by_cols.iter().any(|group_col| {
                    group_col.column == col_ref.column && 
                    group_col.table == col_ref.table
                }) {
                    Ok(()) // Column is in GROUP BY, so it's allowed
                } else {
                    Err(SqlParseError::InvalidInput(
                        format!("Column {} must be aggregated in HAVING clause", col_ref.to_string())
                    ))
                }
            },
            ArithmeticExpr::Literal(_) => Ok(()), // Literals are always allowed
            ArithmeticExpr::Aggregate(_, _) => Ok(()), // Aggregates are allowed
            ArithmeticExpr::BinaryOp(left, _, right) => {
                // Recursively validate both sides
                Self::validate_having_arithmetic(left, group_by_cols)?;
                Self::validate_having_arithmetic(right, group_by_cols)
            }
        }
    }
}