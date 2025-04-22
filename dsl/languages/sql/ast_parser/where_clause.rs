use super::error::SqlParseError;
use super::{sql_ast_structure::*, SqlParser};
use crate::dsl::languages::sql::ast_parser::Rule;
use pest::iterators::Pair;

pub struct ConditionParser;

impl ConditionParser {
    pub fn parse(pair: Pair<Rule>) -> Result<WhereClause, Box<SqlParseError>> {
        let mut inner = pair.into_inner();
        inner.next(); // Skip WHERE keyword

        let conditions = inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing where conditions".to_string()))?;

        Self::parse_where_conditions(conditions)
    }

    fn parse_where_conditions(pair: Pair<Rule>) -> Result<WhereClause, Box<SqlParseError>> {
        let mut pairs = pair.into_inner().peekable();

        // Get the first condition or term
        let first = pairs
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Expected condition".to_string()))?;

        let mut left = match first.as_rule() {
            Rule::where_term => Self::parse_where_term(first)?,
            Rule::condition => Self::parse_condition(first)?,
            _ => {
                return Err(Box::new(SqlParseError::InvalidInput(format!(
                    "Unexpected rule: {:?}",
                    first.as_rule()
                ))))
            }
        };

        // If there are more terms, they must be binary operations
        while let Some(op) = pairs.next() {
            let op = match op.as_str().to_uppercase().as_str() {
                "AND" => BinaryOp::And,
                "OR" => BinaryOp::Or,
                _ => {
                    return Err(Box::new(SqlParseError::InvalidInput(format!(
                        "Invalid binary operator: {}",
                        op.as_str()
                    ))))
                }
            };

            let right_term = pairs.next().ok_or_else(|| {
                SqlParseError::InvalidInput("Expected right term after operator".to_string())
            })?;

            let right = match right_term.as_rule() {
                Rule::where_term => Self::parse_where_term(right_term)?,
                Rule::condition => Self::parse_condition(right_term)?,
                _ => {
                    return Err(Box::new(SqlParseError::InvalidInput(format!(
                        "Unexpected rule: {:?}",
                        right_term.as_rule()
                    ))))
                }
            };

            left = WhereClause::Expression {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_where_term(pair: Pair<Rule>) -> Result<WhereClause, Box<SqlParseError>> {
        let mut inner = pair.into_inner();

        // Get first element
        let first = inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Empty where term".to_string()))?;

        match first.as_rule() {
            Rule::l_paren => {
                // After l_paren we expect where_conditions
                let conditions = inner
                    .next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Empty parentheses".to_string()))?;
                Self::parse_where_conditions(conditions)
            }
            Rule::condition => Self::parse_condition(first),
            _ => Err(Box::new(SqlParseError::InvalidInput(format!(
                "Invalid where term: {:?}",
                first.as_rule()
            )))),
        }
    }

    fn parse_condition(pair: Pair<Rule>) -> Result<WhereClause, Box<SqlParseError>> {
        // Check the condition type by examining the first child rule
        let mut check_pairs = pair.clone().into_inner();
        let first_rule = check_pairs.next();

        if let Some(first) = first_rule {
            if first.as_rule() == Rule::boolean {
                // Handle boolean expressions directly
                let value = match first.as_str() {
                    "true" => true,
                    "false" => false,
                    _ => {
                        return Err(Box::new(SqlParseError::InvalidInput(
                            "Invalid boolean value".to_string(),
                        )))
                    }
                };

                return Ok(WhereClause::Base(WhereBaseCondition::Boolean(value)));
            }
            // Handle EXISTS expression directly
            if first.as_rule() == Rule::exists_expr {
                // Get the inner parts of EXISTS expression
                let mut exists_inner = first.into_inner();

                // First part is the EXISTS keyword
                let exists_keyword = exists_inner.next().ok_or_else(|| {
                    SqlParseError::InvalidInput("Missing EXISTS keyword".to_string())
                })?;

                // Check if it's negated (NOT EXISTS)
                let is_negated = exists_keyword.as_str().to_uppercase().contains("NOT");

                // Next part is the subquery expression
                let subquery_expr = exists_inner.next().ok_or_else(|| {
                    SqlParseError::InvalidInput("Missing subquery in EXISTS".to_string())
                })?;

                // Now parse the subquery from the subquery expression
                if subquery_expr.as_rule() != Rule::subquery_expr {
                    return Err(Box::new(SqlParseError::InvalidInput(format!(
                        "Expected subquery expression after EXISTS, got {:?}",
                        subquery_expr.as_rule()
                    ))));
                }

                // Process the subquery
                let subquery = SqlParser::parse_subquery(subquery_expr)?;

                return Ok(WhereClause::Base(WhereBaseCondition::Exists(
                    Box::new(subquery),
                    is_negated,
                )));
            }

            // Handle IN expression directly
            if first.as_rule() == Rule::in_expr {
                // Get the inner parts of IN expression
                let mut in_inner = first.into_inner();

                // First part is the arithmetic expression or subquery
                let left_expr = in_inner.next().ok_or_else(|| {
                    SqlParseError::InvalidInput("Missing operand in IN expression".to_string())
                })?;

                // check if we have a subquery before the IN

                let mut in_subquery = None;
                let mut complex_field = None;

                if left_expr.as_rule() == Rule::subquery_expr {
                    in_subquery = Some(SqlParser::parse_subquery(left_expr)?);
                } else if left_expr.as_rule() == Rule::arithmetic_expr {
                    // Parse the arithmetic expression into a ComplexField
                    complex_field = Some(WhereField {
                        column: None,
                        value: None,
                        arithmetic: Some((Self::parse_arithmetic_expr(left_expr, false))?),
                        subquery: None,
                    });
                } else {
                    return Err(Box::new(SqlParseError::InvalidInput(format!(
                        "Expected arithmetic expression or subquery in IN expression, got {:?}",
                        left_expr.as_rule()
                    ))));
                }

                // Next part is the IN keyword
                let in_keyword = in_inner
                    .next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing IN keyword".to_string()))?;

                // Check if it's negated (NOT IN)
                let is_negated = in_keyword.as_str().to_uppercase().contains("NOT");

                // Last part is the subquery
                let subquery_expr = in_inner.next().ok_or_else(|| {
                    SqlParseError::InvalidInput("Missing subquery in IN expression".to_string())
                })?;

                // Parse the inner SQL directly
                let subquery = SqlParser::parse_subquery(subquery_expr)?;

                if let Some(in_subquery) = in_subquery {
                    return Ok(WhereClause::Base(WhereBaseCondition::In(
                        InCondition::InSubquery(
                            Box::new(in_subquery),
                            Box::new(subquery),
                            is_negated,
                        ),
                    )));
                } else {
                    return Ok(WhereClause::Base(WhereBaseCondition::In(
                        InCondition::InWhere(
                            complex_field.unwrap(),
                            Box::new(subquery),
                            is_negated,
                        ),
                    )));
                }
            }
        }

        // If we get here, it's a regular comparison or NULL check
        let mut inner = pair.into_inner();
        let left = inner.next().ok_or_else(|| {
            SqlParseError::InvalidInput("Missing left side of condition".to_string())
        })?;

        let operator = inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing operator".to_string()))?;

        match operator.as_rule() {
            Rule::null_operator => {
                let op = match operator.as_str().to_uppercase().as_str() {
                    "IS NULL" => NullOp::IsNull,
                    "IS NOT NULL" => NullOp::IsNotNull,
                    _ => {
                        return Err(Box::new(SqlParseError::InvalidInput(format!(
                            "Invalid null operator: {}",
                            operator.as_str()
                        ))))
                    }
                };

                Ok(WhereClause::Base(WhereBaseCondition::NullCheck(
                    WhereNullCondition {
                        field: Self::parse_where_field(left)?,
                        operator: op,
                    },
                )))
            }
            Rule::operator => {
                let right = inner.next().ok_or_else(|| {
                    SqlParseError::InvalidInput("Missing right side of condition".to_string())
                })?;

                let op = match operator.as_str() {
                    ">" => ComparisonOp::GreaterThan,
                    "<" => ComparisonOp::LessThan,
                    ">=" => ComparisonOp::GreaterOrEqualThan,
                    "<=" => ComparisonOp::LessOrEqualThan,
                    "=" => ComparisonOp::Equal,
                    "!=" | "<>" => ComparisonOp::NotEqual,
                    _ => {
                        return Err(Box::new(SqlParseError::InvalidInput(format!(
                            "Invalid operator: {}",
                            operator.as_str()
                        ))))
                    }
                };

                Ok(WhereClause::Base(WhereBaseCondition::Comparison(
                    WhereCondition {
                        left_field: Self::parse_where_field(left)?,
                        operator: op,
                        right_field: Self::parse_where_field(right)?,
                    },
                )))
            }
            _ => Err(Box::new(SqlParseError::InvalidInput(format!(
                "Expected operator, got {:?}",
                operator.as_rule()
            )))),
        }
    }


    fn parse_arithmetic_expr(pair: Pair<Rule>, is_parenthesized: bool) -> Result<ArithmeticExpr, Box<SqlParseError>> {
        match pair.as_rule() {
            Rule::arithmetic_expr => {
                let mut pairs = pair.clone().into_inner().peekable();
    
                let first_term = pairs
                    .next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing first term".to_string()))?;
                    
                let mut left = Self::parse_arithmetic_term(first_term)?;
    
                // Process any subsequent operations
                while let Some(op) = pairs.next() {
                    if let Some(next_term) = pairs.next() {
                        let right = Self::parse_arithmetic_term(next_term)?;
                        left = ArithmeticExpr::NestedExpr(
                            Box::new(left),
                            op.as_str().to_string(),
                            Box::new(right),
                            false // Intermediate operations are not parenthesized
                        );
                    }
                }
    
                if is_parenthesized {
                    if let ArithmeticExpr::NestedExpr(l, op, r, _) = left {
                        left = ArithmeticExpr::NestedExpr(l, op, r, true);
                    }
                }
    
                Ok(left)
            }
            _ => Err(Box::new(SqlParseError::InvalidInput(format!(
                "Expected arithmetic expression, got {:?}",
                pair.as_rule()
            ))))
        }
    }
    
    fn parse_arithmetic_term(pair: Pair<Rule>) -> Result<ArithmeticExpr, Box<SqlParseError>> {
        match pair.as_rule() {
            Rule::arithmetic_term => {
                let mut inner = pair.clone().into_inner();
                let first = inner.next().ok_or_else(|| {
                    SqlParseError::InvalidInput("Empty arithmetic term".to_string())
                })?;
    
                match first.as_rule() {
                    Rule::l_paren => {
                        // For parenthesized expressions, create a new expression
                        let expr = inner.next().ok_or_else(|| {
                            SqlParseError::InvalidInput("Empty parentheses".to_string())
                        })?;
                        
                        // Parse the inner expression
                        let result = Self::parse_arithmetic_expr(expr, true)?;
                        
                        // Return the parenthesized expression
                        Ok(result)
                    }
                    _ => Self::parse_arithmetic_factor(first),
                }
            }
            _ => Self::parse_arithmetic_factor(pair),
        }
    }

    fn parse_arithmetic_factor(pair: Pair<Rule>) -> Result<ArithmeticExpr, Box<SqlParseError>> {
        let factor = pair
            .into_inner()
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Empty arithmetic factor".to_string()))?;

        match factor.as_rule() {
            Rule::number => {
                // Parse number as SqlLiteral
                let value = if let Ok(int_val) = factor.as_str().parse::<i64>() {
                    SqlLiteral::Integer(int_val)
                } else if let Ok(float_val) = factor.as_str().parse::<f64>() {
                    SqlLiteral::Float(float_val)
                } else {
                    return Err(Box::new(SqlParseError::InvalidInput(
                        "Invalid number format".to_string(),
                    )));
                };
                Ok(ArithmeticExpr::Literal(value))
            }
            Rule::boolean => {
                let value = match factor.as_str() {
                    "true" => SqlLiteral::Boolean(true),
                    "false" => SqlLiteral::Boolean(false),
                    _ => {
                        return Err(Box::new(SqlParseError::InvalidInput(
                            "Invalid boolean value".to_string(),
                        )))
                    }
                };
                Ok(ArithmeticExpr::Literal(value))
            }
            Rule::string_literal => {
                let inner_str = factor.as_str();
                let clean_str = inner_str[1..inner_str.len() - 1].to_string();
                Ok(ArithmeticExpr::Literal(SqlLiteral::String(clean_str)))
            }
            Rule::table_column => {
                let mut inner = factor.into_inner();
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
                Ok(ArithmeticExpr::Column(ColumnRef {
                    table: Some(table),
                    column,
                }))
            }
            Rule::variable => Ok(ArithmeticExpr::Column(ColumnRef {
                table: None,
                column: factor.as_str().to_string(),
            })),
            Rule::aggregate_expr => {
                let mut agg = factor.into_inner();
                let func = match agg
                    .next()
                    .ok_or_else(|| {
                        SqlParseError::InvalidInput("Missing aggregate function".to_string())
                    })?
                    .as_str()
                    .to_uppercase()
                    .as_str()
                {
                    "MAX" => AggregateFunction::Max,
                    "MIN" => AggregateFunction::Min,
                    "SUM" => AggregateFunction::Sum,
                    "COUNT" => AggregateFunction::Count,
                    "AVG" => AggregateFunction::Avg,
                    _ => {
                        return Err(Box::new(SqlParseError::InvalidInput(
                            "Unknown aggregate function".to_string(),
                        )))
                    }
                };

                let col_ref = Self::parse_column_ref(agg.next().ok_or_else(|| {
                    SqlParseError::InvalidInput("Missing aggregate column".to_string())
                })?)?;

                Ok(ArithmeticExpr::Aggregate(func, col_ref))
            }
            Rule::subquery_expr => {
                // New: Handle subquery in arithmetic expression
                let subquery = SqlParser::parse_subquery(factor)?;
                Ok(ArithmeticExpr::Subquery(Box::new(subquery)))
            }
            _ => Err(Box::new(SqlParseError::InvalidInput(format!(
                "Invalid arithmetic factor: {:?}",
                factor.as_rule()
            )))),
        }
    }

    fn parse_column_ref(pair: Pair<Rule>) -> Result<ColumnRef, Box<SqlParseError>> {
        match pair.as_rule() {
            Rule::asterisk => Ok(ColumnRef {
                table: None,
                column: "*".to_string(),
            }),
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
                Ok(ColumnRef {
                    table: Some(table),
                    column,
                })
            }
            Rule::variable => Ok(ColumnRef {
                table: None,
                column: pair.as_str().to_string(),
            }),
            _ => Err(Box::new(SqlParseError::InvalidInput(format!(
                "Expected column reference, got {:?}",
                pair.as_rule()
            )))),
        }
    }

    fn parse_where_field(pair: Pair<Rule>) -> Result<WhereField, Box<SqlParseError>> {
        match pair.as_rule() {
            Rule::arithmetic_expr => Ok(WhereField {
                column: None,
                value: None,
                arithmetic: Some(Self::parse_arithmetic_expr(pair, false)?),
                subquery: None,
            }),
            Rule::subquery_expr => {
                // New: Handle subquery in WHERE field
                let subquery = SqlParser::parse_subquery(pair)?;
                Ok(WhereField {
                    column: None,
                    value: None,
                    arithmetic: None,
                    subquery: Some(Box::new(subquery)),
                })
            }
            Rule::boolean => {
                let value = match pair.as_str() {
                    "true" => SqlLiteral::Boolean(true),
                    "false" => SqlLiteral::Boolean(false),
                    _ => {
                        return Err(Box::new(SqlParseError::InvalidInput(
                            "Invalid boolean value".to_string(),
                        )))
                    }
                };
                Ok(WhereField {
                    column: None,
                    value: Some(value),
                    arithmetic: None,
                    subquery: None,
                })
            }
            Rule::string_literal => {
                // Remove the single quotes and store the inner content
                let inner_str = pair.as_str();
                let clean_str = inner_str[1..inner_str.len() - 1].to_string();
                Ok(WhereField {
                    column: None,
                    value: Some(SqlLiteral::String(clean_str)),
                    arithmetic: None,
                    subquery: None,
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
                    subquery: None,
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
                    subquery: None,
                })
            }
            Rule::variable => Ok(WhereField {
                column: Some(ColumnRef {
                    table: None,
                    column: pair.as_str().to_string(),
                }),
                value: None,
                arithmetic: None,
                subquery: None,
            }),
            _ => Err(Box::new(SqlParseError::InvalidInput(format!(
                "Expected where field, got {:?}",
                pair.as_rule()
            )))),
        }
    }
}
