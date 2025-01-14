use pest::Parser;
use pest_derive::Parser;
use pest::iterators::Pair;

#[derive(Parser)]
#[grammar = "dsl/ir/aqua/grammar.pest"]
pub struct AquaParser;

// Main AST structure for a query
#[derive(Debug, PartialEq, Clone)]
pub struct AquaAST {
    // Every query starts with a stream
    pub from: FromClause,
    // Selection (required) - either column or aggregation
    pub select: SelectClause,
    // Optional filtering condition
    pub filter: Option<WhereClause>,
}

// Stream source definition
#[derive(Debug, PartialEq, Clone)]
pub struct FromClause {
    pub stream_name: String,
}

// Select clause can be either a simple column or an aggregation
#[derive(Debug, PartialEq, Clone)]
pub enum SelectClause {
    Column(String),              // Simple column selection
    Aggregate(AggregateFunction), // Aggregation function
    ComplexOp(String, String, String),
    ComplexValue(String, String, AquaLiteral),
}

// Aggregation function with its column
#[derive(Debug, PartialEq, Clone)]
pub struct AggregateFunction {
    pub function: AggregateType,
    pub column: String,
}

// Available aggregation functions
#[derive(Debug, PartialEq, Clone)]
pub enum AggregateType {
    Max,
    Min,
    Avg,
}

#[derive(Debug, PartialEq, Clone)]
pub struct WhereClause {
    pub condition: Condition,
    pub binary_op: Option<BinaryOp>,
    pub next: Option<Box<WhereClause>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Condition {
    pub variable: String,
    pub operator: ComparisonOp,
    pub value: AquaLiteral,
}

// Available comparison operators
#[derive(Debug, PartialEq, Clone)]
pub enum ComparisonOp {
    GreaterThan,
    LessThan,
    Equal,
    NotEqual,
    GreaterThanEquals,
    LessThanEquals,
}

#[derive(Debug, PartialEq, Clone)]
pub enum BinaryOp {
    And,
    Or,
}

#[derive(Debug, PartialEq, Clone)]
pub enum AquaLiteral {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
}

impl AquaAST {
    pub fn parse(input: &str) -> Result<Self, pest::error::Error<Rule>> {
        let pairs = AquaParser::parse(Rule::query, input)?;
        let mut from = None;
        let mut select = None;
        let mut filter = None;

        // Parse the query pairs
        for pair in pairs.into_iter().next().unwrap().into_inner() {
            match pair.as_rule() {
                Rule::from_clause => {
                    from = Some(FromClause::parse(pair)?);
                }
                Rule::select_clause => {
                    select = Some(SelectClause::parse(pair)?);
                }
                Rule::where_clause => {
                    filter = Some(WhereClause::parse(pair)?);
                }
                Rule::EOI => {}
                _ => {}
            }
        }

        // If no explicit select clause, default to selecting all
        if select.is_none() {
            select = Some(SelectClause::Column("*".to_string()));
        }

        Ok(AquaAST {
            from: from.ok_or_else(|| pest::error::Error::new_from_pos(
                pest::error::ErrorVariant::CustomError {
                    message: "Missing FROM clause".to_string(),
                },
                pest::Position::from_start(""),
            ))?,
            select: select.unwrap_or_else(|| SelectClause::Column("*".to_string())),
            filter,
        })
    }
}

impl FromClause {
    fn parse(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        let mut stream_name = None;

        for inner_pair in pair.into_inner() {
            if let Rule::stream_identifier = inner_pair.as_rule() {
                stream_name = Some(inner_pair.as_str().to_string());
            }
        }

        Ok(FromClause {
            stream_name: stream_name.ok_or_else(|| pest::error::Error::new_from_pos(
                pest::error::ErrorVariant::CustomError {
                    message: "Missing stream identifier".to_string(),
                },
                pest::Position::from_start(""),
            ))?,
        })
    }
}

impl SelectClause {
    fn parse(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::aggregate_expr => {
                    let mut inner = inner_pair.into_inner();
                    let func = inner.next().unwrap();
                    let column = inner.next().unwrap();
                    
                    return Ok(SelectClause::Aggregate(AggregateFunction {
                        function: AggregateType::from_str(func.as_str())?,
                        column: column.as_str().to_string(),
                    }));
                }
                Rule::identifier => {
                    return Ok(SelectClause::Column(inner_pair.as_str().to_string()));
                }
                Rule::complex_op => {
                    let mut inner = inner_pair.into_inner();
                    let variable = inner.next().unwrap().as_str().to_string();
                    let operator = inner.next().unwrap().as_str().to_string();
                    let var2 = inner.next().unwrap().as_str().to_string();
                    let literal = parse_literal(&var2);

                    return Ok(SelectClause::ComplexValue(variable, operator, literal));
                    
                    }
                    _ => unreachable!(),
                }
            }

            Err(pest::error::Error::new_from_pos(
                pest::error::ErrorVariant::CustomError {
                    message: "Invalid SELECT clause".to_string(),
                },
                pest::Position::from_start(""),
            ))
    }
}


impl WhereClause {
    fn parse(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        let mut inner = pair.into_inner();
        let where_conditions = inner.next().unwrap();
        
        Self::parse_where_conditions(where_conditions)
    }

    fn parse_where_conditions(conditions_pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        let mut pairs = conditions_pair.into_inner().peekable();
        
        // Parse first condition
        let first_condition = pairs.next().unwrap();
        let mut result = WhereClause {
            condition: Condition::parse(first_condition)?,
            binary_op: None,
            next: None,
        };
        
        let mut current = &mut result;
        
        // Process remaining operators and conditions sequentially
        while let Some(op_pair) = pairs.next() {
            if let Some(cond_pair) = pairs.next() {
                let op = match op_pair.as_str().to_uppercase().as_str() {
                    "AND" => BinaryOp::And,
                    "OR" => BinaryOp::Or,
                    _ => {
                        return Err(pest::error::Error::new_from_pos(
                            pest::error::ErrorVariant::CustomError {
                                message: format!("Invalid binary operator: {}", op_pair.as_str()),
                            },
                            pest::Position::from_start(""),
                        ))
                    }
                };
                
                current.binary_op = Some(op);
                current.next = Some(Box::new(WhereClause {
                    condition: Condition::parse(cond_pair)?,
                    binary_op: None,
                    next: None,
                }));
                
                if let Some(ref mut next) = current.next {
                    current = next;
                }
            }
        }
        
        Ok(result)
    }
}

impl Condition {
    fn parse(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        let mut inner = pair.into_inner();
        let variable = inner.next().unwrap().as_str().to_string();
        let operator = ComparisonOp::from_str(inner.next().unwrap().as_str())?;
        let value = inner.next().unwrap().as_str();
        let literal = parse_literal(value);

        Ok(Condition {
            variable,
            operator,
            value: literal,
        })
    }
}

impl ComparisonOp {
    fn from_str(s: &str) -> Result<Self, pest::error::Error<Rule>> {
        match s {
            ">" => Ok(ComparisonOp::GreaterThan),
            "<" => Ok(ComparisonOp::LessThan),
            "==" => Ok(ComparisonOp::Equal),
            ">=" => Ok(ComparisonOp::GreaterThanEquals),
            "<=" => Ok(ComparisonOp::LessThanEquals),
            "!=" => Ok(ComparisonOp::NotEqual),
            _ => Err(pest::error::Error::new_from_pos(
                pest::error::ErrorVariant::CustomError {
                    message: format!("Invalid comparison operator: {}", s),
                },
                pest::Position::from_start(""),
            )),
        }
    }
}

impl AggregateType {
    fn from_str(s: &str) -> Result<Self, pest::error::Error<Rule>> {
        match s {
            "max" => Ok(AggregateType::Max),
            "min" => Ok(AggregateType::Min),
            "avg" => Ok(AggregateType::Avg),
            _ => Err(pest::error::Error::new_from_pos(
                pest::error::ErrorVariant::CustomError {
                    message: format!("Invalid aggregate function: {}", s),
                },
                pest::Position::from_start(""),
            )),
        }
    }
}

// function to parse the literal value
pub fn parse_literal(val: &str) -> AquaLiteral {
    if let Ok(float_val) = val.parse::<f64>() {
        AquaLiteral::Float(float_val)
    } else if let Ok(int_val) = val.parse::<i64>() {
        AquaLiteral::Integer(int_val)
    } else if let Ok(bool_val) = val.parse::<bool>() {
        AquaLiteral::Boolean(bool_val)
    } else {
        AquaLiteral::String(val.to_string())
    }
}