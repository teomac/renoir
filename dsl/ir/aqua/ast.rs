use pest::Parser;
use pest_derive::Parser;
use pest::iterators::Pair;

#[derive(Parser)]
#[grammar = "dsl/ir/aqua/grammar.pest"]
pub struct AquaParser;

#[derive(Debug, PartialEq, Clone)]
pub struct AquaAST {
    pub from: FromClause,
    pub select: SelectClause,
    pub filter: Option<WhereClause>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct FromClause {
    pub scan: ScanClause,
    pub join: Option<JoinClause>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ScanClause {
    pub stream_name: String,
    pub alias: Option<String>,
    pub input_source: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct JoinClause {
    pub scan: ScanClause,
    pub condition: JoinCondition,
}

#[derive(Debug, PartialEq, Clone)]
pub struct JoinCondition {
    pub left_col: ColumnRef,
    pub right_col: ColumnRef,
}

#[derive(Debug, PartialEq, Clone)]
pub enum SelectClause {
    Column(ColumnRef),
    Aggregate(AggregateFunction),
    ComplexValue(ColumnRef, String, AquaLiteral),
}

#[derive(Debug, PartialEq, Clone)]
pub struct AggregateFunction {
    pub function: AggregateType,
    pub column: ColumnRef,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub column: String,
}

impl ColumnRef {
    pub fn to_string(&self) -> String {
        match &self.table {
            Some(table) => format!("{}.{}", table, self.column),
            None => self.column.clone(),
        }
    }

    fn parse(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        match pair.as_rule() {
            Rule::qualified_column => {
                let mut inner = pair.into_inner();
                let table = inner.next().unwrap().as_str().to_string();
                let column = inner.next().unwrap().as_str().to_string();
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
            _ => Err(pest::error::Error::new_from_pos(
                pest::error::ErrorVariant::CustomError {
                    message: format!("Expected column reference, got {:?}", pair.as_rule()),
                },
                pest::Position::from_start(""),
            )),
        }
    }
}

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
    pub variable: ColumnRef,
    pub operator: ComparisonOp,
    pub value: AquaLiteral,
}

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
    ColumnRef(ColumnRef),
}

impl AquaAST {
    pub fn parse(input: &str) -> Result<Self, pest::error::Error<Rule>> {
        let pairs = AquaParser::parse(Rule::query, input)?;
        let mut from = None;
        let mut select = None;
        let mut filter = None;

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

        Ok(AquaAST {
            from: from.ok_or_else(|| pest::error::Error::new_from_pos(
                pest::error::ErrorVariant::CustomError {
                    message: "Missing FROM clause".to_string(),
                },
                pest::Position::from_start(""),
            ))?,
            select: select.ok_or_else(|| pest::error::Error::new_from_pos(
                pest::error::ErrorVariant::CustomError {
                    message: "Missing SELECT clause".to_string(),
                },
                pest::Position::from_start(""),
            ))?,
            filter,
        })
    }
}

impl FromClause {
    fn parse(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        let mut inner = pair.into_inner();
        let mut scan = None;
        let mut join = None;

        // Parse base scan
        if let Some(scan_expr) = inner.next() {
            if scan_expr.as_rule() == Rule::scan_expr {
                scan = Some(ScanClause::parse(scan_expr)?);
            }
        }

        // Parse optional join
        while let Some(next_pair) = inner.next() {
            match next_pair.as_rule() {
                Rule::join => {
                    if let Some(join_scan) = inner.next() {
                        if join_scan.as_rule() == Rule::scan_expr {
                            let join_scan_clause = ScanClause::parse(join_scan)?;
                            
                            // Parse ON condition
                            if let Some(on_pair) = inner.next() {
                                if on_pair.as_rule() == Rule::on {
                                    if let Some(condition_pair) = inner.next() {
                                        let condition = JoinCondition::parse(condition_pair)?;
                                        join = Some(JoinClause {
                                            scan: join_scan_clause,
                                            condition,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(FromClause {
            scan: scan.ok_or_else(|| pest::error::Error::new_from_pos(
                pest::error::ErrorVariant::CustomError {
                    message: "Missing scan clause".to_string(),
                },
                pest::Position::from_start(""),
            ))?,
            join,
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
                    let col_ref = ColumnRef::parse(column)?;
                    
                    return Ok(SelectClause::Column(col_ref))
                }
                Rule::identifier | Rule::qualified_column => {
                    let col_ref = ColumnRef::parse(inner_pair)?;
                    return Ok(SelectClause::Column(col_ref))
                }
                Rule::complex_op => {
                    let mut inner = inner_pair.into_inner();
                    let var_pair = inner.next().unwrap();
                    let col_ref = ColumnRef::parse(var_pair)?;
                    let operator = inner.next().unwrap().as_str().to_string();
                    let value = inner.next().unwrap().as_str();
                    let literal = parse_literal(value);

                    return Ok(SelectClause::ComplexValue(col_ref, operator, literal))
                }
                _ => return  Err(pest::error::Error::new_from_pos(
                    pest::error::ErrorVariant::CustomError {
                        message: "Invalid SELECT clause".to_string(),
                    },
                    pest::Position::from_start(""),
                )),
            }
        }
        
        Err(pest::error::Error::new_from_pos(
            pest::error::ErrorVariant::CustomError {
                message: "Empty SELECT clause".to_string(),
            },
            pest::Position::from_start(""),
        ))
    }
}

impl ScanClause {
    fn parse(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        let mut inner = pair.into_inner();
        let mut stream_name = None;
        let mut alias = None;
        let mut input_source = None;

        while let Some(pair) = inner.next() {
            match pair.as_rule() {
                Rule::identifier => {
                    if stream_name.is_none() {
                        stream_name = Some(pair.as_str().to_string());
                    } else {
                        alias = Some(pair.as_str().to_string());
                    }
                }
                Rule::stream_input => {
                    input_source = Some(pair.as_str().to_string());
                }
                Rule::as_keyword => {}
                Rule::in_keyword => {}
                _ => {}
            }
        }

        Ok(ScanClause {
            stream_name: stream_name.ok_or_else(|| pest::error::Error::new_from_pos(
                pest::error::ErrorVariant::CustomError {
                    message: "Missing stream name".to_string(),
                },
                pest::Position::from_start(""),
            ))?,
            alias,
            input_source: input_source.ok_or_else(|| pest::error::Error::new_from_pos(
                pest::error::ErrorVariant::CustomError {
                    message: "Missing input source".to_string(),
                },
                pest::Position::from_start(""),
            ))?,
        })
    }
}

impl JoinCondition {
    fn parse(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        let mut inner = pair.into_inner();
        
        let left_pair = inner.next().unwrap();
        let right_pair = inner.next().unwrap();

        Ok(JoinCondition {
            left_col: ColumnRef::parse(left_pair)?,
            right_col: ColumnRef::parse(right_pair)?,
        })
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
        
        let first_condition = pairs.next().unwrap();
        let mut result = WhereClause {
            condition: Condition::parse(first_condition)?,
            binary_op: None,
            next: None,
        };
        
        let mut current = &mut result;
        
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
        
        let col_ref_pair = inner.next().unwrap();
        let variable = ColumnRef::parse(col_ref_pair)?;
        
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