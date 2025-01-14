use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "dsl/parsers/sql/grammar.pest"]
pub struct SqlParser;

#[derive(Debug, PartialEq, Clone)]
pub struct SqlAST {
    pub select: SelectClause,
    pub from: FromClause,
    pub filter: Option<WhereClause>, // Made optional
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectClause {
    pub selection: SelectType,
}

#[derive(Debug, PartialEq, Clone)]
pub enum SelectType {
    Simple(String),
    Aggregate(AggregateFunction, String),
    ComplexValue(String, String, SqlLiteral),
}


#[derive(Debug, PartialEq, Clone)]
pub enum AggregateFunction {
    Max,
}

#[derive(Debug, PartialEq, Clone)] 
pub struct FromClause {
    pub scan: ScanClause,
    pub join: Option<JoinClause>,
}

#[derive(Debug, PartialEq, Clone)] 
pub struct ScanClause {
    pub variable: String,
    pub alias: Option<String>,
}

#[derive(Debug, PartialEq, Clone)] 
pub struct JoinClause {
    pub join_type: JoinType,
    pub join_scan: ScanClause,
    pub join_expr: JoinExpr,
}

#[derive(Debug, PartialEq, Clone)] 
pub enum JoinType {
    Inner,
    Left,
    LeftOuter,
}

#[derive(Debug, PartialEq, Clone)] 
pub struct JoinExpr {
    pub left_var: String,
    pub right_var: String,
}



#[derive(Debug, PartialEq, Clone)]
pub struct  WhereClause {
    pub condition: Condition,
    pub binary_op: Option<BinaryOp>,
    pub next: Option<Box<WhereClause>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Condition {
    pub variable: String,
    pub operator: ComparisonOp,
    pub value: SqlLiteral,
}

#[derive(Debug, PartialEq, Clone)]
pub enum ComparisonOp {
    GreaterThan,
    LessThan,
    GreaterOrEqualThan,
    LessOrEqualThan,
    Equal,
    NotEqual,
}

#[derive(Debug, PartialEq, Clone)]
pub enum SqlLiteral {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
}

#[derive(Debug, PartialEq, Clone)]
pub enum BinaryOp {
    And,
    Or,
}

impl SqlAST {
    pub fn parse(input: &str) -> Result<Self, pest::error::Error<Rule>> {
        let pairs = SqlParser::parse(Rule::query, input)?;
        
        for pair in pairs {
            if let Rule::query = pair.as_rule() {
                let mut inner = pair.into_inner();
                inner.next(); // Skip SELECT
                
                let select_part = inner.next().unwrap();
                let selection = match select_part.as_rule() {
                    Rule::variable => SelectType::Simple(select_part.as_str().to_string()),
                    Rule::aggregate_expr => {
                        let mut agg = select_part.into_inner();
                        let func = match agg.next().unwrap().as_str().to_uppercase().as_str() {
                            "MAX" => AggregateFunction::Max,
                            _ => unreachable!(),
                        };
                        let var = agg.next().unwrap().as_str().to_string();
                        SelectType::Aggregate(func, var)
                    },
                    Rule::select_expr => {
                        let mut complex = select_part.into_inner();
                        let var1 = complex.next().unwrap().as_str().to_string();
                        let op = complex.next().unwrap().as_str().to_string();
                        let val_str = complex.next().unwrap().as_str();
                        let literal = SqlAST::parse_literal(val_str);
                        SelectType::ComplexValue(var1, op, literal)
                        
                    },
                    _ => unreachable!(),
                };

                let from_expr = inner.next().unwrap();
                println!("from_expr: {:?}", from_expr);
                let from_clause = Self::parse_from_clause(from_expr)?;
                
                // Handle optional where expression
                let filter = match inner.next() {
                    Some(where_pair) if where_pair.as_rule() == Rule::where_expr => {
                        let mut where_inner = where_pair.into_inner();
                        where_inner.next();
                        let conditions = where_inner.next().unwrap(); 
                        
                        // Parse the conditions chain
                        Some(Self::parse_where_conditions(conditions))
                    },
                    _ => None
                };

                return Ok(SqlAST {
                    select: SelectClause { selection },
                    from: from_clause,
                    filter,
                });
            }
        }
        unreachable!()
    }

fn parse_from_clause(pair: pest::iterators::Pair<Rule>) -> Result<FromClause, pest::error::Error<Rule>> {
    let mut inner = pair.into_inner();

    let _ = inner.next(); // Skip 'FROM'
    
    // Get and parse the scan expression (the first table)
    let scan_expr = inner.next().unwrap();
    if scan_expr.as_rule() != Rule::scan_expr {
        return Err(pest::error::Error::new_from_pos(
            pest::error::ErrorVariant::CustomError {
                message: format!("Expected scan_expr, got {:?}", scan_expr.as_rule()),
            },
            pest::Position::from_start(""),
        ));
    }
    let scan = Self::parse_scan_clause(scan_expr)?;
    
    // Look for JOIN expression
    let join = if let Some(join_expr) = inner.next() {
        if join_expr.as_rule() == Rule::join_expr {
            Some(Self::parse_join_clause(join_expr)?)
        } else {
            None
        }
    } else {
        None
    };

    Ok(FromClause { scan, join })
}

// Also update parse_join_clause to handle the join parsing better
fn parse_join_clause(pair: pest::iterators::Pair<Rule>) -> Result<JoinClause, pest::error::Error<Rule>> {
    let mut inner = pair.into_inner();
    
    // Skip the JOIN keyword if it's there
    match inner.next() {
        Some(token) if token.as_rule() == Rule::join => {},
        _ => return Err(pest::error::Error::new_from_pos(
            pest::error::ErrorVariant::CustomError {
                message: "Expected JOIN keyword".to_string(),
            },
            pest::Position::from_start(""),
        )),
    }

    // Parse the table to join with
    let scan_expr = inner.next().ok_or_else(|| pest::error::Error::new_from_pos(
        pest::error::ErrorVariant::CustomError {
            message: "Missing join table specification".to_string(),
        },
        pest::Position::from_start(""),
    ))?;
    let join_scan = Self::parse_scan_clause(scan_expr)?;

    // Skip ON keyword
    match inner.next() {
        Some(token) if token.as_rule() == Rule::on => {},
        _ => return Err(pest::error::Error::new_from_pos(
            pest::error::ErrorVariant::CustomError {
                message: "Expected ON keyword".to_string(),
            },
            pest::Position::from_start(""),
        )),
    }

    // Parse join condition
    let join_condition = inner.next().ok_or_else(|| pest::error::Error::new_from_pos(
        pest::error::ErrorVariant::CustomError {
            message: "Missing join condition".to_string(),
        },
        pest::Position::from_start(""),
    ))?;

    let mut condition_parts = join_condition.into_inner();
    
    // Parse the join condition components
    let left_table = condition_parts.next().unwrap().as_str().to_string();
    let left_column = condition_parts.next().unwrap().as_str().to_string();
    let right_table = condition_parts.next().unwrap().as_str().to_string();
    let right_column = condition_parts.next().unwrap().as_str().to_string();

    Ok(JoinClause {
        join_type: JoinType::Inner, // Default to inner join for now
        join_scan,
        join_expr: JoinExpr {
            left_var: format!("{}.{}", left_table, left_column),
            right_var: format!("{}.{}", right_table, right_column),
        }
    })
}

    fn parse_scan_clause(pair: pest::iterators::Pair<Rule>) -> Result<ScanClause, pest::error::Error<Rule>> {
        let mut inner = pair.into_inner();
     
        let variable = inner.next()
            .ok_or_else(|| pest::error::Error::new_from_pos(
                pest::error::ErrorVariant::CustomError {
                    message: "Missing table name".to_string(),
                },
                pest::Position::from_start("")
            ))?
            .as_str()
            .to_string();
        
        // Process alias if present
        let mut alias = None;
        while let Some(next_token) = inner.next() {
            match next_token.as_rule() {
                Rule::as_keyword => {
                    if let Some(alias_token) = inner.next() {
                        alias = Some(alias_token.as_str().to_string());
                    }
                }
                Rule::variable => {
                    alias = Some(next_token.as_str().to_string());
                }
                _ => {}
            }
        }

        Ok(ScanClause { variable, alias })
    }



    fn parse_where_conditions(conditions_pair: pest::iterators::Pair<Rule>) -> WhereClause {
        let mut pairs = conditions_pair.into_inner().peekable();
        
        // Parse first condition
        let first_condition = pairs.next().unwrap();
        let current = WhereClause {
            condition: Self::parse_single_condition(first_condition),
            binary_op: None,
            next: None
        };
        
        // Keep track of the original clause to return
        let mut original = current.clone();
        let mut last = &mut original;
        
        // Process remaining pairs sequentially
        while let Some(op_pair) = pairs.next() {
            if let Some(condition_pair) = pairs.next() {
                let op = match op_pair.as_str().to_uppercase().as_str() {
                    "AND" => BinaryOp::And,
                    "OR" => BinaryOp::Or,
                    _ => unreachable!(),
                };
                
                let next_condition = Self::parse_single_condition(condition_pair);
                last.binary_op = Some(op);
                last.next = Some(Box::new(WhereClause {
                    condition: next_condition,
                    binary_op: None,
                    next: None,
                }));
                
                if let Some(ref mut next) = last.next {
                    last = next;
                }
            }
        }
        
        original
    }

    // Helper method to parse a single condition pair
    fn parse_single_condition(condition_pair: pest::iterators::Pair<Rule>) -> Condition {
        let mut inner = condition_pair.into_inner();
        
        let variable = inner.next().unwrap().as_str().to_string();
        let operator = match inner.next().unwrap().as_str() {
            ">" => ComparisonOp::GreaterThan,
            "<" => ComparisonOp::LessThan,
            ">=" => ComparisonOp::GreaterOrEqualThan,
            "<=" => ComparisonOp::LessOrEqualThan,
            "=" => ComparisonOp::Equal,
            "!=" | "<>" => ComparisonOp::NotEqual,
            op => panic!("Unexpected operator: {}", op),
        };
        let value_str = inner.next().unwrap().as_str();
        let value = SqlAST::parse_literal(value_str);

        Condition {
            variable,
            operator,
            value,
        }
    }

    pub fn to_aqua_string(&self) -> String {
        let mut parts = Vec::new();

        // FROM clause
        parts.push(format!("from input:Stream"));

        // WHERE clause (only if present)
        if let Some(where_clause) = &self.filter {
            parts.push(format!("where {}", Self::where_clause_to_string(where_clause)));
        }

        // SELECT clause
        match &self.select.selection {
            SelectType::Simple(column) => {
                parts.push(format!("select {}", column));
            },
            SelectType::Aggregate(func, column) => {
                let agg = match func {
                    AggregateFunction::Max => "max",
                };
                parts.push(format!("select {}({})", agg, column));
            }
            SelectType::ComplexValue(var1, op, val) => {
                let value = match val {
                    SqlLiteral::Float(val) => format!("{:.2}", val),
                    SqlLiteral::Integer(val) => val.to_string(),
                    SqlLiteral::String(val) => val.clone(),
                    SqlLiteral::Boolean(val) => val.to_string(),
                };
                parts.push(format!("select {} {} {}", var1, op, value));
            }
        }

        parts.join("\n")
    }

    // Helper method to convert where clause to string
    fn where_clause_to_string(clause: &WhereClause) -> String {
        let mut result = Self::condition_to_string(&clause.condition);
        
        if let (Some(op), Some(next)) = (&clause.binary_op, &clause.next) {
            let op_str = match op {
                BinaryOp::And => "AND",
                BinaryOp::Or => "OR",
            };
            result = format!("{} {} {}", result, op_str, Self::where_clause_to_string(next));
        }
        
        result
    }

    pub fn convert_operator(op: &ComparisonOp) -> &'static str {
        match op {
            ComparisonOp::GreaterThan => ">",
            ComparisonOp::LessThan => "<",
            ComparisonOp::GreaterOrEqualThan => ">=",
            ComparisonOp::LessOrEqualThan => "<=",
            ComparisonOp::Equal => "==",
            ComparisonOp::NotEqual => "!=",
        }
    }

    pub fn parse_literal(val: &str) -> SqlLiteral {
        if let Ok(float_val) = val.parse::<f64>() {
            SqlLiteral::Float(float_val)
        } else if let Ok(int_val) = val.parse::<i64>() {
            SqlLiteral::Integer(int_val)
        } else if let Ok(bool_val) = val.parse::<bool>() {
            SqlLiteral::Boolean(bool_val)
        } else {
            SqlLiteral::String(val.to_string())
        }
    }

    // Helper method to convert a single condition to string
    fn condition_to_string(condition: &Condition) -> String {
        let value_str = match &condition.value {
            SqlLiteral::Float(val) => format!("{:.2}", val),
            SqlLiteral::Integer(val) => val.to_string(),
            SqlLiteral::String(val) => val.clone(),
            SqlLiteral::Boolean(val) => val.to_string(),
        };

        format!("{} {} {}",
            condition.variable,
            Self::convert_operator(&condition.operator),
            value_str
        )
    }
}