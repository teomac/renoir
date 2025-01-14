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
    ComplexValue(String, char, SqlLiteral),
}


#[derive(Debug, PartialEq, Clone)]
pub enum AggregateFunction {
    Max,
}

#[derive(Debug, PartialEq, Clone)] 
pub struct FromClause {
    pub table: String,
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
    Equals,
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
    Xor,
    Nand,
    Nor,
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
                        let op = complex.next().unwrap().as_str().chars().next().unwrap();
                        let val_str = complex.next().unwrap().as_str();
                        let literal = SqlAST::parse_literal(val_str);
                        SelectType::ComplexValue(var1, op, literal)
                        
                    },
                    _ => unreachable!(),
                };
                
                inner.next(); // Skip FROM
                let table = inner.next().unwrap().as_str().to_string();
                
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

                //println!("select clause: {:?}", selection);

                return Ok(SqlAST {
                    select: SelectClause { selection },
                    from: FromClause { table },
                    filter,
                });
            }
        }
        unreachable!()
    }

    fn parse_where_conditions(conditions_pair: pest::iterators::Pair<Rule>) -> WhereClause {
        let mut pairs = conditions_pair.into_inner().peekable();
        
        // Parse first condition
        let first_condition = pairs.next().unwrap();
        let mut current = WhereClause {
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
                    "XOR" => BinaryOp::Xor,
                    "NAND" => BinaryOp::Nand,
                    "NOR" => BinaryOp::Nor,
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
            "=" => ComparisonOp::Equals,
            _ => unreachable!(),
        };
        let value_str = inner.next().unwrap().as_str();
        let value = SqlAST::parse_literal(value_str);

        Condition {
            variable,
            operator,
            value,
        }
    }

    // Helper to convert a single condition into a WhereClause
    fn parse_single_condition_to_where_clause(condition_pair: pest::iterators::Pair<Rule>) -> WhereClause {
        WhereClause {
            condition: Self::parse_single_condition(condition_pair),
            binary_op: None,
            next: None,
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
                    // Add other aggregates as needed
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

        // Join all parts with newlines
        parts.join("\n")
    }

    // Helper method to convert where clause to string
    fn where_clause_to_string(clause: &WhereClause) -> String {
        let mut result = Self::condition_to_string(&clause.condition);
        
        // If there's a binary operator and next condition, append them
        if let (Some(op), Some(next)) = (&clause.binary_op, &clause.next) {
            let op_str = match op {
                BinaryOp::And => "AND",
                BinaryOp::Or => "OR",
                BinaryOp::Xor => "XOR",
                BinaryOp::Nand => "NAND",
                BinaryOp::Nor => "NOR",
            };
            result = format!("{} {} {}", result, op_str, Self::where_clause_to_string(next));
        }
        
        result
    }

    pub fn convert_operator(op: &ComparisonOp) -> &'static str {
        match op {
            ComparisonOp::GreaterThan => ">",
            ComparisonOp::LessThan => "<",
            ComparisonOp::Equals => "==",
        }
    }

    // function to parse the literal value
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