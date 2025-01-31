#[derive(Debug, PartialEq, Clone)]
pub struct AquaAST {
    pub from: FromClause,
    pub select: Vec<SelectClause>,
    pub filter: Option<WhereClause>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct FromClause {
    pub scan: ScanClause,
    pub joins: Option<Vec<JoinClause>>,
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
    Column(ColumnRef, Option<String>),  // Added Option<String> for alias
    Aggregate(AggregateFunction, Option<String>),  // Added Option<String> for alias 
    ComplexValue(ColumnRef, String, AquaLiteral, Option<String>),  // Added Option<String> for alias
}

#[derive(Debug, PartialEq, Clone)]
pub struct AggregateFunction {
    pub function: AggregateType,
    pub column: ColumnRef,
}

#[derive(Debug, PartialEq, Clone, Eq, Hash)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub column: String,
}

#[derive(Debug, PartialEq, Clone)]
pub enum AggregateType {
    Max,
    Min,
    Avg,
    Count,
    Sum,
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

impl ColumnRef {
    pub fn to_string(&self) -> String {
        match &self.table {
            Some(table) => format!("{}.{}", table, self.column),
            None => self.column.clone(),
        }
    }
}