#[derive(Debug, PartialEq, Clone)]
pub struct SqlAST {
    pub select: Vec<SelectClause>,
    pub from: FromClause,
    pub filter: Option<WhereClause>, // Made optional
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectClause {
    pub selection: SelectType,
}

#[derive(Debug, PartialEq, Clone)]
pub enum SelectType {
    Simple(ColumnRef),
    Aggregate(AggregateFunction, ColumnRef),
    ComplexValue(ColumnRef, String, SqlLiteral),
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
    pub variable: ColumnRef,
    pub operator: ComparisonOp,
    pub value: SqlLiteral,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub column: String,
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
    //ColumnRef(ColumnRef), TODO
}

#[derive(Debug, PartialEq, Clone)]
pub enum BinaryOp {
    And,
    Or,
}

impl ColumnRef {
    pub fn to_string(&self) -> String {
        match &self.table {
            Some(table) => format!("{}.{}", table, self.column),
            None => self.column.clone(),
        }
    }
}