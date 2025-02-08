#[derive(Debug, PartialEq, Clone)]
pub struct SqlAST {
    pub select: Vec<SelectClause>,
    pub from: FromClause,
    pub filter: Option<WhereClause>,
    pub group_by: Option<GroupByClause>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectClause {
    pub selection: SelectType,
    pub alias: Option<String>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum SelectType {
    Simple(ColumnRef),
    Aggregate(AggregateFunction, ColumnRef),
    ComplexValue(ComplexField, String, ComplexField),
}


#[derive(Debug, PartialEq, Clone)]
pub struct ComplexField {
    pub column_ref: Option<ColumnRef>,
    pub literal: Option<SqlLiteral>,
    pub aggregate: Option<(AggregateFunction, ColumnRef)>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum AggregateFunction {
    Max,
    Min,
    Avg,
    Count,
    Sum,
}

#[derive(Debug, PartialEq, Clone)] 
pub struct FromClause {
    pub scan: ScanClause,
    pub joins: Option<Vec<JoinClause>>,
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
pub struct  GroupByClause {
    pub columns: Vec<ColumnRef>,
    pub having: Option<HavingClause>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct  HavingClause {
    pub condition: HavingCondition,
    pub binary_op: Option<BinaryOp>,
    pub next: Option<Box<HavingClause>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Condition {
    pub left_field: WhereField,
    pub operator: ComparisonOp,
    pub right_field: WhereField,
}

#[derive(Debug, PartialEq, Clone)]
pub struct HavingCondition {
    pub left_field: HavingField,
    pub operator: ComparisonOp,
    pub right_field: HavingField,
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
}

#[derive(Debug, PartialEq, Clone)]
pub struct WhereField {
    pub column: Option<ColumnRef>,
    pub value: Option<SqlLiteral>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct HavingField {
    pub column: Option<ColumnRef>,
    pub value: Option<SqlLiteral>,
    pub aggregate: Option<(AggregateFunction, ColumnRef)>,
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
