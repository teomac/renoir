#[derive(Debug, PartialEq, Clone)]
pub struct SqlAST {
    pub select: SelectClause,
    pub from: FromClause,
    pub filter: Option<WhereClause>,
    pub group_by: Option<GroupByClause>,
    pub order_by: Option<OrderByClause>,
    pub limit: Option<LimitClause>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectClause {
    pub distinct: bool,
    pub select: Vec<SelectColumn>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectColumn {
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
    pub nested_expr: Option<Box<(ComplexField, String, ComplexField)>>,
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
    Outer,
}

#[derive(Debug, PartialEq, Clone)]
pub struct JoinExpr {
    pub conditions: Vec<JoinCondition>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct JoinCondition {
    pub left_var: String,
    pub right_var: String,
}

#[derive(Debug, PartialEq, Clone)]
pub enum WhereClause {
    Base(WhereBaseCondition),
    Expression {
        left: Box<WhereClause>,
        op: BinaryOp,
        right: Box<WhereClause>
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum WhereBaseCondition {
    Comparison(WhereCondition),
    NullCheck(WhereNullCondition)
}

#[derive(Debug, PartialEq, Clone)]
pub struct  GroupByClause {
    pub columns: Vec<ColumnRef>,
    pub having: Option<HavingClause>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum HavingClause {
    Base(HavingBaseCondition),
    Expression {
        left: Box<HavingClause>,
        op: BinaryOp,
        right: Box<HavingClause>
    }
}


#[derive(Debug, PartialEq, Clone)]
pub enum HavingBaseCondition {
    Comparison(HavingCondition),
    NullCheck(HavingNullCondition)
}

#[derive(Debug, PartialEq, Clone)]
pub struct WhereCondition {
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
pub struct WhereNullCondition {
    pub field: WhereField,
    pub operator: NullOp,
}

#[derive(Debug, PartialEq, Clone)]
pub struct HavingNullCondition {
    pub field: HavingField,
    pub operator: NullOp,
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
pub enum NullOp {
    IsNull,
    IsNotNull,
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
    pub arithmetic: Option<ArithmeticExpr>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum ArithmeticExpr {
    Column(ColumnRef),
    Literal(SqlLiteral),
    Aggregate(AggregateFunction, ColumnRef),
    BinaryOp(Box<ArithmeticExpr>, String, Box<ArithmeticExpr>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct HavingField {
    pub column: Option<ColumnRef>,
    pub value: Option<SqlLiteral>,
    pub aggregate: Option<(AggregateFunction, ColumnRef)>,
    pub arithmetic: Option<ArithmeticExpr>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum BinaryOp {
    And,
    Or,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderByClause {
    pub items: Vec<OrderByItem>
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderByItem {
    pub column: ColumnRef,
    pub direction: OrderDirection
}

#[derive(Debug, PartialEq, Clone)]
pub enum OrderDirection {
    Asc,
    Desc
}

#[derive(Debug, PartialEq, Clone)]
pub struct LimitClause {
    pub limit: i64,
    pub offset: Option<i64>,
}

impl ColumnRef {
    pub fn to_string(&self) -> String {
        match &self.table {
            Some(table) => format!("{}.{}", table, self.column),
            None => self.column.clone(),
        }
    }
}
