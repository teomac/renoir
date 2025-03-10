#[derive(Debug, PartialEq, Clone)]
pub struct IrAST {
    pub from: FromClause,
    pub select: SelectClause,
    pub filter: Option<WhereClause>,
    pub group_by: Option<Group>,
    pub order_by: Option<OrderByClause>,
    pub limit: Option<LimitClause>,
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
    pub join_type: JoinType,
    pub join_scan: ScanClause,
    pub condition: JoinCondition,
}

#[derive(Debug, PartialEq, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Outer,
}

#[derive(Debug, PartialEq, Clone)]
pub struct JoinCondition {
    pub conditions: Vec<JoinPair>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct JoinPair {
    pub left_col: ColumnRef,
    pub right_col: ColumnRef,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectClause {
    pub distinct: bool,
    pub select: Vec<SelectColumn>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum SelectColumn {
    Column(ColumnRef, Option<String>), // Added Option<String> for alias
    Aggregate(AggregateFunction, Option<String>), // Added Option<String> for alias
    ComplexValue(ComplexField, Option<String>), // Added Option<String> for alias
}

#[derive(Debug, PartialEq, Clone)]
pub struct ComplexField {
    pub column_ref: Option<ColumnRef>,
    pub literal: Option<IrLiteral>,
    pub aggregate: Option<AggregateFunction>,
    pub nested_expr: Option<Box<(ComplexField, String, ComplexField)>>,
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

#[derive(Debug, PartialEq, Clone, Eq, Hash)]
pub enum AggregateType {
    Max,
    Min,
    Avg,
    Count,
    Sum,
}

#[derive(Debug, PartialEq, Clone)]
pub enum WhereClause {
    Base(WhereConditionType),
    Expression {
        left: Box<WhereClause>,
        binary_op: BinaryOp,
        right: Box<WhereClause>,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum WhereConditionType {
    Comparison(Condition),
    NullCheck(NullCondition),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Group {
    pub columns: Vec<ColumnRef>,
    pub group_condition: Option<GroupClause>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum GroupClause {
    Base(GroupBaseCondition),
    Expression {
        left: Box<GroupClause>,
        op: BinaryOp,
        right: Box<GroupClause>,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum GroupBaseCondition {
    Comparison(Condition),
    NullCheck(NullCondition),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Condition {
    pub left_field: ComplexField,
    pub operator: ComparisonOp,
    pub right_field: ComplexField,
}

#[derive(Debug, PartialEq, Clone)]
pub struct NullCondition {
    pub field: ComplexField,
    pub operator: NullOp,
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
pub enum NullOp {
    IsNull,
    IsNotNull,
}

#[derive(Debug, PartialEq, Clone)]
pub enum IrLiteral {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    ColumnRef(ColumnRef),
}

#[derive(Debug, PartialEq, Clone)]
pub struct LimitClause {
    pub limit: i64,
    pub offset: Option<i64>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderByClause {
    pub items: Vec<OrderByItem>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderByItem {
    pub column: ColumnRef,
    pub direction: OrderDirection,
}

#[derive(Debug, PartialEq, Clone)]
pub enum OrderDirection {
    Asc,
    Desc,
}

impl ColumnRef {
    pub fn to_string(&self) -> String {
        match &self.table {
            Some(table) => format!("{}.{}", table, self.column),
            None => self.column.clone(),
        }
    }
}

impl AggregateType {
    pub fn to_string(&self) -> String {
        match self {
            AggregateType::Max => "max".to_string(),
            AggregateType::Min => "min".to_string(),
            AggregateType::Avg => "avg".to_string(),
            AggregateType::Sum => "sum".to_string(),
            AggregateType::Count => "count".to_string(),
        }
    }
}

impl ComplexField {
    pub fn to_string(&self) -> String {
        if let Some(ref nested) = self.nested_expr {
            let (left, op, right) = &**nested;
            format!("({} {} {})", left.to_string(), op, right.to_string())
        } else if let Some(ref col) = self.column_ref {
            col.to_string()
        } else if let Some(ref lit) = self.literal {
            match lit {
                IrLiteral::Integer(i) => i.to_string(),
                IrLiteral::Float(f) => format!("{:.2}", f),
                IrLiteral::String(s) => s.clone(),
                IrLiteral::Boolean(b) => b.to_string(),
                IrLiteral::ColumnRef(cr) => cr.to_string(),
            }
        } else if let Some(ref agg) = self.aggregate {
            format!(
                "{}({})",
                match agg.function {
                    AggregateType::Max => "max",
                    AggregateType::Min => "min",
                    AggregateType::Avg => "avg",
                    AggregateType::Sum => "sum",
                    AggregateType::Count => "count",
                },
                agg.column.to_string()
            )
        } else {
            String::new()
        }
    }
}
