// New IrAST structure following Polars approach
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum IrPlan {
    // Source operations
    Scan {
        input: Arc<IrPlan>,
        stream_name: String,
        alias: Option<String>,
    },

    // Transformation operations
    Filter {
        input: Arc<IrPlan>,
        predicate: FilterClause,
    },

    Project {
        input: Arc<IrPlan>,
        columns: Vec<ProjectionColumn>,
        distinct: bool,
    },

    GroupBy {
        input: Arc<IrPlan>,
        keys: Vec<ColumnRef>,
        group_condition: Option<GroupClause>,
    },

    Join {
        left: Arc<IrPlan>,
        right: Arc<IrPlan>,
        condition: Vec<JoinCondition>,
        join_type: JoinType,
    },

    OrderBy {
        input: Arc<IrPlan>,
        items: Vec<OrderByItem>,
    },

    Limit {
        input: Arc<IrPlan>,
        limit: i64,
        offset: Option<i64>,
    },
    Table {
        table_name: String,
    },
}

// Supporting structures
#[derive(Debug, PartialEq, Clone)]
pub enum FilterClause {
    Base(FilterConditionType),
    Expression {
        left: Box<FilterClause>,
        binary_op: BinaryOp,
        right: Box<FilterClause>,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum FilterConditionType {
    Comparison(Condition),
    NullCheck(NullCondition),
    In(InCondition),
    Exists(ExistsCondition),
    Boolean(bool),
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
    In(InCondition),
    Exists(ExistsCondition),
    Boolean(bool),
}

#[derive(Debug, PartialEq, Clone)]
pub struct JoinCondition {
    pub left_col: ColumnRef,
    pub right_col: ColumnRef,
}

#[derive(Debug, PartialEq, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Outer,
}

#[derive(Debug, PartialEq, Clone)]
pub enum ProjectionColumn {
    Column(ColumnRef, Option<String>),
    Aggregate(AggregateFunction, Option<String>),
    ComplexValue(ComplexField, Option<String>),
    StringLiteral(String, Option<String>),
    Subquery(Arc<IrPlan>, Option<String>),
    SubqueryVec(String, Option<String>), // name of the result vec and optional alias
}

#[derive(Debug, PartialEq, Clone, Eq, Hash)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub column: String,
}

#[derive(Debug, PartialEq, Clone, Eq, Hash)]
pub struct AggregateFunction {
    pub function: AggregateType,
    pub column: ColumnRef,
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
pub struct ComplexField {
    pub column_ref: Option<ColumnRef>,
    pub literal: Option<IrLiteral>,
    pub aggregate: Option<AggregateFunction>,
    pub nested_expr: Option<Box<(ComplexField, String, ComplexField, bool)>>, //bool true if parenthesized
    pub subquery: Option<Arc<IrPlan>>,
    pub subquery_vec: Option<(String, String)>, // <name, type>
}

#[derive(Debug, PartialEq, Clone)]
pub enum IrLiteral {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderByItem {
    pub column: ColumnRef,
    pub direction: OrderDirection,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum OrderDirection {
    Asc,
    Desc,
}

// Additional structures for conditions
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
pub enum InCondition {
    Subquery {
        field: ComplexField,
        subquery: Arc<IrPlan>,
        negated: bool,
    },
    Vec {
        field: ComplexField,
        vector_name: String,
        vector_type: String,
        negated: bool,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum ExistsCondition {
    Subquery {
        subquery: Arc<IrPlan>,
        negated: bool,
    },
    Vec {
        vector_name: String,
        negated: bool,
    },
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
pub enum NullOp {
    IsNull,
    IsNotNull,
}

#[derive(Debug, PartialEq, Clone)]
pub enum BinaryOp {
    And,
    Or,
}

// Implementation of helper methods for the new structure
impl IrPlan {
    // Convenience method to create a filter operation
    pub(crate) fn filter(input: Arc<IrPlan>, predicate: FilterClause) -> Self {
        IrPlan::Filter { input, predicate }
    }

    // Convenience method to create a project operation
    pub(crate) fn project(
        input: Arc<IrPlan>,
        columns: Vec<ProjectionColumn>,
        distinct: bool,
    ) -> Self {
        IrPlan::Project {
            input,
            columns,
            distinct,
        }
    }

    // Similar convenience methods for other operations
    pub(crate) fn group_by(
        input: Arc<IrPlan>,
        keys: Vec<ColumnRef>,
        group_condition: Option<GroupClause>,
    ) -> Self {
        IrPlan::GroupBy {
            input,
            keys,
            group_condition,
        }
    }

    pub(crate) fn order_by(input: Arc<IrPlan>, items: Vec<OrderByItem>) -> Self {
        IrPlan::OrderBy { input, items }
    }

    pub(crate) fn limit(input: Arc<IrPlan>, limit: i64, offset: Option<i64>) -> Self {
        IrPlan::Limit {
            input,
            limit,
            offset,
        }
    }
}

//implement display for ColumnRef
impl std::fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(ref table) = self.table {
            write!(f, "{}.{}", table, self.column)
        } else {
            write!(f, "{}", self.column)
        }
    }
}

//implement display for AggregateType
impl std::fmt::Display for AggregateType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            AggregateType::Max => write!(f, "max"),
            AggregateType::Min => write!(f, "min"),
            AggregateType::Avg => write!(f, "avg"),
            AggregateType::Sum => write!(f, "sum"),
            AggregateType::Count => write!(f, "count"),
        }
    }
}

//implement display for ComplexField
impl std::fmt::Display for ComplexField {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(ref nested) = self.nested_expr {
            let (left, op, right, is_par) = &**nested;
            write!(f, "{}{} {} {}{}", is_par, left, op, right, is_par)
        } else if let Some(ref col) = self.column_ref {
            write!(f, "{}", col)
        } else if let Some(ref lit) = self.literal {
            match lit {
                IrLiteral::Integer(i) => write!(f, "{}", i),
                IrLiteral::Float(fl) => write!(f, "{:.2}", fl),
                IrLiteral::String(s) => write!(f, "{}", s.clone()),
                IrLiteral::Boolean(b) => write!(f, "{}", b),
            }
        } else if let Some(ref agg) = self.aggregate {
            write!(
                f,
                "{}({})",
                match agg.function {
                    AggregateType::Max => "max",
                    AggregateType::Min => "min",
                    AggregateType::Avg => "avg",
                    AggregateType::Sum => "sum",
                    AggregateType::Count => "count",
                },
                agg.column
            )
        } else {
            write!(f, "")
        }
    }
}
