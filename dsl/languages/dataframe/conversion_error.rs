use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConversionError {
    #[error("Empty Catalyst plan")]
    EmptyPlan,

    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Invalid group keys. Error: {0}")]
    InvalidGroupKeys(String),

    #[error("Invalid class name in Catalyst plan")]
    InvalidClassName,

    #[error("Invalid node index: {0}")]
    InvalidNodeIndex(String),

    #[error("Unsupported node type: {0}")]
    UnsupportedNodeType(String),

    #[error("Invalid expression structure in filter condition")]
    InvalidExpression,

    #[error("Unsupported expression type: {0}")]
    UnsupportedExpressionType(String),

    #[error("Error parsing integer: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("Error parsing join: invalid join type")]
    InvalidJoinType,

    #[error("Error parsing join, unsupported join type: {0}")]
    UnsupportedJoinType(String),
}

