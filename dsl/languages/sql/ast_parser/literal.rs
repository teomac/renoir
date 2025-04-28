use super::error::SqlParseError;
use super::sql_ast_structure::SqlLiteral;

pub struct LiteralParser;

impl LiteralParser {
    pub(crate) fn parse(val: &str) -> Result<SqlLiteral, Box<SqlParseError>> {
        if let Ok(int_val) = val.parse::<i64>() {
            Ok(SqlLiteral::Integer(int_val))
        } else if let Ok(float_val) = val.parse::<f64>() {
            Ok(SqlLiteral::Float(float_val))
        } else if let Ok(bool_val) = val.parse::<bool>() {
            Ok(SqlLiteral::Boolean(bool_val))
        } else {
            Ok(SqlLiteral::String(val.to_string()))
        }
    }
}
