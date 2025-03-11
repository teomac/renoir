use super::error::IrParseError;
use super::ir_ast_structure::IrLiteral;

pub struct LiteralParser;

impl LiteralParser {
    pub fn parse(val: &str) -> Result<IrLiteral, IrParseError> {
        // Try parsing as boolean first
        if val == "true" || val == "false" {
            return Ok(IrLiteral::Boolean(val == "true"));
        }
        // Try parsing as float since integers can be parsed as floats
        if let Ok(float_val) = val.parse::<f64>() {
            // Check if it's actually an integer
            if float_val.fract() == 0.0 {
                if let Ok(int_val) = val.parse::<i64>() {
                    return Ok(IrLiteral::Integer(int_val));
                }
            }
            Ok(IrLiteral::Float(float_val))
        } else if let Ok(bool_val) = val.parse::<bool>() {
            Ok(IrLiteral::Boolean(bool_val))
        } else {
            // Handle string literals - strip quotes if present
            let cleaned_val = val.trim_matches('"').trim_matches('\'').to_string();
            Ok(IrLiteral::String(cleaned_val))
        }
    }

    pub fn parse_ir_literal(lit: &IrLiteral) -> String {
        match lit {
            IrLiteral::Integer(i) => i.to_string(),
            IrLiteral::Float(f) => format!("{:.2}", f),
            IrLiteral::String(s) => s.to_string(),
            IrLiteral::Boolean(b) => b.to_string(),
            IrLiteral::ColumnRef(cr) => cr.to_string(),
        }
    }

    pub fn get_literal_type(lit: &IrLiteral) -> String {
        match lit {
            IrLiteral::Integer(_) => "i64".to_string(),
            IrLiteral::Float(_) => "f64".to_string(),
            IrLiteral::String(_) => "String".to_string(),
            IrLiteral::Boolean(_) => "bool".to_string(),
            IrLiteral::ColumnRef(_) => "ColumnRef".to_string(),
        }
    }

    pub fn parse_column_ref(column_ref: &str) -> Result<IrLiteral, IrParseError> {
        let parts: Vec<&str> = column_ref.split('.').collect();
        match parts.len() {
            1 => Ok(IrLiteral::ColumnRef(super::ir_ast_structure::ColumnRef {
                table: None,
                column: parts[0].to_string(),
            })),
            2 => Ok(IrLiteral::ColumnRef(super::ir_ast_structure::ColumnRef {
                table: Some(parts[0].to_string()),
                column: parts[1].to_string(),
            })),
            _ => Err(IrParseError::InvalidInput(format!(
                "Invalid column reference format: {}",
                column_ref
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_integer() {
        assert!(matches!(
            LiteralParser::parse("42"),
            Ok(IrLiteral::Integer(42))
        ));
    }

    #[test]
    fn test_parse_float() {
        assert!(matches!(
            LiteralParser::parse("42.5"),
            Ok(IrLiteral::Float(42.5))
        ));
    }

    #[test]
    fn test_parse_boolean() {
        assert!(matches!(
            LiteralParser::parse("true"),
            Ok(IrLiteral::Boolean(true))
        ));
        assert!(matches!(
            LiteralParser::parse("false"),
            Ok(IrLiteral::Boolean(false))
        ));
    }

    #[test]
    fn test_parse_string() {
        assert!(matches!(
            LiteralParser::parse("\"hello\""),
            Ok(IrLiteral::String(s)) if s == "hello"
        ));
    }

    #[test]
    fn test_parse_column_ref() {
        assert!(matches!(
            LiteralParser::parse_column_ref("table.column"),
            Ok(IrLiteral::ColumnRef(ref cr)) if cr.table == Some("table".to_string()) && cr.column == "column"
        ));
    }
}
