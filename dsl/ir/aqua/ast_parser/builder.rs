use pest::iterators::Pairs;
use super::ast_structure::*;
use super::error::AquaParseError;
use super::{
    sink::SinkParser,
    source::SourceParser,
    condition::ConditionParser,
};
use crate::dsl::ir::aqua::ast_parser::Rule;

pub struct AquaASTBuilder;

impl AquaASTBuilder {
    pub fn build_ast_from_pairs(pairs: Pairs<Rule>) -> Result<AquaAST, AquaParseError> {
        let mut from = None;
        let mut select = None;
        let mut filter = None;

        // Process each clause in the query
        for pair in pairs {
            match pair.as_rule() {
                Rule::query => {
                    for clause in pair.into_inner() {
                        match clause.as_rule() {
                            Rule::from_clause => {
                                from = Some(SourceParser::parse(clause)?);
                            }
                            Rule::select_clause => {
                                select = Some(SinkParser::parse(clause)?);
                            }
                            Rule::where_clause => {
                                filter = Some(ConditionParser::parse(clause)?);
                            }
                            Rule::EOI => {}
                            _ => return Err(AquaParseError::InvalidInput(
                                format!("Unexpected clause: {:?}", clause.as_rule())
                            )),
                        }
                    }
                }
                _ => return Err(AquaParseError::InvalidInput("Expected query".to_string())),
            }
        }

        // Create and validate the AST
        let ast = AquaAST {
            from: from.ok_or_else(|| 
                AquaParseError::InvalidInput("Missing FROM clause".to_string())
            )?,
            select: select.ok_or_else(|| 
                AquaParseError::InvalidInput("Missing SELECT clause".to_string())
            )?,
            filter,
        };

        // Validate the AST
        Self::validate_ast(&ast)?;

        Ok(ast)
    }

    pub fn validate_ast(ast: &AquaAST) -> Result<(), AquaParseError> {
        // Validate SELECT clause
        match &ast.select {
            SelectClause::Column(col_ref) => {
                Self::validate_field_reference(col_ref, &ast.from)?;
            }
            SelectClause::Aggregate(agg_func) => {
                Self::validate_field_reference(&agg_func.column, &ast.from)?;
            }
            SelectClause::ComplexValue(col_ref, _, _) => {
                Self::validate_field_reference(col_ref, &ast.from)?;
            }
        }

        // Validate stream references in joins match declared streams
        if let Some(ref join) = ast.from.join {
            let main_stream = &ast.from.scan.stream_name;
            let main_alias = ast.from.scan.alias.as_ref().unwrap_or(main_stream);
            let joined_stream = &join.scan.stream_name;
            let joined_alias = join.scan.alias.as_ref().unwrap_or(joined_stream);
    
            let left_stream = join.condition.left_col.table.as_ref()
                .ok_or_else(|| AquaParseError::InvalidInput(
                    "Join condition must use fully qualified field names".to_string()
                ))?;
            let right_stream = join.condition.right_col.table.as_ref()
                .ok_or_else(|| AquaParseError::InvalidInput(
                    "Join condition must use fully qualified field names".to_string()
                ))?;
    
            if !((left_stream == main_stream || left_stream == main_alias) && 
                 (right_stream == joined_stream || right_stream == joined_alias) ||
                (left_stream == joined_stream || left_stream == joined_alias) && 
                 (right_stream == main_stream || right_stream == main_alias)) {
                return Err(AquaParseError::InvalidInput(
                    format!("Join condition references invalid streams: {}.{} = {}.{}",
                        left_stream,
                        join.condition.left_col.column,
                        right_stream,
                        join.condition.right_col.column
                    )
                ));
            }
        }

        // Validate filter conditions if present
        if let Some(ref filter) = ast.filter {
            Self::validate_where_clause(filter, &ast.from)?;
        }

        Ok(())
    }

    fn validate_field_reference(col_ref: &ColumnRef, from_clause: &FromClause) -> Result<(), AquaParseError> {
        if let Some(ref stream) = col_ref.table {
            // Check against both table names and aliases
            let scan_valid = stream == &from_clause.scan.stream_name 
                || stream == from_clause.scan.alias.as_ref().unwrap_or(&from_clause.scan.stream_name);

            let join_valid = from_clause.join.as_ref().map_or(false, |join| {
                stream == &join.scan.stream_name 
                || stream == join.scan.alias.as_ref().unwrap_or(&join.scan.stream_name)
            });

            if !scan_valid && !join_valid {
                return Err(AquaParseError::InvalidInput(
                    format!("Reference to undefined stream: {} (available streams: {}, {})", 
                        stream,
                        from_clause.scan.stream_name,
                        from_clause.join.as_ref().map_or("".to_string(), |j| j.scan.stream_name.clone())
                    )
                ));
            }
        }
        Ok(())
    }

    fn validate_where_clause(clause: &WhereClause, from_clause: &FromClause) -> Result<(), AquaParseError> {
        // Validate the current condition
        Self::validate_field_reference(&clause.condition.variable, from_clause)?;
        
        match &clause.condition.value {
            AquaLiteral::ColumnRef(ref col_ref) => {
                Self::validate_field_reference(col_ref, from_clause)?;
            },
            AquaLiteral::Boolean(_) => {}, // Boolean literals are always valid
            AquaLiteral::Integer(_) => {}, // Integer literals are always valid
            AquaLiteral::Float(_) => {}, // Float literals are always valid
            AquaLiteral::String(_) => {}, // String literals are always valid
        }

        // Recursively validate next condition if it exists
        if let Some(ref next) = clause.next {
            Self::validate_where_clause(next, from_clause)?;
        }

        Ok(())
    }
}