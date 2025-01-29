use super::ast_structure::*;
use super::error::AquaParseError;
use super::{condition::ConditionParser, sink::SinkParser, source::SourceParser};
use crate::dsl::ir::aqua::ast_parser::Rule;
use pest::iterators::Pairs;

pub struct AquaASTBuilder;

impl AquaASTBuilder {
    pub fn build_ast_from_pairs(pairs: Pairs<Rule>) -> Result<AquaAST, AquaParseError> {
        let mut from = None;
        let mut select = Vec::new();
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
                                select = SinkParser::parse(clause)?;
                            }
                            Rule::where_clause => {
                                filter = Some(ConditionParser::parse(clause)?);
                            }
                            Rule::EOI => {}
                            _ => {
                                return Err(AquaParseError::InvalidInput(format!(
                                    "Unexpected clause: {:?}",
                                    clause.as_rule()
                                )))
                            }
                        }
                    }
                }
                _ => return Err(AquaParseError::InvalidInput("Expected query".to_string())),
            }
        }

        // Create and validate the AST
        let ast = AquaAST {
            from: from
                .ok_or_else(|| AquaParseError::InvalidInput("Missing FROM clause".to_string()))?,
            select,
            filter,
        };

        // Validate the AST
        Self::validate_ast(&ast)?;

        Ok(ast)
    }

    pub fn validate_ast(ast: &AquaAST) -> Result<(), AquaParseError> {
        // Add validation to ensure there's at least one SELECT clause
        if ast.select.is_empty() {
            return Err(AquaParseError::InvalidInput(
                "Query must have at least one SELECT expression".to_string(),
            ));
        }

        // Validate all SELECT clauses
        for select_clause in &ast.select {
            match select_clause {
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
        }

        // Validate stream references in all joins
        let main_stream = &ast.from.scan.stream_name;
        let main_alias = ast.from.scan.alias.as_ref().unwrap_or(main_stream);

        if !ast.from.joins.is_none() {
            for join in &ast.from.joins.clone().unwrap() {
                let joined_stream = &join.scan.stream_name;
                let joined_alias = join.scan.alias.as_ref().unwrap_or(joined_stream);

                let left_stream = join.condition.left_col.table.as_ref().ok_or_else(|| {
                    AquaParseError::InvalidInput(
                        "Join condition must use fully qualified field names".to_string(),
                    )
                })?;
                let right_stream = join.condition.right_col.table.as_ref().ok_or_else(|| {
                    AquaParseError::InvalidInput(
                        "Join condition must use fully qualified field names".to_string(),
                    )
                })?;

                // For each join, check if the join condition references valid tables/aliases
                let valid_refs = (left_stream != right_stream)
                    && (right_stream == joined_stream || right_stream == joined_alias);

                if !valid_refs {
                    return Err(AquaParseError::InvalidInput(format!(
                        "Join condition references invalid streams: {}.{} = {}.{}",
                        left_stream,
                        join.condition.left_col.column,
                        right_stream,
                        join.condition.right_col.column
                    )));
                }
            }
        }

        // Validate filter conditions if present
        if let Some(ref filter) = ast.filter {
            Self::validate_where_clause(filter, &ast.from)?;
        }

        Ok(())
    }

    fn validate_field_reference(
        col_ref: &ColumnRef,
        from_clause: &FromClause,
    ) -> Result<(), AquaParseError> {
        if let Some(ref stream) = col_ref.table {
            // Check against main table name and alias
            let scan_valid = stream == &from_clause.scan.stream_name
                || stream
                    == from_clause
                        .scan
                        .alias
                        .as_ref()
                        .unwrap_or(&from_clause.scan.stream_name);
            
            if from_clause.joins.is_none() {
                if !scan_valid {
                    return Err(AquaParseError::InvalidInput(format!(
                        "Reference to undefined stream: {} (available streams: {})",
                        stream, from_clause.scan.stream_name
                    )));
                } else {
                    return Ok(());
                }
            }

            // Check against all joined table names and aliases
            let join_valid = from_clause.joins.clone().unwrap().iter().any(|join| {
                stream == &join.scan.stream_name
                    || stream == join.scan.alias.as_ref().unwrap_or(&join.scan.stream_name)
            });

            if !scan_valid && !join_valid {
                let available_streams = from_clause
                    .joins
                    .clone()
                    .unwrap()
                    .iter()
                    .map(|j| j.scan.stream_name.clone())
                    .collect::<Vec<_>>()
                    .join(", ");

                return Err(AquaParseError::InvalidInput(format!(
                    "Reference to undefined stream: {} (available streams: {}, {})",
                    stream, from_clause.scan.stream_name, available_streams
                )));
            }
        }
        Ok(())
    }

    fn validate_where_clause(
        clause: &WhereClause,
        from_clause: &FromClause,
    ) -> Result<(), AquaParseError> {
        // Validate the current condition
        Self::validate_field_reference(&clause.condition.variable, from_clause)?;

        match &clause.condition.value {
            AquaLiteral::ColumnRef(ref col_ref) => {
                Self::validate_field_reference(col_ref, from_clause)?;
            }
            // Other literal types are always valid
            _ => {}
        }

        // Recursively validate next condition if it exists
        if let Some(ref next) = clause.next {
            Self::validate_where_clause(next, from_clause)?;
        }

        Ok(())
    }
}
