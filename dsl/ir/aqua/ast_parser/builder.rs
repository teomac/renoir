use super::ast_structure::*;
use super::error::AquaParseError;
use super::group::GroupParser;
use super::{condition::ConditionParser, sink::SinkParser, source::SourceParser};
use crate::dsl::ir::aqua::ast_parser::Rule;
use pest::iterators::Pairs;

pub struct AquaASTBuilder;

impl AquaASTBuilder {
    pub fn build_ast_from_pairs(pairs: Pairs<Rule>) -> Result<AquaAST, AquaParseError> {
        let mut from = None;
        let mut select = Vec::new();
        let mut filter = None;
        let mut group_by = None;

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
                            Rule::group_clause => {
                                group_by = Some(GroupParser::parse(clause)?);
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
            group_by,
        };

        // Validate the AST
        //Self::validate_ast(&ast)?;

        Ok(ast)
    }

    /*pub fn validate_ast(ast: &AquaAST) -> Result<(), AquaParseError> {
        // Add validation to ensure there's at least one SELECT clause
        if ast.select.is_empty() {
            return Err(AquaParseError::InvalidInput(
                "Query must have at least one SELECT expression".to_string(),
            ));
        }

        // Validate all SELECT clauses and check for duplicate aliases
        let mut used_aliases = std::collections::HashSet::new();

        // Validate all SELECT clauses
        for select_clause in &ast.select {
            match select_clause {
                SelectClause::Column(col_ref, alias) => {
                    Self::validate_field_reference(col_ref, &ast.from)?;
                    Self::validate_alias(alias, &mut used_aliases)?;
                }
                SelectClause::Aggregate(agg_func, alias) => {
                    Self::validate_field_reference(&agg_func.column, &ast.from)?;
                    Self::validate_alias(alias, &mut used_aliases)?;
                }
                SelectClause::ComplexValue(col_ref, _, _, alias) => {
                    Self::validate_field_reference(col_ref, &ast.from)?;
                    Self::validate_alias(alias, &mut used_aliases)?;
                }
            }
        }


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

        // Validate GROUP BY clause if present
        if let Some(ref group_by) = ast.group_by {
            Self::validate_select_with_group_by(&ast.select, group_by)?;
        }

        Ok(())
    }

    // New helper function to validate aliases
    fn validate_alias(
        alias: &Option<String>,
        used_aliases: &mut std::collections::HashSet<String>
    ) -> Result<(), AquaParseError> {
        if let Some(alias_name) = alias {
            if !used_aliases.insert(alias_name.clone()) {
                return Err(AquaParseError::InvalidInput(
                    format!("Duplicate alias name: {}", alias_name)
                ));
            }
        }
        Ok(())
    }
    

    fn validate_field_reference(
        where_condition: &Condition,
        from_clause: &FromClause,
    ) -> Result<(), AquaParseError> {
        let left = where_condition.left_field;
        let right = where_condition.right_field;

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
        Self::validate_field_reference(&clause.condition, from_clause)?;

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

    fn validate_select_with_group_by(
        select_clauses: &Vec<SelectClause>,
        group_by: &GroupByClause
    ) -> Result<(), AquaParseError> {
        for clause in select_clauses {
            match clause {
                SelectClause::Column(col_ref, _) => {
                    // Non-aggregated columns must appear in GROUP BY
                    if !group_by.columns.iter().any(|gc| gc.column == col_ref.column && gc.table == col_ref.table) {
                        return Err(AquaParseError::InvalidInput(
                            format!("Column {} must appear in GROUP BY clause or be aggregated", col_ref.to_string())
                        ));
                    }
                },
                // Aggregates are always allowed
                SelectClause::Aggregate(_, _) => {},
                SelectClause::ComplexValue(col_ref, _, _, _) => {
                    // Complex expressions need to reference only GROUP BY columns
                    if !group_by.columns.iter().any(|gc| gc.column == col_ref.column && gc.table == col_ref.table) {
                        return Err(AquaParseError::InvalidInput(
                            format!("Complex expression column {} must appear in GROUP BY clause", col_ref.to_string())
                        ));
                    }
                }
            }
        }
        Ok(())
    }*/
}
