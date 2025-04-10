use pest::iterators::{Pair, Pairs};
use std::sync::Arc;

use crate::dsl::ir::ast_parser::error::IrParseError;
use crate::dsl::ir::ast_parser::ir_ast_structure::*;
use crate::dsl::languages::dataframe::df_parser::Rule;

pub struct DataFrameASTBuilder;

impl DataFrameASTBuilder {
    pub fn build_ast_from_pairs(pairs: Pairs<Rule>) -> Result<Arc<IrPlan>, Box<IrParseError>> {
        let mut current_plan: Option<Arc<IrPlan>> = None;

        for pair in pairs {
            match pair.as_rule() {
                Rule::query => {
                    // Process the query parts
                    for method_chain in pair.into_inner() {
                        if method_chain.as_rule() == Rule::method_chain {
                            current_plan = Some(Self::process_method_chain(method_chain)?);
                        }
                    }
                }
                _ => {
                    return Err(Box::new(IrParseError::InvalidInput(format!(
                        "Expected query, got {:?}",
                        pair.as_rule()
                    ))))
                }
            }
        }

        // Ensure we built a complete plan
        Ok(current_plan.ok_or_else(|| IrParseError::InvalidInput("Empty query".to_string()))?)
    }

    fn process_method_chain(pair: Pair<Rule>) -> Result<Arc<IrPlan>, Box<IrParseError>> {
        let mut inner = pair.into_inner();

        // First item should be the table reference
        let table_ref = inner
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Missing table reference".to_string()))?;

        if table_ref.as_rule() != Rule::table_ref {
            return Err(Box::new(IrParseError::InvalidInput(format!(
                "Expected table reference, got {:?}",
                table_ref.as_rule()
            ))));
        }

        // The table name is used directly as the source
        let table_name = table_ref.as_str().to_string();

        // Create initial scan plan
        let mut base_plan = Arc::new(IrPlan::Scan {
            stream_name: format!("stream{}", 0),
            alias: Some(table_name.clone()), // Always set an explicit alias
            input: Arc::new(IrPlan::Table { table_name }),
        });

        // Collect all method calls
        let mut methods = Vec::new();
        for method in inner {
            methods.push(method);
        }

        // Store if we have a join query to enforce qualified columns
        let mut has_join = false;

        // Reorder methods to ensure Project is last
        // First process non-project operations (filter, groupby)
        // Then process project operations (select, agg)
        let mut has_select = false;
        let mut has_agg = false;
        let mut select_method = None;
        let mut agg_method = None;
        let mut groupby_method = None;

        // First pass - categorize methods
        for method in methods {
            match method.as_rule() {
                Rule::select_method => {
                    has_select = true;
                    select_method = Some(method);
                }
                Rule::agg_method => {
                    has_agg = true;
                    agg_method = Some(method);
                }
                Rule::groupby_method => {
                    groupby_method = Some(method);
                }
                Rule::filter_method => {
                    // Process filter right away
                    base_plan = Self::process_filter_method(method, base_plan, has_join)?;
                }
                Rule::join_method => {
                    has_join = true;
                    base_plan = Self::process_join_method(method, base_plan)?;
                }
                _ => {
                    return Err(Box::new(IrParseError::InvalidInput(format!(
                        "Unexpected method: {:?}",
                        method.as_rule()
                    ))))
                }
            }
        }

        // Process groupby if present
        if let Some(group_method) = groupby_method {
            base_plan = Self::process_groupby_method(group_method, base_plan, has_join)?;
        }

        // Process agg or select as the final operation - agg takes precedence as it requires groupby
        if has_agg {
            if let Some(agg) = agg_method {
                if let IrPlan::GroupBy {
                    input,
                    keys,
                    group_condition: _,
                } = &*base_plan
                {
                    base_plan = Self::process_agg_method(agg, input.clone(), keys.clone(), has_join)?;
                } else {
                    return Err(Box::new(IrParseError::InvalidInput(
                        "agg() method must follow groupby()".to_string(),
                    )));
                }
            }
        } else if has_select {
            if let Some(select) = select_method {
                base_plan = Self::process_select_method(select, base_plan, has_join)?;
            }
        } else {
            // If no projection operation was specified, add a default 'SELECT *' equivalent
            let star_projection = ProjectionColumn::Column(
                ColumnRef {
                    table: None,
                    column: "*".to_string(),
                },
                None,
            );

            base_plan = Arc::new(IrPlan::Project {
                input: base_plan,
                columns: vec![star_projection],
                distinct: false,
            });
        }

        Ok(base_plan)
    }

    fn process_select_method(
        pair: Pair<Rule>,
        input: Arc<IrPlan>,
        has_join: bool,
    ) -> Result<Arc<IrPlan>, Box<IrParseError>> {
        let column_list = pair.into_inner().next().ok_or_else(|| {
            IrParseError::InvalidInput("Missing column list in select()".to_string())
        })?;
    
        if column_list.as_rule() != Rule::column_list {
            return Err(Box::new(IrParseError::InvalidInput(
                "Expected column list in select()".to_string(),
            )));
        }
    
        let mut projection_columns = Vec::new();
    
        // Get all available table references for validation
        let mut available_tables = Vec::new();
        match &*input {
            IrPlan::Join { left, right, .. } => {
                if let IrPlan::Scan { alias: Some(left_alias), .. } = &**left {
                    available_tables.push(left_alias.clone());
                }
                if let IrPlan::Scan { alias: Some(right_alias), .. } = &**right {
                    available_tables.push(right_alias.clone());
                }
            },
            IrPlan::Scan { alias: Some(alias), .. } => {
                available_tables.push(alias.clone());
            },
            _ => {}
        }
    
        for col_item in column_list.into_inner() {
            if col_item.as_rule() == Rule::column_with_alias {
                let mut parts = col_item.into_inner();
                
                // First part should be the column reference
                let column_ref_pair = parts
                    .next()
                    .ok_or_else(|| IrParseError::InvalidInput("Missing column reference".to_string()))?;
    
                // Check if there's an alias
                let alias = parts.next().and_then(|p| {
                    if p.as_rule() == Rule::column_alias {
                        // Extract the identifier after "as"
                        let alias_ident = p.into_inner().next()?;
                        Some(alias_ident.as_str().to_string())
                    } else {
                        None
                    }
                });
    
                // Parse the column reference based on its type
                let column_ref = if column_ref_pair.as_rule() == Rule::qualified_column {
                    // Handle qualified column (table.column)
                    let mut qual_parts = column_ref_pair.into_inner();
                    let table = qual_parts.next()
                        .ok_or_else(|| IrParseError::InvalidInput("Missing table in qualified column".to_string()))?
                        .as_str().to_string();
                    let column = qual_parts.next()
                        .ok_or_else(|| IrParseError::InvalidInput("Missing column in qualified column".to_string()))?
                        .as_str().to_string();
                    
                    // Validate table exists
                    if has_join && !available_tables.contains(&table) {
                        return Err(Box::new(IrParseError::InvalidInput(format!(
                            "Unknown table reference '{}' in column. Available tables: {:?}",
                            table, available_tables
                        ))));
                    }
                    
                    ColumnRef {
                        table: Some(table),
                        column,
                    }
                } else if column_ref_pair.as_rule() == Rule::identifier {
                    // Simple column name
                    if has_join {
                        return Err(Box::new(IrParseError::InvalidInput(format!(
                            "Column '{}' must be qualified with a table name when using joins",
                            column_ref_pair.as_str()
                        ))));
                    }
                    
                    ColumnRef {
                        table: if available_tables.len() == 1 { Some(available_tables[0].clone()) } else { None },
                        column: column_ref_pair.as_str().to_string(),
                    }
                } else {
                    return Err(Box::new(IrParseError::InvalidInput(format!(
                        "Unexpected column reference type: {:?}",
                        column_ref_pair.as_rule()
                    ))));
                };
    
                // If no explicit alias is provided and this is a qualified column,
                // create a default alias based on the column name
                let effective_alias = if alias.is_none() && has_join {
                    // Use just the column part as the alias
                    Some(column_ref.column.clone())
                } else {
                    alias
                };
    
                projection_columns.push(ProjectionColumn::Column(column_ref, effective_alias));
            }
        }
    
        Ok(Arc::new(IrPlan::Project {
            input,
            columns: projection_columns,
            distinct: false,
        }))
    }

    fn process_filter_method(
        pair: Pair<Rule>,
        input: Arc<IrPlan>,
        has_join: bool,
    ) -> Result<Arc<IrPlan>, Box<IrParseError>> {
        // Extract filter condition from the string literal
        let filter_string = pair
            .into_inner()
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Missing filter condition".to_string()))?;
    
        if filter_string.as_rule() != Rule::string_literal {
            return Err(Box::new(IrParseError::InvalidInput(
                "Filter condition must be a string literal".to_string(),
            )));
        }
    
        // Strip quotes from string literal
        let condition_text = filter_string.as_str();
        let stripped = &condition_text[1..condition_text.len() - 1];
    
        // Parse the filter condition with join context
        let predicate = Self::parse_filter_condition(stripped, &input, has_join)?;
    
        Ok(Arc::new(IrPlan::Filter { input, predicate }))
    }
    
    // Updated to require qualified columns in join context
    fn parse_filter_condition(
        condition: &str, 
        input: &Arc<IrPlan>, 
        has_join: bool
    ) -> Result<FilterClause, Box<IrParseError>> {
        // Check for AND/OR logic
        if condition.contains("&&") {
            let parts: Vec<&str> = condition.split("&&").collect();
            let left = Self::parse_filter_condition(parts[0].trim(), input, has_join)?;
            let right = Self::parse_filter_condition(parts[1].trim(), input, has_join)?;
    
            return Ok(FilterClause::Expression {
                left: Box::new(left),
                binary_op: BinaryOp::And,
                right: Box::new(right),
            });
        } else if condition.contains("||") {
            let parts: Vec<&str> = condition.split("||").collect();
            let left = Self::parse_filter_condition(parts[0].trim(), input, has_join)?;
            let right = Self::parse_filter_condition(parts[1].trim(), input, has_join)?;
    
            return Ok(FilterClause::Expression {
                left: Box::new(left),
                binary_op: BinaryOp::Or,
                right: Box::new(right),
            });
        }
    
        // Parse individual comparison
        let mut operator = "";
        let mut parts = Vec::new();
    
        for op in &[">=", "<=", "!=", "==", ">", "<"] {
            if condition.contains(op) {
                operator = op;
                parts = condition.split(op).map(|s| s.trim()).collect();
                break;
            }
        }
    
        if parts.len() != 2 {
            return Err(Box::new(IrParseError::InvalidInput(format!(
                "Invalid filter condition: {}",
                condition
            ))));
        }
    
        // Get available tables for validation
        let mut available_tables = Vec::new();
        match &**input {
            IrPlan::Join { left, right, .. } => {
                if let IrPlan::Scan { alias: Some(left_alias), .. } = &**left {
                    available_tables.push(left_alias.clone());
                }
                if let IrPlan::Scan { alias: Some(right_alias), .. } = &**right {
                    available_tables.push(right_alias.clone());
                }
            },
            IrPlan::Scan { alias: Some(alias), .. } => {
                available_tables.push(alias.clone());
            },
            _ => {}
        }
    
        // Parse left side
        let left_field = if parts[0].contains('.') && has_join {
            // Qualified column reference
            let column_parts: Vec<&str> = parts[0].split('.').collect();
            if column_parts.len() != 2 {
                return Err(Box::new(IrParseError::InvalidInput(format!(
                    "Invalid qualified column format: {}", parts[0]
                ))));
            }
            
            let table = column_parts[0].to_string();
            let column = column_parts[1].to_string();
            
            // Validate table exists
            if !available_tables.contains(&table) {
                return Err(Box::new(IrParseError::InvalidInput(format!(
                    "Unknown table reference '{}' in filter condition", table
                ))));
            }
            
            ComplexField {
                column_ref: Some(ColumnRef {
                    table: Some(table),
                    column,
                }),
                literal: None,
                aggregate: None,
                nested_expr: None,
                subquery: None,
                subquery_vec: None,
            }
        } else if !parts[0].contains('.') && has_join {
            // Unqualified column in join context - error
            return Err(Box::new(IrParseError::InvalidInput(format!(
                "Column '{}' must be qualified with a table name when using joins",
                parts[0]
            ))));
        } else {
            // Simple column or non-join context
            ComplexField {
                column_ref: Some(ColumnRef {
                    table: if available_tables.len() == 1 { Some(available_tables[0].clone()) } else { None },
                    column: parts[0].to_string(),
                }),
                literal: None,
                aggregate: None,
                nested_expr: None,
                subquery: None,
                subquery_vec: None,
            }
        };
    
        // Parse right side - either literal or column
        let right_field = if parts[1].starts_with('\'') && parts[1].ends_with('\'') {
            // String literal
            ComplexField {
                column_ref: None,
                literal: Some(IrLiteral::String(
                    parts[1][1..parts[1].len() - 1].to_string(),
                )),
                aggregate: None,
                nested_expr: None,
                subquery: None,
                subquery_vec: None,
            }
        } else if let Ok(num) = parts[1].parse::<i64>() {
            // Integer literal
            ComplexField {
                column_ref: None,
                literal: Some(IrLiteral::Integer(num)),
                aggregate: None,
                nested_expr: None,
                subquery: None,
                subquery_vec: None,
            }
        } else if let Ok(num) = parts[1].parse::<f64>() {
            // Float literal
            ComplexField {
                column_ref: None,
                literal: Some(IrLiteral::Float(num)),
                aggregate: None,
                nested_expr: None,
                subquery: None,
                subquery_vec: None,
            }
        } else if parts[1] == "true" || parts[1] == "false" {
            // Boolean literal
            ComplexField {
                column_ref: None,
                literal: Some(IrLiteral::Boolean(parts[1] == "true")),
                aggregate: None,
                nested_expr: None,
                subquery: None,
                subquery_vec: None,
            }
        } else if parts[1].contains('.') && has_join {
            // Qualified column reference
            let column_parts: Vec<&str> = parts[1].split('.').collect();
            if column_parts.len() != 2 {
                return Err(Box::new(IrParseError::InvalidInput(format!(
                    "Invalid qualified column format: {}", parts[1]
                ))));
            }
            
            let table = column_parts[0].to_string();
            let column = column_parts[1].to_string();
            
            // Validate table exists
            if !available_tables.contains(&table) {
                return Err(Box::new(IrParseError::InvalidInput(format!(
                    "Unknown table reference '{}' in filter condition", table
                ))));
            }
            
            ComplexField {
                column_ref: Some(ColumnRef {
                    table: Some(table),
                    column,
                }),
                literal: None,
                aggregate: None,
                nested_expr: None,
                subquery: None,
                subquery_vec: None,
            }
        } else if !parts[1].contains('.') && has_join {
            // Unqualified column in join context - error
            return Err(Box::new(IrParseError::InvalidInput(format!(
                "Column '{}' must be qualified with a table name when using joins",
                parts[1]
            ))));
        } else {
            // Simple column reference in non-join context
            ComplexField {
                column_ref: Some(ColumnRef {
                    table: if available_tables.len() == 1 { Some(available_tables[0].clone()) } else { None },
                    column: parts[1].to_string(),
                }),
                literal: None,
                aggregate: None,
                nested_expr: None,
                subquery: None,
                subquery_vec: None,
            }
        };
    
        let op = match operator {
            ">" => ComparisonOp::GreaterThan,
            "<" => ComparisonOp::LessThan,
            ">=" => ComparisonOp::GreaterThanEquals,
            "<=" => ComparisonOp::LessThanEquals,
            "==" => ComparisonOp::Equal,
            "!=" => ComparisonOp::NotEqual,
            _ => {
                return Err(Box::new(IrParseError::InvalidInput(format!(
                    "Unsupported operator: {}",
                    operator
                ))))
            }
        };
    
        Ok(FilterClause::Base(FilterConditionType::Comparison(
            Condition {
                left_field,
                operator: op,
                right_field,
            },
        )))
    }

    fn process_join_method(
        pair: Pair<Rule>,
        input: Arc<IrPlan>,
    ) -> Result<Arc<IrPlan>, Box<IrParseError>> {
        let mut parts = pair.into_inner();
        
        // Get the right table reference
        let right_table = parts.next().ok_or_else(|| {
            IrParseError::InvalidInput("Missing right table in join()".to_string())
        })?;
        let right_table_name = right_table.as_str().to_string();
    
        // Get the left qualified column
        let left_col_pair = parts.next().ok_or_else(|| {
            IrParseError::InvalidInput("Missing left column in join()".to_string())
        })?;
        
        // Get the right qualified column
        let right_col_pair = parts.next().ok_or_else(|| {
            IrParseError::InvalidInput("Missing right column in join()".to_string())
        })?;
        
        // Process left qualified column (should be table.column format)
        let left_col = if left_col_pair.as_rule() == Rule::qualified_column {
            let mut qual_parts = left_col_pair.into_inner();
            let table = qual_parts.next()
                .ok_or_else(|| IrParseError::InvalidInput("Missing table in left join column".to_string()))?
                .as_str().to_string();
            let column = qual_parts.next()
                .ok_or_else(|| IrParseError::InvalidInput("Missing column in left join column".to_string()))?
                .as_str().to_string();
            
            // Validate the table refers to the input table
            let input_table = match &*input {
                IrPlan::Scan { alias: Some(alias), .. } => alias.clone(),
                _ => "".to_string()
            };
            
            if table != input_table {
                return Err(Box::new(IrParseError::InvalidInput(format!(
                    "Left join column table '{}' does not match input table '{}'",
                    table, input_table
                ))));
            }
            
            ColumnRef {
                table: Some(table),
                column,
            }
        } else {
            return Err(Box::new(IrParseError::InvalidInput(
                "Left join column must be in table.column format".to_string()
            )));
        };
        
        // Process right qualified column (should be table.column format)
        let right_col = if right_col_pair.as_rule() == Rule::qualified_column {
            let mut qual_parts = right_col_pair.into_inner();
            let table = qual_parts.next()
                .ok_or_else(|| IrParseError::InvalidInput("Missing table in right join column".to_string()))?
                .as_str().to_string();
            let column = qual_parts.next()
                .ok_or_else(|| IrParseError::InvalidInput("Missing column in right join column".to_string()))?
                .as_str().to_string();
            
            // Validate the table refers to the right table
            if table != right_table_name {
                return Err(Box::new(IrParseError::InvalidInput(format!(
                    "Right join column table '{}' does not match right table '{}'",
                    table, right_table_name
                ))));
            }
            
            ColumnRef {
                table: Some(table),
                column,
            }
        } else {
            return Err(Box::new(IrParseError::InvalidInput(
                "Right join column must be in table.column format".to_string()
            )));
        };
        
        // Optional join type (default to inner join)
        let join_type = if let Some(type_token) = parts.next() {
            match type_token.as_str() {
                "inner" => JoinType::Inner,
                "left" => JoinType::Left,
                "outer" => JoinType::Outer,
                _ => {
                    return Err(Box::new(IrParseError::InvalidInput(format!(
                        "Unsupported join type: {}",
                        type_token.as_str()
                    ))))
                }
            }
        } else {
            JoinType::Inner // Default to inner join
        };
        
        // Create the join condition with the qualified columns
        let condition = JoinCondition {
            left_col,
            right_col,
        };
        
        // Create the right side plan - ensure alias is set
        let right_plan = Arc::new(IrPlan::Scan {
            stream_name: format!("stream{}", 1), // Use index 1 for right stream
            alias: Some(right_table_name.clone()),
            input: Arc::new(IrPlan::Table { 
                table_name: right_table_name 
            }),
        });
        
        // Create the join plan
        Ok(Arc::new(IrPlan::Join {
            left: input,
            right: right_plan,
            condition: vec![condition],
            join_type,
        }))
    }

    fn process_groupby_method(
        pair: Pair<Rule>,
        input: Arc<IrPlan>,
        has_join: bool,
    ) -> Result<Arc<IrPlan>, Box<IrParseError>> {
        let column_list = pair.into_inner().next().ok_or_else(|| {
            IrParseError::InvalidInput("Missing column list in groupby()".to_string())
        })?;

        if column_list.as_rule() != Rule::column_list {
            return Err(Box::new(IrParseError::InvalidInput(
                "Expected column list in groupby()".to_string(),
            )));
        }

        let mut group_keys = Vec::new();

        // Get available tables for validation
        let mut available_tables = Vec::new();
        match &*input {
            IrPlan::Join { left, right, .. } => {
                if let IrPlan::Scan { alias: Some(left_alias), .. } = &**left {
                    available_tables.push(left_alias.clone());
                }
                if let IrPlan::Scan { alias: Some(right_alias), .. } = &**right {
                    available_tables.push(right_alias.clone());
                }
            },
            IrPlan::Scan { alias: Some(alias), .. } => {
                available_tables.push(alias.clone());
            },
            _ => {}
        }

        for col_item in column_list.into_inner() {
            if col_item.as_rule() == Rule::column_with_alias {
                let column_ref_pair = col_item
                    .into_inner()
                    .next()
                    .ok_or_else(|| IrParseError::InvalidInput("Missing column reference".to_string()))?;
                
                // Process column reference
                if column_ref_pair.as_rule() == Rule::column_ref {
                    // Get the inner content that might be a qualified column or simple identifier
                    let inner_col = column_ref_pair.into_inner().next()
                        .ok_or_else(|| IrParseError::InvalidInput("Empty column reference".to_string()))?;
                    
                    if inner_col.as_rule() == Rule::qualified_column {
                        // Handle table.column format
                        let mut qual_parts = inner_col.into_inner();
                        let table = qual_parts.next()
                            .ok_or_else(|| IrParseError::InvalidInput("Missing table in qualified column".to_string()))?
                            .as_str().to_string();
                        let column = qual_parts.next()
                            .ok_or_else(|| IrParseError::InvalidInput("Missing column in qualified column".to_string()))?
                            .as_str().to_string();
                        
                        // Validate table exists
                        if has_join && !available_tables.contains(&table) {
                            return Err(Box::new(IrParseError::InvalidInput(format!(
                                "Unknown table reference '{}' in groupby. Available tables: {:?}",
                                table, available_tables
                            ))));
                        }
                        
                        group_keys.push(ColumnRef {
                            table: Some(table),
                            column,
                        });
                    } else {
                        // Simple column name
                        if has_join {
                            return Err(Box::new(IrParseError::InvalidInput(format!(
                                "Column '{}' must be qualified with a table name when using joins",
                                inner_col.as_str()
                            ))));
                        }
                        
                        group_keys.push(ColumnRef {
                            table: if available_tables.len() == 1 { Some(available_tables[0].clone()) } else { None },
                            column: inner_col.as_str().to_string(),
                        });
                    }
                } else {
                    // Direct column identifier
                    if has_join {
                        return Err(Box::new(IrParseError::InvalidInput(format!(
                            "Column '{}' must be qualified with a table name when using joins",
                            column_ref_pair.as_str()
                        ))));
                    }
                    
                    group_keys.push(ColumnRef {
                        table: if available_tables.len() == 1 { Some(available_tables[0].clone()) } else { None },
                        column: column_ref_pair.as_str().to_string(),
                    });
                }
            }
        }

        Ok(Arc::new(IrPlan::GroupBy {
            input,
            keys: group_keys,
            group_condition: None,
        }))
    }

    fn process_agg_method(
        pair: Pair<Rule>,
        input: Arc<IrPlan>,
        keys: Vec<ColumnRef>,
        has_join: bool,
    ) -> Result<Arc<IrPlan>, Box<IrParseError>> {
        let agg_list = pair.into_inner().next().ok_or_else(|| {
            IrParseError::InvalidInput("Missing aggregation list in agg()".to_string())
        })?;

        if agg_list.as_rule() != Rule::agg_list {
            return Err(Box::new(IrParseError::InvalidInput(
                "Expected aggregation list in agg()".to_string(),
            )));
        }

        let mut projection_columns = Vec::new();

        // First add group by keys to projections
        for key in &keys {
            projection_columns.push(ProjectionColumn::Column(key.clone(), None));
        }

        // Get available tables for validation
        let mut available_tables = Vec::new();
        if let IrPlan::GroupBy { input: group_input, .. } = &*input {
            match &**group_input {
                IrPlan::Join { left, right, .. } => {
                    if let IrPlan::Scan { alias: Some(left_alias), .. } = &**left {
                        available_tables.push(left_alias.clone());
                    }
                    if let IrPlan::Scan { alias: Some(right_alias), .. } = &**right {
                        available_tables.push(right_alias.clone());
                    }
                },
                IrPlan::Scan { alias: Some(alias), .. } => {
                    available_tables.push(alias.clone());
                },
                _ => {}
            }
        }

        // Then process aggregation expressions
        for agg_expr in agg_list.into_inner() {
            if agg_expr.as_rule() == Rule::agg_expr {
                let mut parts = agg_expr.into_inner();

                let func_name = parts
                    .next()
                    .ok_or_else(|| {
                        IrParseError::InvalidInput("Missing aggregation function".to_string())
                    })?
                    .as_str();

                let column_ref_pair = parts
                    .next()
                    .ok_or_else(|| {
                        IrParseError::InvalidInput(
                            "Missing column in aggregation function".to_string(),
                        )
                    })?;

                let alias = parts.next().map(|p| p.as_str().to_string());

                let agg_type = match func_name {
                    "sum" => AggregateType::Sum,
                    "avg" => AggregateType::Avg,
                    "min" => AggregateType::Min,
                    "max" => AggregateType::Max,
                    "count" => AggregateType::Count,
                    _ => {
                        return Err(Box::new(IrParseError::InvalidInput(format!(
                            "Unsupported aggregation function: {}",
                            func_name
                        ))))
                    }
                };

                // Parse the column reference in the aggregation
                let column_ref = if column_ref_pair.as_rule() == Rule::column_ref {
                    // Get inner content (qualified column or identifier)
                    let inner_col = column_ref_pair.into_inner().next()
                        .ok_or_else(|| IrParseError::InvalidInput("Empty column reference".to_string()))?;
                    
                    if inner_col.as_rule() == Rule::qualified_column {
                        // Handle table.column format
                        let mut qual_parts = inner_col.into_inner();
                        let table = qual_parts.next()
                            .ok_or_else(|| IrParseError::InvalidInput("Missing table in qualified column".to_string()))?
                            .as_str().to_string();
                        let column = qual_parts.next()
                            .ok_or_else(|| IrParseError::InvalidInput("Missing column in qualified column".to_string()))?
                            .as_str().to_string();
                        
                        // Validate table exists
                        if has_join && !available_tables.contains(&table) {
                            return Err(Box::new(IrParseError::InvalidInput(format!(
                                "Unknown table reference '{}' in aggregation. Available tables: {:?}",
                                table, available_tables
                            ))));
                        }
                        
                        ColumnRef {
                            table: Some(table),
                            column,
                        }
                    } else {
                        // Simple column name
                        if has_join {
                            return Err(Box::new(IrParseError::InvalidInput(format!(
                                "Column '{}' must be qualified with a table name when using joins",
                                inner_col.as_str()
                            ))));
                        }
                        
                        ColumnRef {
                            table: if available_tables.len() == 1 { Some(available_tables[0].clone()) } else { None },
                            column: inner_col.as_str().to_string(),
                        }
                    }
                } else {
                    // Direct reference to column
                    if has_join {
                        return Err(Box::new(IrParseError::InvalidInput(format!(
                            "Column '{}' must be qualified with a table name when using joins",
                            column_ref_pair.as_str()
                        ))));
                    }
                    
                    ColumnRef {
                        table: if available_tables.len() == 1 { Some(available_tables[0].clone()) } else { None },
                        column: column_ref_pair.as_str().to_string(),
                    }
                };

                projection_columns.push(ProjectionColumn::Aggregate(
                    AggregateFunction {
                        function: agg_type,
                        column: column_ref,
                    },
                    alias,
                ));
            }
        }

        Ok(Arc::new(IrPlan::Project {
            input,
            columns: projection_columns,
            distinct: false,
        }))
    }
}