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
            alias: None,
            input: Arc::new(IrPlan::Table { table_name }),
        });

        // Collect all method calls
        let mut methods = Vec::new();
        for method in inner {
            methods.push(method);
        }

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
                    base_plan = Self::process_filter_method(method, base_plan)?;
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
            base_plan = Self::process_groupby_method(group_method, base_plan)?;
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
                    base_plan = Self::process_agg_method(agg, input.clone(), keys.clone())?;
                } else {
                    return Err(Box::new(IrParseError::InvalidInput(
                        "agg() method must follow groupby()".to_string(),
                    )));
                }
            }
        } else if has_select {
            if let Some(select) = select_method {
                base_plan = Self::process_select_method(select, base_plan)?;
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

        for col in column_list.into_inner() {
            if col.as_rule() == Rule::column_with_alias {
                let mut parts = col.into_inner();
                let column_name = parts
                    .next()
                    .ok_or_else(|| IrParseError::InvalidInput("Missing column name".to_string()))?
                    .as_str();

                let alias = parts.next().map(|p| p.as_str().to_string());

                projection_columns.push(ProjectionColumn::Column(
                    ColumnRef {
                        table: None,
                        column: column_name.to_string(),
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

    fn process_filter_method(
        pair: Pair<Rule>,
        input: Arc<IrPlan>,
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

        // Parse the filter condition
        let predicate = Self::parse_filter_condition(stripped)?;

        Ok(Arc::new(IrPlan::Filter { input, predicate }))
    }

    fn parse_filter_condition(condition: &str) -> Result<FilterClause, Box<IrParseError>> {

        // Check for AND/OR logic
        if condition.contains("&&") {
            let parts: Vec<&str> = condition.split("&&").collect();
            let left = Self::parse_filter_condition(parts[0].trim())?;
            let right = Self::parse_filter_condition(parts[1].trim())?;

            return Ok(FilterClause::Expression {
                left: Box::new(left),
                binary_op: BinaryOp::And,
                right: Box::new(right),
            });
        } else if condition.contains("||") {
            let parts: Vec<&str> = condition.split("||").collect();
            let left = Self::parse_filter_condition(parts[0].trim())?;
            let right = Self::parse_filter_condition(parts[1].trim())?;

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

        let left_field = ComplexField {
            column_ref: Some(ColumnRef {
                table: None,
                column: parts[0].to_string(),
            }),
            literal: None,
            aggregate: None,
            nested_expr: None,
            subquery: None,
            subquery_vec: None,
        };

        // Try to parse the right side as a literal or column reference
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
        } else {
            ComplexField {
                column_ref: Some(ColumnRef {
                    table: None,
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

    fn process_groupby_method(
        pair: Pair<Rule>,
        input: Arc<IrPlan>,
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

        for col in column_list.into_inner() {
            if col.as_rule() == Rule::column_with_alias {
                let column_name = col
                    .into_inner()
                    .next()
                    .ok_or_else(|| IrParseError::InvalidInput("Missing column name".to_string()))?
                    .as_str();

                group_keys.push(ColumnRef {
                    table: None,
                    column: column_name.to_string(),
                });
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

                let column_name = parts
                    .next()
                    .ok_or_else(|| {
                        IrParseError::InvalidInput(
                            "Missing column in aggregation function".to_string(),
                        )
                    })?
                    .as_str();

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

                projection_columns.push(ProjectionColumn::Aggregate(
                    AggregateFunction {
                        function: agg_type,
                        column: ColumnRef {
                            table: None,
                            column: column_name.to_string(),
                        },
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
