use serde_json::Value;
use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use super::conversion_error::ConversionError;
use crate::dsl::ir::ast_parser::ir_ast_structure::*;
use indexmap::IndexMap;

pub struct CatalystConverter {
    // Track expression IDs and their tables
    expr_to_table: HashMap<String, String>,
    // Track column origins
    column_origins: HashMap<String, HashMap<String, HashSet<String>>>,
    // Store RDD expressions
    rdd_exprs: HashMap<String, Value>,
}

impl CatalystConverter {
    pub fn new() -> Self {
        CatalystConverter {
            expr_to_table: HashMap::new(),
            column_origins: HashMap::new(),
            rdd_exprs: HashMap::new(),
        }
    }

    pub fn convert_to_ir(json_str: &str) -> Result<Arc<IrPlan>, ConversionError> {
        let mut converter = Self::new();
        let catalyst_plan: Vec<Value> = serde_json::from_str(json_str)
            .map_err(|e| ConversionError::JsonParsing(e.to_string()))?;

        if catalyst_plan.is_empty() {
            return Err(ConversionError::EmptyPlan);
        }

        converter.build_plan(&catalyst_plan)
    }

    fn build_plan(&mut self, plan: &[Value]) -> Result<Arc<IrPlan>, ConversionError> {
        // Build bottom-up starting from LogicalRDD
        let mut current_plan = None;
        
        // First find and process LogicalRDD (table scan)
        for (idx, node) in plan.iter().enumerate() {
            if let Some(class) = node["class"].as_str() {
                if class.ends_with("LogicalRDD") {
                    current_plan = Some(self.process_logical_rdd(node)?);
                    // Store RDD expression for later reference
                    self.rdd_exprs.insert(format!("rdd_{}", idx), node.clone());
                    break;
                }
            }
        }

        // Process remaining nodes in order
        for node in plan.iter() {
            if let Some(class) = node["class"].as_str() {
                match class.split('.').last() {
                    Some("Filter") => {
                        if let Some(input) = current_plan {
                            current_plan = Some(self.process_filter(node, input)?);
                        }
                    },
                    Some("Project") => {
                        if let Some(input) = current_plan {
                            current_plan = Some(self.process_project(node, input)?);
                        }
                    },
                    Some("LogicalRDD") => continue, // Already processed
                    Some(unknown) => return Err(ConversionError::UnsupportedNodeType(unknown.to_string())),
                    None => return Err(ConversionError::InvalidClassName),
                }
            }
        }

        current_plan.ok_or(ConversionError::EmptyPlan)
    }

    fn process_logical_rdd(&mut self, node: &Value) -> Result<Arc<IrPlan>, ConversionError> {
        let output = node["output"].as_array()
            .ok_or(ConversionError::MissingField("output".to_string()))?;

        if output.is_empty() {
            return Err(ConversionError::EmptySchema);
        }

        // Register columns with the table
        let mut table_columns = HashMap::new();
        let table_name = self.derive_table_name(node)?;

        for cols in output {
            for col in cols.as_array().unwrap_or(&vec![]) {
                if let Some(name) = col["name"].as_str() {
                    let expr_id = self.get_expr_id(col)?;
                    
                    // Register column with table
                    table_columns.entry(name.to_string())
                        .or_insert_with(HashSet::new)
                        .insert(expr_id.clone());
                    
                    // Map expression to table
                    self.expr_to_table.insert(expr_id, table_name.clone());
                }
            }
        }

        self.column_origins.insert(table_name.clone(), table_columns);

        Ok(Arc::new(IrPlan::Table { 
            table_name 
        }))
    }

    fn process_filter(&mut self, node: &Value, input: Arc<IrPlan>) -> Result<Arc<IrPlan>, ConversionError> {
        let condition = node["condition"].as_array()
            .ok_or(ConversionError::MissingField("condition".to_string()))?;

        let predicate = self.build_filter_clause(condition)?;

        Ok(Arc::new(IrPlan::Filter {
            input,
            predicate,
        }))
    }

    fn build_filter_clause(&self, condition: &[Value]) -> Result<FilterClause, ConversionError> {
        let root = &condition[0];
        let expr_type = root["class"].as_str()
            .ok_or(ConversionError::MissingField("class".to_string()))?
            .split('.')
            .last()
            .ok_or(ConversionError::InvalidClassName)?;

        match expr_type {
            "And" => {
                let left_idx = root["left"].as_u64()
                    .ok_or(ConversionError::MissingField("left index".to_string()))? as usize;
                let right_idx = root["right"].as_u64()
                    .ok_or(ConversionError::MissingField("right index".to_string()))? as usize;

                Ok(FilterClause::Expression {
                    left: Box::new(self.build_filter_clause(&condition[left_idx + 1..])?),
                    binary_op: BinaryOp::And,
                    right: Box::new(self.build_filter_clause(&condition[right_idx + 1..])?),
                })
            },
            "IsNotNull" => {
                let field = self.build_complex_field(&condition[1])?;
                Ok(FilterClause::Base(FilterConditionType::NullCheck(
                    NullCondition {
                        field,
                        operator: NullOp::IsNotNull,
                    }
                )))
            },
            "GreaterThan" => {
                let left_field = self.build_complex_field(&condition[1])?;
                let right_field = self.build_complex_field(&condition[2])?;
                
                Ok(FilterClause::Base(FilterConditionType::Comparison(
                    Condition {
                        left_field,
                        operator: ComparisonOp::GreaterThan,
                        right_field,
                    }
                )))
            },
            _ => Err(ConversionError::UnsupportedExpressionType(expr_type.to_string())),
        }
    }

    fn process_project(&mut self, node: &Value, input: Arc<IrPlan>) -> Result<Arc<IrPlan>, ConversionError> {
        let project_list = node["projectList"].as_array()
            .ok_or(ConversionError::MissingField("projectList".to_string()))?;

        let mut columns = Vec::new();
        
        for proj_array in project_list {
            for proj in proj_array.as_array().unwrap_or(&vec![]) {
                let column = self.build_projection_column(proj, &input)?;
                columns.push(column);
            }
        }

        Ok(Arc::new(IrPlan::Project {
            input,
            columns,
            distinct: false,
        }))
    }

    fn build_projection_column(&self, expr: &Value, input: &IrPlan) -> Result<ProjectionColumn, ConversionError> {
        let expr_type = expr["class"].as_str()
            .ok_or(ConversionError::MissingField("class".to_string()))?
            .split('.')
            .last()
            .ok_or(ConversionError::InvalidClassName)?;

        match expr_type {
            "AttributeReference" => {
                let name = expr["name"].as_str()
                    .ok_or(ConversionError::MissingField("name".to_string()))?;
                let expr_id = self.get_expr_id(expr)?;

                // Find the table for this column
                let table = self.expr_to_table.get(&expr_id)
                    .ok_or(ConversionError::MissingField("table reference".to_string()))?
                    .clone();

                Ok(ProjectionColumn::Column(
                    ColumnRef {
                        table: Some(table),
                        column: name.to_string(),
                    },
                    None // No alias
                ))
            },
            _ => Err(ConversionError::UnsupportedExpressionType(expr_type.to_string())),
        }
    }

    fn build_complex_field(&self, expr: &Value) -> Result<ComplexField, ConversionError> {
        let expr_type = expr["class"].as_str()
            .ok_or(ConversionError::MissingField("class".to_string()))?
            .split('.')
            .last()
            .ok_or(ConversionError::InvalidClassName)?;

        match expr_type {
            "AttributeReference" => {
                let name = expr["name"].as_str()
                    .ok_or(ConversionError::MissingField("name".to_string()))?;
                let expr_id = self.get_expr_id(expr)?;

                let table = self.expr_to_table.get(&expr_id)
                    .ok_or(ConversionError::MissingField("table reference".to_string()))?
                    .clone();

                Ok(ComplexField {
                    column_ref: Some(ColumnRef {
                        table: Some(table),
                        column: name.to_string(),
                    }),
                    literal: None,
                    aggregate: None,
                    nested_expr: None,
                    subquery: None,
                    subquery_vec: None,
                })
            },
            "Literal" => {
                let value = expr["value"].as_str()
                    .ok_or(ConversionError::MissingField("value".to_string()))?;
                
                Ok(ComplexField {
                    column_ref: None,
                    literal: Some(IrLiteral::Integer(value.parse()?)),
                    aggregate: None,
                    nested_expr: None,
                    subquery: None,
                    subquery_vec: None,
                })
            },
            _ => Err(ConversionError::UnsupportedExpressionType(expr_type.to_string())),
        }
    }

    fn get_expr_id(&self, node: &Value) -> Result<String, ConversionError> {
        let expr_id = node["exprId"].as_object()
            .ok_or(ConversionError::MissingField("exprId".to_string()))?;
        
        let id = expr_id["id"].as_u64()
            .ok_or(ConversionError::MissingField("id".to_string()))?;
        let jvm_id = expr_id["jvmId"].as_str()
            .ok_or(ConversionError::MissingField("jvmId".to_string()))?;

        Ok(format!("{}_{}", id, jvm_id))
    }

    fn derive_table_name(&self, node: &Value) -> Result<String, ConversionError> {
        // For now, use the first column name as the base for table name
        // This could be enhanced to use more metadata from the RDD/DataFrame
        if let Some(output) = node["output"].as_array() {
            if let Some(first_col) = output.first() {
                if let Some(col_array) = first_col.as_array() {
                    if let Some(col) = col_array.first() {
                        if let Some(name) = col["name"].as_str() {
                            return Ok(format!("table_{}", name));
                        }
                    }
                }
            }
        }
        
        Err(ConversionError::MissingField("table name".to_string()))
    }


    pub fn process_table_metadata(metadata_list: Vec<String>) -> Vec<(String, IndexMap<String, String>)> {
        let mut result = Vec::new();
        
        for metadata_str in metadata_list {
            // Parse JSON 
            let metadata: serde_json::Value = serde_json::from_str(&metadata_str)
                .unwrap_or_else(|e| panic!("Failed to parse metadata JSON: {}", e));
                
            // For each table in metadata
            for (table_name, table_info) in metadata.as_object().unwrap() {
                let mut column_types = IndexMap::new();
                
                // Get columns array
                let columns = table_info.get("columns")
                    .and_then(|c| c.as_array())
                    .unwrap_or_else(|| panic!("No columns found for table {}", table_name));
                    
                // Process each column
                for column in columns {
                    let name = column.get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or_else(|| panic!("Column name not found"));
                        
                    let type_str = column.get("type")
                        .and_then(|t| t.as_str())
                        .unwrap_or_else(|| panic!("Column type not found"));
                        
                    // Convert Spark types to Renoir types
                    let renoir_type = match type_str {
                        "LongType()" => "i64",
                        "DoubleType()" => "f64",
                        "StringType()" => "String", 
                        "BooleanType()" => "bool",
                        _ => panic!("Unsupported type: {}", type_str)
                    };
                    
                    column_types.insert(name.to_string(), renoir_type.to_string());
                }
                
                result.push((table_name.to_string(), column_types));
            }
        }
        
        result
    }
}