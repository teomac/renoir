use crate::dsl::ir::ir_ast_structure::{
    AggregateType, GroupClause
};
use crate::dsl::ir::r_utils::check_alias;
use crate::dsl::ir::{AggregateFunction, ProjectionColumn};
use crate::dsl::ir::{ColumnRef, QueryObject};
use indexmap::IndexMap;
use crate::dsl::ir::r_group::conditions::{
    r_group_conditions::parse_group_conditions, 
    r_group_filter::create_filter_operation, 
    r_group_fold::create_fold_operation};

use crate::dsl::struct_object::utils::*;

// Base enum for tracking accumulator values
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GroupAccumulatorValue {
    Aggregate(AggregateType, ColumnRef),
}

//AccumulatorInfo to track all accumulators position in the .fold() operation
#[derive(Debug)]
pub struct GroupAccumulatorInfo {
    // Track positions of aggregates
    pub agg_positions: IndexMap<GroupAccumulatorValue, (usize, String)>, // (position, type)
}

impl GroupAccumulatorInfo {
    fn new() -> Self {
        GroupAccumulatorInfo {
            agg_positions: IndexMap::new(),
        }
    }

    pub fn add_aggregate(&mut self, value: GroupAccumulatorValue, val_type: String) -> usize {
        if let Some((pos, _)) = self.agg_positions.get(&value) {
            *pos
        } else {
            let pos = self.agg_positions.len();
            self.agg_positions.insert(value, (pos, val_type));
            pos
        }
    }

    // Special handling for AVG which needs both sum and count
    pub fn add_avg(&mut self, column: ColumnRef, val_type: String) -> (usize, usize) {
        let sum_pos = self.add_aggregate(
            GroupAccumulatorValue::Aggregate(AggregateType::Sum, column.clone()),
            val_type,
        );
        let count_pos = self.add_aggregate(
            GroupAccumulatorValue::Aggregate(AggregateType::Count, column),
            "usize".to_string(),
        );
        (sum_pos, count_pos)
    }

    pub fn get_agg_position(&self, agg: &AggregateFunction) -> usize {
        let col = &agg.column;
        let agg_value = GroupAccumulatorValue::Aggregate(agg.function.clone(), col.clone());
        if agg.function == AggregateType::Avg {
            let sum_col = GroupAccumulatorValue::Aggregate(AggregateType::Sum, col.clone());
            if let Some((pos, _)) = self.agg_positions.get(&sum_col) {
                *pos
            } else {
                panic!("Aggregate {:?} not found in accumulator", sum_col);
            }
        } else if let Some((pos, _)) = self.agg_positions.get(&agg_value) {
            *pos
        } else {
            panic!("Aggregate {:?} not found in accumulator", agg_value);
        }
    }
}

/// Process the GroupByClause from Ir AST and generate the corresponding Renoir operator string.
///
/// # Arguments
///
/// * `group_by` - The GroupByClause from the Ir AST containing group by columns and having conditions
/// * `query_object` - The QueryObject containing metadata about tables and columns
///
/// # Returns
///
/// A String containing the Renoir operator chain for the group by operation
pub fn process_group_by(
    keys: &Vec<ColumnRef>,
    group_condition: &Option<GroupClause>, 
    stream_name: &String,
    query_object: &mut QueryObject) -> Result<(), Box<dyn std::error::Error>> {

        let mut group_string_keys = String::new();
        let mut group_string_condition = String::new();

        // Validate GROUP BY columns
        for col in keys {
            if query_object.has_join {
                let table = col
                    .table
                    .as_ref()
                    .expect("Column in GROUP BY must have table reference in JOIN query");
                let table_name = check_alias(table, query_object);
                check_column_validity(col, &table_name, query_object);
            } else {
                check_column_validity(col, stream_name, query_object);
            }
        }
    
        // Generate GROUP BY operation
        let group_by_keys = process_group_by_keys(keys, query_object);
        group_string_keys.push_str(&format!(".group_by(|x| ({}))", group_by_keys));
    
        // Process having conditions if present
        let mut acc_info = GroupAccumulatorInfo::new();
        if let Some(ref condition) = group_condition {
            // First parse conditions and collect information
            parse_group_conditions(condition, query_object, &mut acc_info, keys);
    
            //collect all the aggregates from the sink
            let sink_agg = &query_object.projection_agg;
    
            //insert all the aggregates from the sink into the accumulator
            for agg in sink_agg {
                match agg {
                    ProjectionColumn::Aggregate(agg, _ ) => {
                        let col_type = query_object.get_type(&agg.column);
                        let agg_value = GroupAccumulatorValue::Aggregate(agg.function.clone(), agg.column.clone());
                        if agg.function == AggregateType::Avg {
                            acc_info.add_avg(agg.column.clone(), col_type);
                        } else if agg.function == AggregateType::Count {
                            acc_info.add_aggregate(agg_value, "usize".to_string());
                        } else {
                            acc_info.add_aggregate(agg_value, col_type);
                        }
                    }
                    _ => panic!("Unexpected ProjectionColumn type in sink"),
                }
                
            }
    
            // Generate operations using the collected information
            if !acc_info.agg_positions.is_empty(){
                group_string_condition.push_str(&create_fold_operation(&acc_info, stream_name, query_object));
            }
            group_string_condition.push_str(&create_filter_operation(condition, keys, query_object, &acc_info));
        }
    
        // Store the operation in the correct stream
        let stream = query_object.get_mut_stream(stream_name);
        stream.insert_op(group_string_keys);
        if !group_string_condition.is_empty() {stream.insert_op(group_string_condition);}
    
        Ok(())
}

/// Process the group by keys and generate the corresponding tuple of column references.
///
/// # Arguments
///
/// * `columns` - Vector of ColumnRef representing the group by columns
/// * `query_object` - The QueryObject containing metadata about tables and columns
///
/// # Returns
///
/// A String containing the tuple of column references for group by
fn process_group_by_keys(columns: &[ColumnRef], query_object: &mut QueryObject) -> String {
    if !query_object.has_join {
        let stream_name = query_object.streams.keys().cloned().collect::<Vec<String>>()[0].clone();
        let stream  = query_object.get_stream(&stream_name).clone();
        // No joins - simple reference to columns
        let final_string  = columns
            .iter()
            .map(|col| {
                let col_stream = col.table.as_ref().unwrap_or(&stream_name);
                check_column_validity(col, col_stream, query_object);
                let needs_casting = stream.get_field_type(&col.column) == "f64";
                format!("x.{}.clone(){}", col.column, if needs_casting { ".map(OrderedFloat)" } else { "" })
            })
            .collect::<Vec<_>>()
            .join(", ");
            
            let stream = query_object.get_mut_stream(&stream_name);
            stream.is_keyed = true;
            stream.key_columns.extend(columns.to_owned());

            final_string
    } else {
        // With joins - need to handle tuple access
        let final_string = columns
            .iter()
            .map(|col| {
                let stream_name = if col.table.is_some(){
                    query_object.get_stream_from_alias(col.table.as_ref().unwrap()).unwrap().clone()
                }
                else{
                    let all_streams = query_object.streams.keys().cloned().collect::<Vec<String>>();
                    if all_streams.len() == 1{
                        all_streams[0].clone()
                    }
                    else{
                        panic!("Column {} does not have a table reference in a join query", col.column);
                    }
                };

                let stream = query_object.get_stream(&stream_name);

                stream.check_if_column_exists(&col.column);

                let needs_casting = stream.get_field_type(&col.column) == "f64";

                let stream_access = stream.get_access().get_base_path();

                let mut_stream = query_object.get_mut_stream(&stream_name);
                mut_stream.is_keyed = true;
                mut_stream.key_columns.push(col.clone());
                
                format!(
                    "x{}.{}.clone(){}",
                    stream_access,
                    col.column,
                    if needs_casting { ".map(OrderedFloat)" } else { "" }
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        final_string
    }
}

