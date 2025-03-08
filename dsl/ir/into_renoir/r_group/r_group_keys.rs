use crate::dsl::ir::ir_ast_structure::{
    AggregateType, Group
};
use crate::dsl::ir::r_sink::{base::r_sink_utils::collect_sink_aggregates, grouped::r_sink_grouped::process_grouping_projections};
use crate::dsl::ir::r_utils::check_alias;
use crate::dsl::ir::AggregateFunction;
use crate::dsl::ir::{ColumnRef, QueryObject};
use indexmap::IndexMap;
use crate::dsl::ir::r_group::conditions::{
    r_group_conditions::parse_group_conditions, 
    r_group_filter::create_filter_operation, 
    r_group_fold::create_fold_operation};

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
pub fn process_group_by(group_by: &Group, query_object: &QueryObject) -> String {
    let mut group_string = String::new();
    let table_names = query_object.get_all_table_names();

    // Validate GROUP BY columns
    for col in &group_by.columns {
        if query_object.has_join {
            let table = col
                .table
                .as_ref()
                .expect("Column in GROUP BY must have table reference in JOIN query");
            let table_name = check_alias(table, query_object);
            query_object.check_column_validity(col, &table_name);
        } else {
            let table_name = table_names
                .first()
                .expect("No tables found in query object");
            query_object.check_column_validity(col, table_name);
        }
    }

    // Generate GROUP BY operation
    let group_by_keys = process_group_by_keys(&group_by.columns, query_object);
    group_string.push_str(&format!(".group_by(|x| ({}))", group_by_keys));

    // Process having conditions if present
    let mut acc_info = GroupAccumulatorInfo::new();
    if let Some(ref group_condition) = group_by.group_condition {
        // First parse conditions and collect information
        parse_group_conditions(group_condition, query_object, &mut acc_info, group_by);

        //now collect all the aggregates from the sink. We need to add them to the fold
        let sink_agg = collect_sink_aggregates(query_object);

        //insert all the aggregates from the sink into the accumulator
        sink_agg.iter().for_each(|agg| {
            let col_type = query_object.get_type(&agg.column);
            let agg_value =
                GroupAccumulatorValue::Aggregate(agg.function.clone(), agg.column.clone());
            if agg.function == AggregateType::Avg {
                acc_info.add_avg(agg.column.clone(), col_type);
            } else if agg.function == AggregateType::Count {
                acc_info.add_aggregate(agg_value, "usize".to_string());
            } else {
                acc_info.add_aggregate(agg_value, col_type);
            }
        });

        // Then generate operations using the collected information
        group_string.push_str(&create_fold_operation(&acc_info, group_by, query_object));

        group_string.push_str(&create_filter_operation(
            group_condition,
            group_by,
            query_object,
            &acc_info,
        ));

        // Process select clauses, keeping in mind the grouping
        group_string.push_str(&process_grouping_projections(query_object, &acc_info));
    } else {
        //now collect all the aggregates from the sink. We need to add them to the fold
        let sink_agg = collect_sink_aggregates(query_object);

        //insert all the aggregates from the sink into the accumulator
        sink_agg.iter().for_each(|agg| {
            let col_type = query_object.get_type(&agg.column);
            let agg_value =
                GroupAccumulatorValue::Aggregate(agg.function.clone(), agg.column.clone());
            if agg.function == AggregateType::Avg {
                let _ = acc_info.add_avg(agg.column.clone(), col_type);
            } else if agg.function == AggregateType::Count {
                let _ = acc_info.add_aggregate(agg_value, "usize".to_string());
            } else {
                let _ = acc_info.add_aggregate(agg_value, col_type);
            }
        });
        // Then generate operations using the collected information
        group_string.push_str(&create_fold_operation(&acc_info, group_by, query_object));
        // Process select clauses, keeping in mind the grouping
        group_string.push_str(&process_grouping_projections(query_object, &acc_info));
    }

    group_string.push_str(".drop_key()");
    group_string
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
fn process_group_by_keys(columns: &Vec<ColumnRef>, query_object: &QueryObject) -> String {
    if !query_object.has_join {
        let stream_name = query_object.streams.keys().cloned().collect::<Vec<String>>()[0].clone();
        // No joins - simple reference to columns
        columns
            .iter()
            .map(|col| {
                query_object.check_column_validity(col, &stream_name);
                format!("x.{}.clone()", col.column)
            })
            .collect::<Vec<_>>()
            .join(", ")
    } else {
        // With joins - need to handle tuple access
        columns
            .iter()
            .map(|col| {
                let stream_name = if col.table.is_some(){
                    query_object.get_stream_from_alias(col.table.as_ref().unwrap()).unwrap()
                }
                else{
                    let all_streams = query_object.streams.keys().cloned().collect::<Vec<String>>();
                    if all_streams.len() == 1{
                        &all_streams[0].clone()
                    }
                    else{
                        panic!("Column {} does not have a table reference in a join query", col.column);
                    }
                };

                let stream = query_object.get_stream(stream_name);

                stream.check_if_column_exists(&col.column);
                
                format!(
                    "x{}.{}.clone()",
                    stream.get_access().get_base_path(),
                    col.column
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

