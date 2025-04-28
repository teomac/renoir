use crate::dsl::ir::ir_ast_structure::ProjectionColumn;
use crate::dsl::ir::r_sink::r_sink_fold::create_map;
use crate::dsl::ir::r_sink::r_sink_utils::{AccumulatorInfo, AccumulatorValue};
use crate::dsl::ir::AggregateType;
use crate::dsl::struct_object::object::QueryObject;

/// Helper function to convert StreamInfo's agg_position into AccumulatorInfo
fn create_accumulator_info_from_stream(
    query_object: &QueryObject,
    stream_name: &String,
) -> AccumulatorInfo {
    let mut acc_info = AccumulatorInfo::new();
    let stream = query_object.get_stream(stream_name);

    // Convert each aggregate position from the stream into AccumulatorInfo
    for (agg_func, position) in &stream.agg_position {
        let value = AccumulatorValue::Aggregate(agg_func.function.clone(), agg_func.column.clone());
        let val_type = match agg_func.function {
            AggregateType::Count => "usize".to_string(),
            AggregateType::Avg => "f64".to_string(),
            _ => query_object.get_type(&agg_func.column),
        };

        // Extract position number from the position string (assuming format "x.1.N")
        let pos = position
            .split('.')
            .last()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        acc_info.value_positions.insert(value, (pos, val_type));
    }

    acc_info
}

/// Function to handle the case where aggregates have already been computed in a previous fold
pub(crate) fn create_aggregate_map_from_previous(
    projection_clauses: &[ProjectionColumn],
    stream_name: &String,
    query_object: &QueryObject,
) -> String {
    // Create AccumulatorInfo from StreamInfo's agg_position
    let acc_info = create_accumulator_info_from_stream(query_object, stream_name);

    // Reuse the existing create_map function
    create_map(projection_clauses, &acc_info, stream_name, query_object)
}
