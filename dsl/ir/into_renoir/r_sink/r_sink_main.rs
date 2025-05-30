use crate::dsl::ir::ir_ast_structure::ProjectionColumn;
use crate::dsl::ir::r_sink::r_sink_fold::*;
use crate::dsl::ir::r_sink::r_sink_simple::*;
use crate::dsl::ir::r_sink::r_sink_star::*;
use crate::dsl::ir::r_sink::r_sink_utils::*;
use crate::dsl::struct_object::object::QueryObject;

use super::r_sink_agg::create_aggregate_map_from_previous;

pub(crate) fn process_projections(
    projections: &[ProjectionColumn],
    stream_name: &String,
    query_object: &mut QueryObject,
) -> Result<(), Box<dyn std::error::Error>> {
    let final_string;
    let result_column_types = query_object.result_column_types.clone();
    let stream = query_object.get_stream(stream_name);
    let final_struct = stream.final_struct.clone();
    let last_struct_name = final_struct.keys().last().unwrap();

    // Check if any aggregations are present using recursive traversal
    let has_aggregates: bool = projections.iter().any(|clause| match clause {
        ProjectionColumn::Aggregate(_, _) => true,
        ProjectionColumn::ComplexValue(field, _) => has_aggregate_in_complex_field(field),
        _ => false,
    });

    let new_struct_name = if final_struct.get(last_struct_name).unwrap().is_empty() {
        last_struct_name.to_string()
    } else {
        format!("{}_new", last_struct_name)
    };

    // Check for SELECT * case
    if projections.len() == 1 && !has_aggregates {
        match &projections[0] {
            ProjectionColumn::Column(col_ref, _) if col_ref.column == "*" => {
                final_string = create_star_map(stream_name, &new_struct_name, query_object);
            }
            _ => {
                final_string = create_simple_map(projections, stream_name, &new_struct_name, query_object);
            }
        }
        let stream = query_object.get_mut_stream(stream_name);

        stream.insert_op(final_string.clone());

        if stream.is_keyed {
            stream.insert_op(".drop_key()".to_string());
            stream.is_keyed = false;
            stream.key_columns.clear();
        }

        if final_struct.get(last_struct_name).unwrap().is_empty() {
            stream
                .final_struct
                .insert(last_struct_name.to_string(), result_column_types);
        } else {
            stream
                .final_struct
                .insert(format!("{}_new", last_struct_name), result_column_types);
        }

        return Ok(());
    }

    if has_aggregates {
        if !(query_object.get_stream(stream_name).agg_position.is_empty()) {
            //1. there is a group with a condition with aggregates ->
            //we have already performed a .fold(), we only have to access aggregates
            final_string =
                create_aggregate_map_from_previous(projections, stream_name, &new_struct_name, query_object);
        } else {
            //2. there is a group with a condition without aggregates || there is no group ->
            //we have to perform a .fold() and access the aggregates
            final_string = create_aggregate_map(projections, stream_name, &new_struct_name, query_object);
        }
    } else {
        final_string = create_simple_map(projections, stream_name, &new_struct_name, query_object);
    }

    let stream = query_object.get_mut_stream(stream_name);

    stream.insert_op(final_string.clone());

    if stream.is_keyed {
        stream.insert_op(".drop_key()".to_string());
        stream.is_keyed = false;
        stream.key_columns.clear();
    }

    if final_struct.get(last_struct_name).unwrap().is_empty() {
        stream
            .final_struct
            .insert(last_struct_name.to_string(), result_column_types);
    } else {
        stream
            .final_struct
            .insert(format!("{}_new", last_struct_name), result_column_types);
    }

    Ok(())
}
