use crate::dsl::ir::ir_ast_structure::ProjectionColumn;
use crate::dsl::ir::r_sink::r_sink_utils::*;
use crate::dsl::ir::r_sink::r_sink_star::*;
use crate::dsl::ir::r_sink::r_sink_simple::*;
use crate::dsl::ir::r_sink::r_sink_agg::*;
use crate::dsl::struct_object::object::QueryObject;

pub fn process_projections(
    projections: &Vec<ProjectionColumn>,
    stream_name: &String,
    query_object: &mut QueryObject,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut final_string = String::new();
    // Check for SELECT * case
    if projections.len() == 1 {
        match &projections[0] {
            ProjectionColumn::Column(col_ref, _) if col_ref.column == "*" => {
                final_string = create_star_map(stream_name, query_object);
            }
            _ => {}
        }
        let stream = query_object.get_mut_stream(&stream_name);

        stream.insert_op(final_string.clone());

        return Ok(());
    }
    // Check if any aggregations are present using recursive traversal
    let has_aggregates: bool = projections.iter().any(|clause| match clause {
        ProjectionColumn::Aggregate(_, _) => true,
        ProjectionColumn::ComplexValue(field, _) => has_aggregate_in_complex_field(field),
        _ => false,
    });

    if has_aggregates {
        if !(query_object.get_stream(stream_name).agg_position.is_empty()){
            //1. there is a group with a condition with aggregates -> 
            //we have already performed a .fold(), we only have to access aggregates

            //TODO - implement this case
        }
        else{
            //2. there is a group with a condition without aggregates || there is no group -> 
            //we have to perform a .fold() and access the aggregates
            final_string = create_aggregate_map(projections, stream_name, query_object);
        }
    } else {
        final_string = create_simple_map(projections, stream_name, query_object);
    }

    let stream = query_object.get_mut_stream(&stream_name);

    stream.insert_op(final_string.clone());

    Ok(())
}
