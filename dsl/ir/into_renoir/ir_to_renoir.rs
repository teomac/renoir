use crate::dsl::{
    ir::{
        ast_parser::ir_ast_structure::IrPlan,
        into_renoir::{
            r_condition::process_filter_clause, r_group::r_group_keys::process_group_by, r_join::*,
            r_sink::r_sink_main::process_projections,
        },
        OrderDirection,
    },
    struct_object::object::QueryObject,
};
use std::sync::Arc;
pub struct IrToRenoir;

impl IrToRenoir {
    /// Converts the IR plan into Renoir code.
    pub(crate) fn convert(
        ast: &Arc<IrPlan>,
        query_object: &mut QueryObject,
    ) -> Result<String, Box<dyn std::error::Error>> {
        match &**ast {
            IrPlan::Scan { stream_name, .. } => {
                // For Scan, we already have the stream_name
                Ok(stream_name.clone())
            }
            IrPlan::Filter { input, predicate } => {
                // Get the stream from processing the input
                let stream_name = Self::convert(input, query_object)?;
                // Pass both predicate and stream name
                process_filter_clause(predicate, &stream_name, query_object)?;
                Ok(stream_name)
            }
            IrPlan::Project {
                input,
                columns,
                distinct,
            } => {
                let stream_name = Self::convert(input, query_object)?;

                //function used to fill the result_column_types object in the query_object
                query_object.populate_result_mappings(columns, &stream_name);

                process_projections(columns, &stream_name, query_object)?;
                if *distinct {
                    let stream = query_object.get_mut_stream(&stream_name);
                    // If distinct is true, we need to set the distinct flag in the stream
                    stream.distinct = true;
                }
                Ok(stream_name)
            }
            IrPlan::GroupBy {
                input,
                keys,
                group_condition,
            } => {
                let stream_name = Self::convert(input, query_object)?;
                process_group_by(keys, group_condition, &stream_name, query_object)?;
                Ok(stream_name)
            }
            IrPlan::Join {
                left,
                right,
                condition,
                join_type,
            } => {
                query_object.has_join = true;
                // For joins we need both stream names
                let left_stream = Self::convert(left, query_object)?;
                let right_stream = Self::convert(right, query_object)?;

                // The join will create/modify operations on the left stream
                process_join(
                    &left_stream,
                    &right_stream,
                    condition,
                    join_type,
                    query_object,
                )?;

                // After join we continue with left stream
                Ok(left_stream)
            }
            IrPlan::OrderBy { input, items } => {
                let stream_name = Self::convert(input, query_object)?;
                let stream = query_object.get_mut_stream(&stream_name);

                //for each item in items, push the order by clause to the stream
                for item in items {
                    let order = match item.direction {
                        OrderDirection::Asc => "asc".to_string(),
                        OrderDirection::Desc => "desc".to_string(),
                    };
                    stream.order_by.push((item.column.clone(), order));
                }
                // Store for output phase
                //process_order_by(items, query_object);
                Ok(stream_name)
            }
            IrPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let stream_name = Self::convert(input, query_object)?;
                // Store for output phase
                let stream = query_object.get_mut_stream(&stream_name);
                stream.limit = Some(((*limit as usize), (offset.unwrap_or(0) as usize)));
                //process_limit(*offset, *limit, query_object);
                Ok(stream_name)
            }
            IrPlan::Table { table_name } => Ok(table_name.clone()),
        }
    }
}
