use crate::dsl::ir::ir_ast_structure::*;
use crate::dsl::ir::QueryObject;
use crate::dsl::struct_object::support_structs::JoinTree;
use crate::dsl::struct_object::utils::*;

pub(crate) fn process_join(
    left_stream: &String,
    right_stream: &String,
    conditions: &Vec<JoinCondition>,
    join_type: &JoinType,
    query_object: &mut QueryObject,
) -> Result<(), Box<dyn std::error::Error>> {
    // Get the join type string
    let join_method = match join_type {
        JoinType::Inner => "join",
        JoinType::Left => "left_join",
        JoinType::Outer => "outer_join",
    };

    let mut left_tuple: Vec<String> = Vec::new();
    let mut right_tuple: Vec<String> = Vec::new();

    // Generate the join conditions
    for condition in conditions {
        let mut left_col = condition.left_col.clone();
        let mut right_col = condition.right_col.clone();

        // Get the stream names from aliases
        let mut left_stream_name = query_object
            .get_stream_from_alias(left_col.table.as_ref().unwrap())
            .unwrap_or_else(|| {
                &query_object.streams.get(left_col.table.as_ref().unwrap()).as_ref().unwrap().id
            })
            .clone();
        let mut right_stream_name = query_object
            .get_stream_from_alias(right_col.table.as_ref().unwrap())
            .unwrap_or_else(|| {
                &query_object.streams.get(right_col.table.as_ref().unwrap()).as_ref().unwrap().id
            })
            .clone();

        // Validate columns
        check_column_validity(&left_col, &left_stream_name, query_object);
        check_column_validity(&right_col, &right_stream_name, query_object);

        // Check if columns need to be swapped
        if left_stream_name == *right_stream {
            let temp = left_col.clone();
            left_col = right_col.clone();
            right_col = temp.clone();

            let temp2 = left_stream_name.clone();
            left_stream_name = right_stream_name.clone();
            right_stream_name = temp2.clone();
        }

        let left_stream = query_object.get_stream(&left_stream_name);
        let right_stream = query_object.get_stream(&right_stream_name);

        // Build tuple expressions
        let needs_casting =
            query_object.get_type(&left_col) == "f64" || query_object.get_type(&right_col) == "f64";

        left_tuple.push(format!(
            "x{}.{}.clone(){}",
            left_stream.get_access().get_base_path(),
            left_col.column,
            if needs_casting {
                ".map(OrderedFloat)"
            } else {
                ""
            }
        ));

        right_tuple.push(format!(
            "y{}.{}.clone(){}",
            right_stream.get_access().get_base_path(),
            right_col.column,
            if needs_casting {
                ".map(OrderedFloat)"
            } else {
                ""
            }
        ));
    }
    // Construct the join operation string
    let join_op = format!(
        ".{}({}, |x| ({}), |y| ({})).drop_key()",
        join_method,
        right_stream,
        left_tuple.join(", "),
        right_tuple.join(", ")
    );

    // Store the join operation in the left stream
    let stream = query_object.get_mut_stream(left_stream);
    stream.insert_op(join_op);

    let mut final_join_op = String::new();

    match join_type {
        JoinType::Left => {
            let right_stream_info = query_object.get_stream(right_stream);
            let right_stream_final_struct_names = right_stream_info.final_struct.keys().collect::<Vec<_>>();

            final_join_op.push_str(&format!(
                ".filter_map(|x| {{ if x.1.is_none() {{
                    Some((x.0, {}::default()))
                }} else {{
                    Some((x.0, x.1.unwrap()))
                }} }})",
                if right_stream_final_struct_names.len() > 1 {
                    right_stream_final_struct_names
                        .get(right_stream_final_struct_names.len() - 2)
                        .unwrap()
                        .to_string()
                } else {
                    format!("Struct_{}", right_stream_info.source_table)
                },
            ));
        }
        JoinType::Outer => {
            let left_stream_info = query_object.get_stream(left_stream);
            let right_stream_info = query_object.get_stream(right_stream);

            //Retireve the final struct names
            let left_stream_final_struct_names = left_stream_info.final_struct.keys().collect::<Vec<_>>();
            let right_stream_final_struct_names = right_stream_info.final_struct.keys().collect::<Vec<_>>();

            // Determine if left side is from a previous join by checking join_tree
            let left_is_join = left_stream_info.join_tree.is_some();

            // Get the current nesting level and structure
            let nesting_level = if left_is_join {
                left_stream_info
                    .join_tree
                    .as_ref()
                    .unwrap()
                    .get_nesting_level()
            } else {
                0
            };

            // Generate the left default expression based on nesting level
            let left_default = if nesting_level == 0 {
                format!(
                    "{}::default()",
                    if left_stream_final_struct_names.len() > 1 {
                        left_stream_final_struct_names
                            .get(left_stream_final_struct_names.len() - 2)
                            .unwrap()
                            .to_string()
                    } else {
                        format!("Struct_{}", left_stream_info.source_table)
                    }
                )
            } else if nesting_level == 1 {
                format!(
                    "({}::default(), {}::default())",
                    left_stream_info.get_initial_struct_name(),
                    left_stream_info.get_second_struct_name(query_object)
                )
            } else {
                // For deeper nesting, we need to reconstruct the entire nested tuple
                left_stream_info.generate_nested_default(query_object)
            };

            final_join_op.push_str(&format!(
                ".filter_map(|x| {{ if x.0.is_none() || x.1.is_none() {{
                    Some((
                        if x.0.is_none() {{ {} }} else {{ x.0.unwrap() }},
                        if x.1.is_none() {{ {}::default() }} else {{ x.1.unwrap() }}
                    ))
                }} else {{
                    Some((x.0.unwrap(), x.1.unwrap()))
                }} }})",
                left_default,
                if right_stream_final_struct_names.len() > 1 {
                    right_stream_final_struct_names
                        .get(right_stream_final_struct_names.len() - 2)
                        .unwrap()
                        .to_string()
                } else {
                    format!("Struct_{}", right_stream_info.source_table)
                },
            ));
        }
        JoinType::Inner => {
            // Inner join doesn't need filter_map
        }
    }

    // Create or update join tree
    let join_tree = JoinTree::Join {
        left: Box::new(match &query_object.get_stream(left_stream).join_tree {
            Some(tree) => tree.clone(),
            None => JoinTree::Leaf(left_stream.clone()),
        }),
        right: Box::new(match &query_object.get_stream(right_stream).join_tree {
            Some(tree) => tree.clone(),
            None => JoinTree::Leaf(right_stream.clone()),
        }),
    };

    // Store join tree and update access paths
    join_tree.update_access_paths(query_object);

    query_object.get_mut_stream(left_stream).join_tree = Some(join_tree);

    if !final_join_op.is_empty() {
        let temp = query_object.get_mut_stream(left_stream);
        temp.insert_op(final_join_op);
    }

    Ok(())
}
