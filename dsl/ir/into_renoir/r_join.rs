use crate::dsl::ir::ir_ast_structure::*;
use crate::dsl::ir::QueryObject;
use crate::dsl::struct_object::support_structs::JoinTree;
use crate::dsl::struct_object::utils::*;

pub fn process_join(
    left_stream: &String,
    right_stream: &String, 
    conditions: &Vec<JoinCondition>,
    join_type: &JoinType,
    query_object: &mut QueryObject) -> Result<(), Box<dyn std::error::Error>> {

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
        let mut left_stream_name = query_object.get_stream_from_alias(left_col.table.as_ref().unwrap()).unwrap().clone();
        let mut right_stream_name = query_object.get_stream_from_alias(right_col.table.as_ref().unwrap()).unwrap().clone();

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
        let needs_casting = query_object.get_type(&left_col) == "f64" 
            || query_object.get_type(&right_col) == "f64";

        left_tuple.push(format!("x{}.{}.clone(){}",
            left_stream.get_access().get_base_path(),
            left_col.column,
            if needs_casting { ".map(OrderedFloat)" } else { "" }
        ));

        right_tuple.push(format!("y{}.{}.clone(){}",
            right_stream.get_access().get_base_path(),
            right_col.column,
            if needs_casting { ".map(OrderedFloat)" } else { "" }
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

         // Create or update join tree
    let join_tree = JoinTree::Join {
        left: Box::new(match &query_object.get_stream(left_stream).join_tree {
            Some(tree) => tree.clone(),
            None => JoinTree::Leaf(left_stream.clone())
        }),
        right: Box::new(match &query_object.get_stream(right_stream).join_tree {
            Some(tree) => tree.clone(),
            None => JoinTree::Leaf(right_stream.clone())
        }),
        join_type: join_type.clone()
    };

    // Store join tree and update access paths
    join_tree.update_access_paths(query_object);

    query_object.get_mut_stream(left_stream).join_tree = Some(join_tree);

    Ok(())
}
