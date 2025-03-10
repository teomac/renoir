use crate::dsl::ir::ir_ast_structure::*;
use crate::dsl::ir::FromClause;
use crate::dsl::ir::QueryObject;

pub fn process_from_clause(from_clause: &FromClause, query_object: &mut QueryObject) -> Result<(), Box<dyn std::error::Error>> {
    if !query_object.has_join {
        Ok(())
    }
    //case with at least one join
    else{
    let mut stream_list_join: Vec<String> = Vec::new();
    stream_list_join.push(from_clause.scan.stream_name.clone());


    let mut join_string: String = String::new();

    // Process each join in order
    for (i, join) in from_clause.joins.clone().unwrap().iter().enumerate() {
        let joined_stream = &join.join_scan.stream_name;

        stream_list_join.push(joined_stream.clone());

        let mut left_tuple: Vec<String> = Vec::new();
        let mut right_tuple: Vec<String> = Vec::new();
    

        for (_j, join) in join.condition.conditions.iter().enumerate() {
            let mut left_col = join.left_col.clone();
            let mut right_col = join.right_col.clone();

            // Get the stream name from the alias
            let mut left_stream_name = query_object.get_stream_from_alias(&left_col.table.as_ref().unwrap()).unwrap().clone();
            let mut right_stream_name = query_object.get_stream_from_alias(&right_col.table.as_ref().unwrap()).unwrap().clone();


            //validate left and right columns
            query_object.check_column_validity(&left_col, &left_stream_name);
            query_object.check_column_validity(&right_col, &right_stream_name);

            // check if left and right col need to be swapped
            if left_stream_name == *joined_stream {
                let temp = left_col.clone();
                left_col = right_col.clone();
                right_col = temp.clone();

                let temp2 = left_stream_name.clone();
                left_stream_name = right_stream_name.clone();
                right_stream_name = temp2.clone();
            }

            let left_stream = query_object.get_stream(&left_stream_name);
            let right_stream = query_object.get_stream(&right_stream_name);

            // Get the correct tuple access for the left table
            let left_access = if i == 0 {
                // First join - direct access
                String::new()
            } else {
                // Get access from our tracking structure
                left_stream.get_access().get_base_path()
            };

            let left_field = if left_stream.check_if_column_exists(&left_col.column) {
                // If the column is in the struct, use it directly
                left_col.column.clone()
            } else {
                // If the column is not in the struct, use the validated field
                panic!(
                    "Column {} not found in struct for table {}",
                    left_col.column, left_stream_name
                );
            };

            let right_access = 
                // Get access from our tracking structure
                right_stream.get_access().get_base_path()
            ;

            let right_field = if right_stream.check_if_column_exists(&right_col.column) {
                // If the column is in the struct, use it directly
                right_col.column.clone()
            } else {
                // If the column is not in the struct, use the validated field
                panic!(
                    "Column {} not found in struct for table {}",
                    right_col.column, right_stream_name
                );
            };

            let left_field_type = left_stream.get_field_type(&left_field);
            let right_field_type = right_stream.get_field_type(&right_field);

            if left_field_type != right_field_type {
                panic!(
                    "Field types do not match for join: {} ({}) and {} ({})",
                    left_field, left_field_type, right_field, right_field_type
                );
            }

            let needs_casting = left_field_type == "f64" || right_field_type == "f64";

            left_tuple.push(format!("x{}.{}.clone() {}", left_access, left_field, if needs_casting { ".map(OrderedFloat)" } else { "" }));
            right_tuple.push(format!("y{}.{}.clone() {}", right_access, right_field, if needs_casting { ".map(OrderedFloat)" } else { "" }));
        }

        // Determine the join method based on the join type
        let join_type = match join.join_type {
            JoinType::Inner => "join",
            JoinType::Left => "left_join",
            JoinType::Outer => "outer_join",
        };

        let join_op= format!(
            ".{}({}, |x| ({}), |y| ({})).drop_key()",
            join_type,
            joined_stream,
            left_tuple.join(", "),
            right_tuple.join(", ")
        );
        join_string.push_str(&join_op);

        let stream0 = query_object.get_mut_stream(&stream_list_join[0]);
        stream0.insert_op(join_op);

        // Update IndexMap after this join
        if i == 0 {
            // After first join: (t1, t2)

            //we need to update the tuple access in the two streams of the join
            let updated_first_access = format!(".0{}", query_object.get_stream(&stream_list_join[0]).get_access().get_base_path());
            let updated_second_access = format!(".1{}", query_object.get_stream(&stream_list_join[1]).get_access().get_base_path());

            query_object.streams.get_mut(&stream_list_join[0]).unwrap().access.update_base_path(updated_first_access);
            query_object.streams.get_mut(&stream_list_join[1]).unwrap().access.update_base_path(updated_second_access);

        } else {
            //this is the case in which we have more than one join
            // After second join: ((t1, t2), t3)

            //we need to update the tuple access in all the streams of the join
            for i in 0..(stream_list_join.len()-1) {
                let updated_access = format!(".0{}", query_object.get_stream(&stream_list_join[i]).get_access().get_base_path());
                query_object.streams.get_mut(&stream_list_join[i]).unwrap().access.update_base_path(updated_access);
            }

            let joined_stream = query_object.get_stream(&stream_list_join[stream_list_join.len()-1]);
            let updated_access = format!(".1{}", joined_stream.get_access().get_base_path());
            query_object.streams.get_mut(&stream_list_join[stream_list_join.len()-1]).unwrap().access.update_base_path(updated_access);
        }
    }

    Ok(())
}}
