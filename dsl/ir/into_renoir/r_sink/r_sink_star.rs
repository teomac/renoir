use crate::dsl::struct_object::object::QueryObject;
use core::panic;

pub(crate) fn create_star_map(stream_name: &String, query_object: &QueryObject) -> String {
    let stream = query_object.get_stream(stream_name);
    let mut result = format!(".map(|x| {} {{ ", stream.final_struct_name.last().unwrap());

    //cases: JOIN -> WITH GROUP / WITHOUT GROUP
    //and
    // NO JOIN -> WITH GROUP / WITHOUT GROUP

    // Handle joined case - need to use tuple access
    //for stream in all_streams, build all the columns mapping in the .map
    let mut offset: usize = 0;
    let mut all_streams = Vec::new();

    //if it has a join tree, get all the streams involved in the join
    if stream.join_tree.is_some() {
        all_streams.extend(stream.join_tree.clone().unwrap().get_involved_streams());
    } else {
        all_streams.push(stream_name.clone());
    }

    let is_grouped = stream.is_keyed && !stream.key_columns.is_empty();

    if !is_grouped {
        for stream in all_streams.iter() {
            let stream = query_object.get_stream(stream);
            let tuple_access = stream.get_access().get_base_path();
            let table_struct = if stream.final_struct.is_empty() {
                query_object.get_struct(&stream.source_table).unwrap()
            } else {
                &stream.final_struct
            };

            for (column_index, field_name) in table_struct.iter().enumerate() {
                result.push_str(&format!(
                    "{}: x{}.{}, ",
                    query_object
                        .result_column_types
                        .get_index(offset + column_index)
                        .unwrap()
                        .0,
                    tuple_access,
                    field_name.0
                ));
            }

            offset += table_struct.len();
        }
    } else {
        //grouped case
        //retrieve the key columns
        let mut key_columns = Vec::new();
        for stream in all_streams.iter() {
            key_columns.extend(query_object.get_stream(stream).key_columns.clone());
        }
        for (index, key_column) in key_columns.iter().enumerate() {
            let is_single_key = key_columns.len() == 1;
            let col_table = key_column.table.clone().unwrap_or(String::new());
            let col_type = query_object.get_type(key_column);

            let col_stream_name = if col_table.is_empty() {
                stream_name
            } else {
                query_object.get_stream_from_alias(&col_table).unwrap()
            };

            let col_stream = query_object.get_stream(col_stream_name);
            if col_stream.check_if_column_exists(&key_column.column) {
                if is_single_key {
                    if col_type == "f64" {
                        result.push_str(&format!(
                            "{}: if x.0.is_some() {{ Some(x.0.unwrap().into_inner() as f64) }} else {{ None }},",
                            query_object
                                .result_column_types
                                .get_index(offset)
                                .unwrap()
                                .0,
                        ));
                    } else {
                        result.push_str(&format!(
                            "{}: {}x.0{},",
                            query_object
                                .result_column_types
                                .get_index(offset)
                                .unwrap()
                                .0,
                            if col_type == "bool" { "*" } else { "" },
                            if col_type == "String" { ".clone()" } else { "" }
                        ));
                    }
                } else if col_type == "f64" {
                    result.push_str(&format!(
                        "{}: if x.0.{}.is_some() {{ Some(x.0.{}.unwrap().into_inner() as f64) }} else {{ None }},",
                        query_object
                            .result_column_types
                            .get_index(offset)
                            .unwrap()
                            .0,
                        index,
                        index
                    ));
                } else {
                    result.push_str(&format!(
                        "{}: {}x.0.{}{},",
                        query_object
                            .result_column_types
                            .get_index(offset)
                            .unwrap()
                            .0,
                        if col_type == "bool" { "*" } else { "" },
                        index,
                        if col_type == "String" { ".clone()" } else { "" }
                    ));
                }
            } else {
                panic!(
                    "Column {} does not exist in stream {}",
                    key_column.column, col_stream_name
                );
            }
            offset += 1;
        }
    }
    result.push_str(" })");
    result
}
