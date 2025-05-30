use crate::dsl::ir::ColumnRef;
use crate::dsl::struct_object::object::QueryObject;

///Checks if the column reference is valid in the context of the given stream name and query object.
pub(crate) fn check_column_validity(
    col_ref: &ColumnRef,
    stream_name: &String,
    query_object: &QueryObject,
) { 
    //check if the col ref corresponds to a real column
    let col_to_check = col_ref.column.clone();
    if col_ref.table.is_some() {
        let alias = col_ref.table.as_ref().unwrap();

        //check if the alias corresponds to the actual stream
        if query_object.alias_to_stream.contains_key(alias)
            && query_object.alias_to_stream.get(alias).unwrap() != stream_name
        {
            panic!(
                "Alias {} does not correspond to the actual stream. Stream name: {}",
                alias, stream_name
            );
        }

        //get the struct map for the table
        let table_name = query_object.get_stream(stream_name).source_table.clone();

        let struct_map = query_object
            .tables_info
            .get(&table_name)
            .unwrap_or_else(|| {
                panic!("Error in retrieving struct_map for table {}.", alias);
            });
        if !struct_map.contains_key(&col_to_check) 
        && !struct_map.contains_key(format!("{}_{}", col_to_check, table_name).as_str()) 
        && !query_object.get_stream(stream_name).initial_columns.contains_key(&col_to_check) && !query_object.get_stream(stream_name).initial_columns.contains_key(format!("{}_{}", col_to_check, table_name).as_str()) {
            panic!("Column {} does not exist in table {}", col_to_check, alias);
        }
    } else {
        let mut found = false;
        if !stream_name.is_empty() {
            let table = query_object.get_stream(stream_name).source_table.clone();
            let struct_map = query_object.tables_info.get(&table).unwrap();
            if struct_map.contains_key(&col_to_check) {
                found = true;
            } else{
                //let's check on the final struct
                let final_struct = query_object.get_stream(stream_name).final_struct.clone();
                let last_struct  = final_struct.get(final_struct.keys().last().unwrap()).unwrap();
                if last_struct.contains_key(&col_to_check) {
                    found = true;
                } else{
                    //Check if the column exists in the initial columns
                    if query_object.get_stream(stream_name).initial_columns.contains_key(&col_to_check) ||
                       query_object.get_stream(stream_name).initial_columns.contains_key(format!("{}_{}", col_to_check, table).as_str()) {
                        found = true;
                    }
                }
            }
        }
        if !found {
            panic!("Column {} does not exist in any table", col_to_check);
        }
    }
}
