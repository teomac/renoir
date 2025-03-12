use crate::dsl::struct_object::object::QueryObject;
use crate::dsl::ir::ColumnRef;

pub fn check_column_validity(col_ref: &ColumnRef, stream_name: &String, query_object: &QueryObject) {
    //check if the col ref corresponds to a real column
    let col_to_check = col_ref.column.clone();
    if col_ref.table.is_some() {
        let alias = col_ref.table.as_ref().unwrap();

        //check if the alias corresponds to the actual stream
        if query_object.alias_to_stream.contains_key(alias) {
            if query_object.alias_to_stream.get(alias).unwrap() != stream_name {
                panic!(
                    "Alias {} does not correspond to the actual stream. Stream name: {}",
                    alias, stream_name
                );
            }
        }

        //get the struct map for the table
        let table_name = query_object.get_stream(stream_name).source_table.clone();
        let struct_map = query_object.tables_info.get(&table_name).unwrap_or_else(|| {
            panic!("Error in retrieving struct_map for table {}.", alias);
        });
        if !struct_map.contains_key(&col_to_check) {
            panic!("Column {} does not exist in table {}", col_to_check, alias);
        }
    } else {
        let mut found = false;
        if !stream_name.is_empty() {
            let table = query_object.get_stream(stream_name).source_table.clone();
            let struct_map = query_object.tables_info.get(&table).unwrap();
            if struct_map.contains_key(&col_to_check) {
                found = true;
            }
        }
        if !found {
            panic!("Column {} does not exist in any table", col_to_check);
        }
    }
}