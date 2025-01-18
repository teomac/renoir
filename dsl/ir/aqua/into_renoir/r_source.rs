use crate::dsl::ir::aqua::FromClause;
use crate::dsl::ir::aqua::QueryObject;
use crate::dsl::ir::aqua::r_utils::check_alias;

pub fn process_from_clause(from_clause: &FromClause, query_object: &QueryObject) -> String {
    if !query_object.has_join {
        "".to_string();
    }

    if let Some(join) = &from_clause.join {
        let joined_table = &join.scan.stream_name;

        let left_col = &join.condition.left_col;
        let right_col = &join.condition.right_col;

        let first_struct = query_object.get_struct_name(&joined_table).unwrap();
        let first_index = first_struct.chars().last().unwrap();

        // check if left_col.table is not an alias in the query object hashmap
        let left_table_name = check_alias(&left_col.table.clone().unwrap(), &query_object);

        // same for right_col
        let right_table_name = check_alias(&right_col.table.clone().unwrap(), &query_object);


        let left_field = query_object
            .get_struct_field(
                &left_table_name, 
                &left_col.column
            )
            .unwrap();
        
        let right_field = query_object
            .get_struct_field(
                &right_table_name, 
                &right_col.column
            )
            .unwrap();

        
        format!(
            ".join(stream{}, |x| x.{}.clone(), |y| y.{}.clone()).drop_key()",
            first_index,
            left_field,
            right_field
        )
    } else {
        String::new()
    }
}