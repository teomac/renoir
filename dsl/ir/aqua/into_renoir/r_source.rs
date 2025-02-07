use crate::dsl::ir::aqua::r_utils::check_alias;
use crate::dsl::ir::aqua::FromClause;
use crate::dsl::ir::aqua::QueryObject;
use indexmap::IndexMap;

pub fn process_from_clause(from_clause: &FromClause, query_object: &mut QueryObject) -> String {
    if !query_object.has_join {
        return "".to_string();
    }

    let mut join_string = String::new();

    // Single IndexMap to track tuple access paths
    let mut struct_positions: IndexMap<String, String> = IndexMap::new();

    // Process each join in order
    for (i, join) in from_clause.joins.clone().unwrap().iter().enumerate() {
        let joined_table = &join.scan.stream_name;
        let left_col = &join.condition.left_col;
        let right_col = &join.condition.right_col;

        // Get the actual table names using check_alias
        let left_table_name = check_alias(&left_col.table.clone().unwrap(), &query_object);
        let right_table_name = check_alias(&right_col.table.clone().unwrap(), &query_object);

        // Get struct names
        let joined_struct = query_object.get_struct_name(&joined_table).unwrap();
        let struct_index = joined_struct.chars().last().unwrap();

        // Get the correct tuple access for the left table
        let left_access = if i == 0 {
            // First join - direct access
            String::new()
        } else {
            // Get access from our tracking structure
            struct_positions
                .get(&left_table_name)
                .expect(&format!(
                    "Could not find tuple position for table {}",
                    left_table_name
                ))
                .clone()
        };

        let left_field = if query_object
            .table_to_struct
            .get(&left_table_name)
            .unwrap()
            .get(&left_col.column)
            .is_some()
        {
            // If the column is in the struct, use it directly
            left_col.column.clone()
        } else {
            // If the column is not in the struct, use the validated field
            panic!(
                "Column {} not found in struct for table {}",
                left_col.column, left_table_name
            );
        };

        let right_field = if query_object
            .table_to_struct
            .get(&right_table_name)
            .unwrap()
            .get(&right_col.column)
            .is_some()
        {
            // If the column is in the struct, use it directly
            right_col.column.clone()
        } else {
            // If the column is not in the struct, use the validated field
            panic!(
                "Column {} not found in struct for table {}",
                right_col.column, right_table_name
            );
        };

        join_string.push_str(&format!(
            ".join(stream{}, |x| x{}.{}.clone(), |y| y.{}.clone()).drop_key()",
            struct_index, left_access, left_field, right_field
        ));

        // Update IndexMap after this join
        if i == 0 {
            // After first join: (t1, t2)
            struct_positions.insert(from_clause.scan.stream_name.clone(), ".0".to_string());
            struct_positions.insert(joined_table.clone(), ".1".to_string());
        } else {
            // Create temporary map to store new positions
            let mut new_positions = IndexMap::new();
            // Update all existing positions to be nested under .0
            for (table, pos) in struct_positions.iter() {
                new_positions.insert(table.clone(), format!(".0{}", pos));
            }
            // Add the new table at .1
            new_positions.insert(right_table_name, ".1".to_string());
            // Replace old positions with new ones
            struct_positions = new_positions;
        }
    }

    query_object.update_tuple_access(&struct_positions);

    join_string
}
