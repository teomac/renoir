use indexmap::IndexMap;
use crate::dsl::ir::aqua::FromClause;
use crate::dsl::ir::aqua::QueryObject;
use crate::dsl::ir::aqua::r_utils::check_alias;

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
            struct_positions.get(&left_table_name)
                .expect(&format!("Could not find tuple position for table {}", left_table_name))
                .clone()
        };

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

        join_string.push_str(&format!(
            ".join(stream{}, |x| x{}.{}.clone(), |y| y.{}.clone()).drop_key()",
            struct_index,
            left_access,
            left_field,
            right_field
        ));

        // Update IndexMap after this join
        if i == 0 {
            // After first join: (t1, t2)
            struct_positions.insert(left_table_name, ".0".to_string());
            struct_positions.insert(right_table_name, ".1".to_string());
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