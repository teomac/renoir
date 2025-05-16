use indexmap::IndexMap;

use crate::dsl::ir::{OrderByItem, OrderDirection, QueryObject};

/// Applies the distinct, order_by, and limit clauses to the stream.
pub(crate) fn process_distinct_order(stream_name: &String, query_object: &mut QueryObject) {
    let stream = query_object.streams.get(stream_name).unwrap();

    //if the stream has no distinct, nor order_by, nor limit, return
    if !stream.distinct && stream.order_by.is_empty() && stream.limit.is_none() {
        return;
    }

    let distinct = stream.distinct;
    let order_by = stream.order_by.clone();
    let limit = stream.limit;

    let final_struct = stream
        .final_struct
        .get(stream.final_struct.keys().last().unwrap())
        .unwrap()
        .clone();
    //map the output struct to an ordered float struct
    let final_struct_of_name = format!("{}_of", stream.final_struct.keys().last().unwrap());
    let mut final_struct_of = final_struct.clone();

    let mut needs_mapping = false;
    let mut forward_map = String::new();
    let mut backward_map = String::new();

    //replace all the float types with ordered float types in the final_struct_of
    for (_, value) in final_struct_of.iter_mut() {
        if value == "f64" {
            *value = "OrderedFloat<f64>".to_string();
            needs_mapping = true;
        }
    }

    if needs_mapping {
        //insert the final_struct_of into the query_object
        query_object
            .structs
            .insert(final_struct_of_name.clone(), final_struct_of.clone());

        // Create map operation to convert from original struct to OrderedFloat struct
        let mut forward_map_fields = String::new();
        for (field_name, field_type) in &final_struct {
            if field_type == "f64" {
                forward_map_fields.push_str(&format!(
               "                {}: if x.{}.is_some() {{ Some(OrderedFloat(x.{}.unwrap())) }} else {{ None }},\n",
               field_name, field_name, field_name
           ));
            } else {
                forward_map_fields.push_str(&format!(
                    "                {}: x.{},\n",
                    field_name, field_name
                ));
            }
        }

        // Create map operation to convert back from OrderedFloat struct to original struct
        let mut backward_map_fields = String::new();
        for (field_name, field_type) in &final_struct {
            if field_type == "f64" {
                backward_map_fields.push_str(&format!(
               "                {}: if x.{}.is_some() {{ Some(x.{}.unwrap().into_inner()) }} else {{ None }},\n",
               field_name, field_name, field_name
           ));
            } else {
                backward_map_fields.push_str(&format!(
                    "                {}: x.{},\n",
                    field_name, field_name
                ));
            }
        }

        // Create the complete distinct operation chain
        forward_map = format!(
            ".map(move |x| {} {{\n{}\n            }})",
            final_struct_of_name.clone(),
            forward_map_fields
        );
        backward_map = format!(
            ".map(move |x| {} {{\n{}\n            }})",
            stream.final_struct.keys().last().unwrap(),
            backward_map_fields
        );
    }

    //now let's process distinct, order_by and limit
    let unique_op = if distinct {
        ".unique_assoc()".to_string()
    } else {
        String::new()
    };

    let order_op = if !order_by.is_empty() {
        if limit.is_none() {
            format!(
                ".sorted_by({})",
                generate_sort_code(order_by, final_struct_of)
            )
        } else {
            //check if offset exists
            let (limit, offset) = limit.unwrap();

            format!(
                ".sorted_limit_by({}, {}, Some({}))",
                generate_sort_code(order_by, final_struct_of),
                limit,
                offset
            )
        }
    } else {
        String::new()
    };

    let limit_op = if limit.is_some() && order_op.is_empty() {
        let (limit, offset) = limit.unwrap();
        format!(".limit({}, Some({}))", limit, offset)
    } else {
        String::new()
    };

    let stream_mut = query_object.get_mut_stream(stream_name);
    if !forward_map.is_empty() {
        stream_mut.op_chain.push(forward_map)
    };
    if distinct {
        stream_mut.op_chain.push(unique_op)
    };
    if !order_op.is_empty() {
        stream_mut.op_chain.push(order_op);
    }
    if !limit_op.is_empty() {
        stream_mut.op_chain.push(limit_op);
    }
    if !backward_map.is_empty() {
        stream_mut.op_chain.push(backward_map)
    };
}

fn generate_sort_code(
    order_by: Vec<OrderByItem>,
    final_struct_of: IndexMap<String, String>,
) -> String {
    // Build sorting comparison function based on the order_by vector
    let mut sort_fn = String::new();
    sort_fn.push_str("|a,b| ");

    // Iterate through order_by in reverse to apply sorting in correct order
    let mut order_conditions = Vec::new();

    for item in order_by.iter() {
        let col_name = item.column.to_owned();
        let direction = match item.direction {
            OrderDirection::Asc => "asc",
            OrderDirection::Desc => "desc",
        };
        // Determine null handling behavior
        // Default behavior: nulls last for ASC, nulls first for DESC
        let nulls_first = item.nulls_first.unwrap_or_else(|| direction == "desc");

        let mut field_name = if col_name.table.is_some() {
            format!("{}_{}", col_name.column, col_name.table.as_ref().unwrap())
        } else {
            //check if the final struct has the field_name
            //if not, we need to add the table name to the field name

            if final_struct_of.get(&col_name.column).is_some() {
                col_name.column.clone()
            } else {
                //take the table name from the final_struct
                let key = final_struct_of.keys().last().unwrap();
                let table_name = key.rsplit_once("_").unwrap().1;
                format!("{}_{}", col_name.column, table_name)
            }
        };

        println!("field_name: {}", field_name);
        println!("final_struct_of: {:?}", final_struct_of);
        let field_type = if final_struct_of.get(&field_name).is_some() {
            final_struct_of.get(&field_name).unwrap()
        } else if final_struct_of.get(&col_name.column).is_some() {
            field_name = col_name.column.clone();
            final_struct_of.get(&col_name.column).unwrap()
        } else {
            //we need to iterate on all the keys of the final struct to find one key that contains the field_name
            let key = final_struct_of
                .keys()
                .find(|key| key.contains(&field_name))
                .unwrap();
            final_struct_of.get(key).unwrap()
        };

        // Handle different field types and sort directions including null handling
        let comparison = if field_type == "f64" || field_type == "OrderedFloat<f64>" {
            // For floating point fields
            if nulls_first {
                if direction == "desc" {
                    format!(
                        "match (a.{}.is_some(), b.{}.is_some()) {{
                        (false, false) => std::cmp::Ordering::Equal,
                        (false, true) => std::cmp::Ordering::Less,
                        (true, false) => std::cmp::Ordering::Greater,
                        (true, true) => std::cmp::Ord::cmp(
                            &b.{}.as_ref().unwrap(),
                            &a.{}.as_ref().unwrap()
                        )
                    }}",
                        field_name, field_name, field_name, field_name
                    )
                } else {
                    format!(
                        "match (a.{}.is_some(), b.{}.is_some()) {{
                        (false, false) => std::cmp::Ordering::Equal,
                        (false, true) => std::cmp::Ordering::Less,
                        (true, false) => std::cmp::Ordering::Greater,
                        (true, true) => std::cmp::Ord::cmp(
                            &a.{}.as_ref().unwrap(),
                            &b.{}.as_ref().unwrap()
                        )
                    }}",
                        field_name, field_name, field_name, field_name
                    )
                }
            } else {
                // nulls last
                if direction == "desc" {
                    format!(
                        "match (a.{}.is_some(), b.{}.is_some()) {{
                        (false, false) => std::cmp::Ordering::Equal,
                        (false, true) => std::cmp::Ordering::Greater,
                        (true, false) => std::cmp::Ordering::Less,
                        (true, true) => std::cmp::Ord::cmp(
                            &b.{}.as_ref().unwrap(),
                            &a.{}.as_ref().unwrap()
                        )
                    }}",
                        field_name, field_name, field_name, field_name
                    )
                } else {
                    format!(
                        "match (a.{}.is_some(), b.{}.is_some()) {{
                        (false, false) => std::cmp::Ordering::Equal,
                        (false, true) => std::cmp::Ordering::Greater,
                        (true, false) => std::cmp::Ordering::Less,
                        (true, true) => std::cmp::Ord::cmp(
                            &a.{}.as_ref().unwrap(),
                            &b.{}.as_ref().unwrap()
                        )
                    }}",
                        field_name, field_name, field_name, field_name
                    )
                }
            }
        } else if field_type == "String" {
            // For string fields
            if nulls_first {
                if direction == "desc" {
                    format!(
                        "match (a.{}.is_some(), b.{}.is_some()) {{
                        (false, false) => std::cmp::Ordering::Equal,
                        (false, true) => std::cmp::Ordering::Less,
                        (true, false) => std::cmp::Ordering::Greater,
                        (true, true) => b.{}.as_ref().unwrap().cmp(a.{}.as_ref().unwrap())
                    }}",
                        field_name, field_name, field_name, field_name
                    )
                } else {
                    format!(
                        "match (a.{}.is_some(), b.{}.is_some()) {{
                        (false, false) => std::cmp::Ordering::Equal,
                        (false, true) => std::cmp::Ordering::Less,
                        (true, false) => std::cmp::Ordering::Greater,
                        (true, true) => a.{}.as_ref().unwrap().cmp(b.{}.as_ref().unwrap())
                    }}",
                        field_name, field_name, field_name, field_name
                    )
                }
            } else {
                // nulls last
                if direction == "desc" {
                    format!(
                        "match (a.{}.is_some(), b.{}.is_some()) {{
                        (false, false) => std::cmp::Ordering::Equal,
                        (false, true) => std::cmp::Ordering::Greater,
                        (true, false) => std::cmp::Ordering::Less,
                        (true, true) => b.{}.as_ref().unwrap().cmp(a.{}.as_ref().unwrap())
                    }}",
                        field_name, field_name, field_name, field_name
                    )
                } else {
                    format!(
                        "match (a.{}.is_some(), b.{}.is_some()) {{
                        (false, false) => std::cmp::Ordering::Equal,
                        (false, true) => std::cmp::Ordering::Greater,
                        (true, false) => std::cmp::Ordering::Less,
                        (true, true) => a.{}.as_ref().unwrap().cmp(b.{}.as_ref().unwrap())
                    }}",
                        field_name, field_name, field_name, field_name
                    )
                }
            }
        } else {
            // Default comparison for other types
            if nulls_first {
                if direction == "desc" {
                    format!(
                        "match (a.{}.is_some(), b.{}.is_some()) {{
                        (false, false) => std::cmp::Ordering::Equal,
                        (false, true) => std::cmp::Ordering::Less,
                        (true, false) => std::cmp::Ordering::Greater,
                        (true, true) => b.{}.cmp(&a.{})
                    }}",
                        field_name, field_name, field_name, field_name
                    )
                } else {
                    format!(
                        "match (a.{}.is_some(), b.{}.is_some()) {{
                        (false, false) => std::cmp::Ordering::Equal,
                        (false, true) => std::cmp::Ordering::Less,
                        (true, false) => std::cmp::Ordering::Greater,
                        (true, true) => a.{}.cmp(&b.{})
                    }}",
                        field_name, field_name, field_name, field_name
                    )
                }
            } else {
                // nulls last
                if direction == "desc" {
                    format!(
                        "match (a.{}.is_some(), b.{}.is_some()) {{
                        (false, false) => std::cmp::Ordering::Equal,
                        (false, true) => std::cmp::Ordering::Greater,
                        (true, false) => std::cmp::Ordering::Less,
                        (true, true) => b.{}.cmp(&a.{})
                    }}",
                        field_name, field_name, field_name, field_name
                    )
                } else {
                    format!(
                        "match (a.{}.is_some(), b.{}.is_some()) {{
                        (false, false) => std::cmp::Ordering::Equal,
                        (false, true) => std::cmp::Ordering::Greater,
                        (true, false) => std::cmp::Ordering::Less,
                        (true, true) => a.{}.cmp(&b.{})
                    }}",
                        field_name, field_name, field_name, field_name
                    )
                }
            }
        };

        order_conditions.push(comparison);
    }

    // Combine conditions with .then_with()
    if order_conditions.len() == 1 {
        sort_fn.push_str(&order_conditions[0]);
    } else {
        sort_fn.push_str(&order_conditions[0]);
        for condition in &order_conditions[1..] {
            sort_fn.push_str(&format!(".then_with(|| {})", condition));
        }
    }

    sort_fn
}
