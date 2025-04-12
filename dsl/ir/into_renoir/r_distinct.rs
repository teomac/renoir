use crate::dsl::ir::QueryObject;

pub fn process_distinct_old(query_object: &mut QueryObject) {
    let csv_path = query_object.output_path.replace("\\", "/");

    // Generate code to remove duplicates from the CSV
    let distinct_code = format!(
        r#"
        // Process DISTINCT - remove duplicate rows from CSV
        let mut rdr = csv::Reader::from_path(format!("{}.csv")).unwrap();
        let mut wtr = csv::Writer::from_path(format!("{}_distinct.csv")).unwrap();
        
        // Copy the header
        let headers = rdr.headers().unwrap().clone();
        wtr.write_record(&headers).unwrap();
        
        // Use a HashSet to track unique rows
        let mut seen_rows = std::collections::HashSet::new();
        
        // Process records and keep only unique ones
        for result in rdr.records() {{
            if let Ok(record) = result {{
                // Convert record to a string that can be hashed
                let record_str = record.iter().collect::<Vec<_>>().join("\t");
                
                // Only write this record if we haven't seen it before
                if seen_rows.insert(record_str) {{
                    wtr.write_record(&record).unwrap();
                }}
            }}
        }}
        
        wtr.flush().unwrap();
        drop(wtr);
        drop(rdr);
        
        // Replace original file with distinct version
        std::fs::rename(format!("{}_distinct.csv"), format!("{}.csv")).unwrap();
        "#,
        csv_path, csv_path, csv_path, csv_path
    );

    query_object.distinct_string = distinct_code;
}

pub fn process_distinct_order(stream_name: &String, query_object: &mut QueryObject) {
    let stream = query_object.streams.get(stream_name).unwrap();

    //if the stream has no distinct, nor order_by, nor limit, return
    if !stream.distinct && stream.order_by.is_empty() && stream.limit.is_none() {
        return;
    }

    let distinct = stream.distinct;
    let order_by = stream.order_by.clone();
    let limit = stream.limit.clone();

    let final_struct = stream.final_struct.clone();
    //map the output struct to an ordered float struct
    let final_struct_of_name = format!("{}_of", stream.final_struct_name.clone().last().unwrap());
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
            stream.final_struct_name.last().unwrap(),
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
            // Build sorting comparison function based on the order_by vector
            let mut sort_fn = String::new();
            sort_fn.push_str("|a,b| ");

            // Iterate through order_by in reverse to apply sorting in correct order
            let mut order_conditions = Vec::new();

            for (col_name, direction) in order_by.iter() {
                let field_name = if col_name.table.is_some() {
                    format!("{}.{}", col_name.column, col_name.table.as_ref().unwrap())
                } else {
                    col_name.column.clone()
                };

                let field_type = if final_struct_of.get(&field_name).is_some() {
                    final_struct_of.get(&field_name).unwrap()
                } else {
                    //we need to iterate on all the keys of the final struct to find one key that contains the field_name
                    let key = final_struct_of
                        .keys()
                        .find(|key| key.contains(&field_name))
                        .unwrap();
                    final_struct_of.get(key).unwrap()
                };

                // Handle different field types and sort directions
                let comparison = if field_type == "f64" || field_type == "OrderedFloat<f64>" {
                    // For floating point fields using OrderedFloat
                    if direction == "desc" {
                        format!("std::cmp::Ord::cmp(\n                    &b.{}.as_ref().unwrap_or(&OrderedFloat(f64::MIN)),\n                    &a.{}.as_ref().unwrap_or(&OrderedFloat(f64::MIN))\n                )", col_name, col_name)
                    } else {
                        format!("std::cmp::Ord::cmp(\n                    &a.{}.as_ref().unwrap_or(&OrderedFloat(f64::MIN)),\n                    &b.{}.as_ref().unwrap_or(&OrderedFloat(f64::MIN))\n                )", col_name, col_name)
                    }
                } else if field_type == "String" {
                    // For string fields
                    if direction == "desc" {
                        format!("b.{}.as_ref().unwrap_or(&String::new()).cmp(a.{}.as_ref().unwrap_or(&String::new()))", col_name, col_name)
                    } else {
                        format!("a.{}.as_ref().unwrap_or(&String::new()).cmp(b.{}.as_ref().unwrap_or(&String::new()))", col_name, col_name)
                    }
                } else {
                    // Default comparison for other types
                    if direction == "desc" {
                        format!("b.{}.cmp(&a.{})", col_name, col_name)
                    } else {
                        format!("a.{}.cmp(&b.{})", col_name, col_name)
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

            format!(".sorted_by({})", sort_fn)
        } else {
            //TODO
            //need to use .sorted_limit_by()
            "TODO".to_string()
        }
    } else {
        String::new()
    };

    let limit_op = if limit.is_some() && order_op.is_empty() {
        //TODO
        //need to use .limit()
        "TODO".to_string()
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
