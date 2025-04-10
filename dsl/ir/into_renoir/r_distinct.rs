use core::panic;

use crate::dsl::{ir::QueryObject, struct_object::support_structs::StreamInfo};

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

pub fn process_distinct_new(stream_name: &String, query_object: &mut QueryObject) {
    let stream = query_object.streams.get(stream_name).unwrap();
    let final_struct = stream.final_struct.clone();
    //map the output struct to an ordered float struct
    let final_struct_of_name = format!("{}_of", stream.final_struct_name.clone().last().unwrap());
    let mut final_struct_of = final_struct.clone();

    let mut needs_mapping = false;
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
        let forward_map = format!(
            ".map(move |x| {} {{\n{}\n            }})",
            final_struct_of_name.clone(),
            forward_map_fields
        );

        let unique_op = ".unique_assoc()".to_string();

        let backward_map = format!(
            ".map(move |x| {} {{\n{}\n            }})",
            stream.final_struct_name.last().unwrap(),
            backward_map_fields
        );

        let stream_mut = query_object.get_mut_stream(stream_name);
        stream_mut.op_chain.push(forward_map);
        stream_mut.op_chain.push(unique_op);
        stream_mut.op_chain.push(backward_map);
    } else {
        let stream_mut = query_object.get_mut_stream(stream_name);
        stream_mut.op_chain.push(".unique_assoc()".to_string());
    }
}

pub fn process_distinct(stream_info: &StreamInfo, is_subquery: bool) -> String {
    let stream_name = &stream_info.id;

    if is_subquery {
        format!(
            r#"
            let mut seen = indexmap::IndexSet::new();
            {}_result.into_iter().for_each(|item| {{ seen.insert(item); }});
            let {}_result = seen.into_iter().collect::<Vec<_>>();
            "#,
            stream_name, stream_name
        )
    } else {
        panic!("Distinct processing for non-subquery is not implemented yet.")
    }
}
