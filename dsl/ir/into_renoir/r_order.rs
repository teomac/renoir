use crate::dsl::ir::ir_ast_structure::{OrderByClause, OrderDirection};
use crate::dsl::ir::QueryObject;

/// Process the OrderByClause and generate sorting code for the output CSV.
/// This function assumes the CSV has already been written and will sort it in place.
///
/// # Arguments
/// * `order_by` - The OrderByClause containing columns and sort directions
/// * `query_object` - The QueryObject containing metadata about tables and columns
///
/// # Returns
/// A String containing the Rust code to sort the CSV
pub fn process_order_by(order_by: &OrderByClause, query_object: &QueryObject) -> String {
    let mut order_string = String::new();

    let csv_path = query_object.output_path.replace("\\", "/");

    // Open the CSV for reading
    order_string.push_str(&format!(
        r#"
        // Sort the output CSV based on order by clause
        let mut rdr = csv::Reader::from_path(format!("{}.csv")).unwrap();
        let mut records: Vec<csv::StringRecord> = rdr.records().map(|r| r.unwrap()).collect();
        let headers = rdr.headers().unwrap().clone();
        
        // Get column indices for sorting
        "#,
        csv_path
    ));

    // save keys of result_column_types in a vector
    let mut result_column_types_keys = Vec::new();
    for key in query_object.result_column_types.keys() {
        result_column_types_keys.push(key.clone());
    }

    let mut order_by_items = order_by.items.clone();

    for item in &mut order_by_items {
        if result_column_types_keys.iter().any(|x| x.contains(&item.column.column)) {
            let x = result_column_types_keys.iter().find(|x| x.contains(&item.column.column)).unwrap();
            item.column.column = x.clone();
            
            order_string.push_str(&format!(
                r#"let {}_idx = headers.iter().position(|h| h == "{}").unwrap();"#,
                x, x
            ));
        }
    }


    // Generate sorting code
    order_string.push_str(r#"
        records.sort_by(|a, b| {
            "#);

    let length = &order_by_items.len();

    // Generate comparison chain for each column
    for i in 0..*length {
        let column_name = order_by_items[i].column.column.clone();
        let column_type = query_object.result_column_types.get(&column_name).unwrap();
        let comparison = match (column_type.as_str(), &order_by_items[i].direction) {
            ("f64", OrderDirection::Asc) => format!(
                "a.get({}_idx).unwrap().parse::<f64>().unwrap().partial_cmp(&b.get({}_idx).unwrap().parse::<f64>().unwrap()).unwrap()",
                column_name, column_name
            ),
            ("f64", OrderDirection::Desc) => format!(
                "b.get({}_idx).unwrap().parse::<f64>().unwrap().partial_cmp(&a.get({}_idx).unwrap().parse::<f64>().unwrap()).unwrap()",
                column_name, column_name
            ),
            ("i64", OrderDirection::Asc) => format!(
                "a.get({}_idx).unwrap().parse::<i64>().unwrap().cmp(&b.get({}_idx).unwrap().parse::<i64>().unwrap())",
                column_name, column_name
            ),
            ("i64", OrderDirection::Desc) => format!(
                "b.get({}_idx).unwrap().parse::<i64>().unwrap().cmp(&a.get({}_idx).unwrap().parse::<i64>().unwrap())",
                column_name, column_name
            ),
            ("bool", OrderDirection::Asc) => format!(
                "a.get({}_idx).unwrap().parse::<bool>().unwrap().cmp(&b.get({}_idx).unwrap().parse::<bool>().unwrap())",
                column_name, column_name
            ),
            ("bool", OrderDirection::Desc) => format!(
                "b.get({}_idx).unwrap().parse::<bool>().unwrap().cmp(&a.get({}_idx).unwrap().parse::<bool>().unwrap())",
                column_name, column_name
            ),
            ("String", OrderDirection::Asc) => format!(
                "a.get({}_idx).unwrap().cmp(&b.get({}_idx).unwrap())",
                column_name, column_name
            ),
            ("String", OrderDirection::Desc) => format!(
                "b.get({}_idx).unwrap().cmp(&a.get({}_idx).unwrap())",
                column_name, column_name
            ),
            _ => panic!("Unsupported type for column {}", column_name)
        };
        
        if i > 0 {
            order_string.push_str(".then_with(|| ");
        }
        order_string.push_str(&format!("{}", comparison));
        if i > 0 {
            order_string.push(')');
        }
    }

    // Close the sort closure and write sorted records
    order_string.push_str(&format!(r#"
        }});

        // Write sorted records back to CSV
        let mut wtr = csv::Writer::from_path(format!("{}_sorted.csv")).unwrap();
        wtr.write_record(&headers).unwrap();
        for record in records {{
            wtr.write_record(&record).unwrap();
        }}
        wtr.flush().unwrap();
        drop(wtr);
        drop(rdr);

        // Replace original file with sorted file
        std::fs::rename(format!("{}_sorted.csv"), format!("{}.csv")).unwrap();
    "#,
    csv_path,
    csv_path,
    csv_path,        
    ).to_string());

    order_string
}