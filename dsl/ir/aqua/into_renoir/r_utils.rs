use crate::dsl::ir::aqua::{AggregateFunction, AggregateType, AquaLiteral};
use crate::dsl::ir::aqua::ColumnRef;
use crate::dsl::ir::aqua::QueryObject;

// helper function to convert column reference to string
pub fn convert_column_ref(column_ref: &ColumnRef, query_object: &QueryObject) -> String {
    let table_names = query_object.get_all_table_names();

    if column_ref.column == "*" {
        if !query_object.has_join {
            return "x".to_string();
        } else {
            let val = column_ref.table.clone().unwrap();
            let table_name = if query_object.table_to_alias.contains_key(&val) {
                val
            } else {
                query_object.table_names_list.iter().find(|&x| x == &val).unwrap().clone()
            };
            return format!("x{}", query_object.table_to_tuple_access.get(&table_name).unwrap());
        }
    }

    if !query_object.has_join {
        let table_name = table_names.first().unwrap();
        println!("table to struct : {:?}", query_object.table_to_struct);
        let col = if query_object.table_to_struct.get(table_name).unwrap().get(&column_ref.column).is_some() {
            format!("{}", column_ref.column)
        } else {
            //throw error
            panic!("Column {} does not exist in table {}", column_ref.column, table_name);
        };
        format!("x.{}", col)
    } else {
        // take value from column_ref.table
        let val = column_ref.table.clone().unwrap();
        // check if value is an alias in the query object hashmap
        let table_name;
        if query_object.table_to_alias.contains_key(&val) {
            table_name = val;
        }
        // else it's a table name
        else {
            table_name = query_object.table_names_list.iter().find(|&x| x == &val).unwrap().clone();
        }

        let col = if query_object.table_to_struct.get(&table_name).unwrap().get(&column_ref.column).is_some() {
            format!("{}", column_ref.column)
        } else {
            //throw error
            panic!("Column {} does not exist in table {}", column_ref.column, table_name);
        };
        let i = query_object.table_to_struct_name.get(&table_name).unwrap().chars().last().unwrap();
        if !query_object.has_join {
            return format!("x.{}.{}", i, col)
        } else {
            return format!("x{}.{}", query_object.table_to_tuple_access.get(&table_name).unwrap(), col)
        }
        
    }
    
}

// helper function to convert literal to string
pub fn convert_literal(literal: &AquaLiteral) -> String {
    match literal {
        AquaLiteral::Integer(val) => format!("{}", val),
        AquaLiteral::Float(val) => format!("{:.2}", val),
        AquaLiteral::String(val) => format!("{}", val),
        AquaLiteral::Boolean(val) => format!("{}", val),
        AquaLiteral::ColumnRef(_val) => "".to_string(),
        
    }
}

pub fn convert_aggregate(aggregate: &AggregateFunction, query_object: &QueryObject) -> String {
    let func = match aggregate.function
        {
            AggregateType::Max => "max",
            AggregateType::Min=>"min"  ,
              AggregateType::Avg=>"avg",
            AggregateType::Sum=> "sum" ,
             AggregateType::Count=>"count" ,
        };


    let col = convert_column_ref(&aggregate.column, query_object);

    format!("{}.{}", func, col)
}

// method to check if a table is an alias and return the table name
pub fn check_alias(table: &str, query_object: &QueryObject) -> String {
    if query_object.table_to_alias.contains_key(table) {
        table.to_string()
    } else {
        query_object.table_names_list.iter().find(|&x| x == table).unwrap().clone()
    }
}