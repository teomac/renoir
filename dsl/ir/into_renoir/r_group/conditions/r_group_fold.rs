use crate::dsl::ir::ir_ast_structure::{AggregateType, Group};
use crate::dsl::ir::r_group::r_group_keys::{GroupAccumulatorInfo, GroupAccumulatorValue};
use crate::dsl::ir::r_utils::check_alias;
use crate::dsl::ir::QueryObject;

// Function to create fold operation if needed
pub fn create_fold_operation(
    acc_info: &GroupAccumulatorInfo,
    _group_by: &Group,
    query_object: &QueryObject,
) -> String {
    let mut tuple_types = Vec::new();
    let mut tuple_inits = Vec::new();
    let mut update_code = String::new();

    //if there are no aggregates, return empty string
    if acc_info.agg_positions.is_empty() {
        return "".to_string();
    }

    let single_agg = acc_info.agg_positions.len() == 1;
    
    // First add types and initializers for regular columns and aggregates
    for (value, (pos, val_type)) in &acc_info.agg_positions {
        match value {
            GroupAccumulatorValue::Aggregate(agg_type, _) => {
                match agg_type {
                    AggregateType::Max | AggregateType::Min | AggregateType::Sum => {
                        // These will be Option types
                        let actual_type = match (agg_type, val_type.as_str()) {
                            (AggregateType::Max | AggregateType::Min, "i64") => {
                                "Option<f64>".to_string()
                            }
                            _ => format!("Option<{}>", val_type),
                        };
                        tuple_types.push(actual_type);
                        tuple_inits.push("None".to_string());
                    }
                    AggregateType::Count => {
                        // Count stays as is
                        tuple_types.push(val_type.clone());
                        match val_type.as_str() {
                            "f64" => tuple_inits.push("0.0".to_string()),
                            "i64" | "usize" => tuple_inits.push("0".to_string()),
                            _ => panic!("Unsupported type for Count: {}", val_type),
                        }
                    }
                    AggregateType::Avg => {
                        // Avg is handled through Sum and Count
                        tuple_types.push(val_type.clone());
                        tuple_inits.push("0.0".to_string());
                    }
                }

                // Generate update code
                match value {
                    GroupAccumulatorValue::Aggregate(agg_type, col) => {
                        let col_access = if query_object.has_join {
                            let table = col.table.as_ref().unwrap();
                            let table_name = check_alias(table, query_object);
                            format!(
                                "x{}.{}",
                                query_object.table_to_tuple_access.get(&table_name).unwrap(),
                                col.column
                            )
                        } else {
                            format!("x.{}", col.column)
                        };

                        match agg_type {
                            AggregateType::Count => {
                                if col.column == "*" {
                                    update_code.push_str(&format!(
                                        "{}acc{} += 1;\n",
                                        if !single_agg {
                                            String::from("")
                                        } else {
                                            String::from("*")
                                        },
                                        if single_agg {
                                            String::from("")
                                        } else {
                                            format!(".{}", pos)
                                        }
                                    ));
                                } else {
                                    update_code.push_str(&format!(
                                        "    if {}.is_some() {{ {}acc{} += 1; }}\n",
                                        col_access,
                                        if !single_agg {
                                            String::from("")
                                        } else {
                                            String::from("*")
                                        },
                                        if single_agg {
                                            String::from("")
                                        } else {
                                            format!(".{}", pos)
                                        }
                                    ));
                                }
                            }
                            AggregateType::Sum => {
                                update_code.push_str(&format!(
                                    "    if let Some(val) = {} {{ 
                                        {}acc{} = Some({}acc{}.unwrap_or(0.0) + val);
                                    }}\n",
                                    col_access,
                                    if !single_agg {
                                        String::from("")
                                    } else {
                                        String::from("*")
                                    },
                                    if single_agg {
                                        String::from("")
                                    } else {
                                        format!(".{}", pos)
                                    },
                                    if !single_agg {
                                        String::from("")
                                    } else {
                                        String::from("*")
                                    },
                                    if single_agg {
                                        String::from("")
                                    } else {
                                        format!(".{}", pos)
                                    }
                                ));
                            }
                            AggregateType::Max => {
                                update_code.push_str(&format!(
                                    "    if let Some(val) = {} {{
                                        {}acc{} = Some(match {}acc{} {{
                                            Some(current_max) => current_max.max(val as f64),
                                            None => val as f64
                                        }});
                                    }}\n",
                                    col_access,
                                    if !single_agg {
                                        String::from("")
                                    } else {
                                        String::from("*")
                                    },
                                    if single_agg {
                                        String::from("")
                                    } else {
                                        format!(".{}", pos)
                                    },
                                    if !single_agg {
                                        String::from("")
                                    } else {
                                        String::from("*")
                                    },
                                    if single_agg {
                                        String::from("")
                                    } else {
                                        format!(".{}", pos)
                                    }
                                ));
                            }
                            AggregateType::Min => {
                                update_code.push_str(&format!(
                                    "    if let Some(val) = {} {{
                                        {}acc{} = Some(match {}acc{} {{
                                            Some(current_min) => current_min.min(val as f64),
                                            None => val as f64
                                        }});
                                    }}\n",
                                    col_access,
                                    if !single_agg {
                                        String::from("")
                                    } else {
                                        String::from("*")
                                    },
                                    if single_agg {
                                        String::from("")
                                    } else {
                                        format!(".{}", pos)
                                    },
                                    if !single_agg {
                                        String::from("")
                                    } else {
                                        String::from("*")
                                    },
                                    if single_agg {
                                        String::from("")
                                    } else {
                                        format!(".{}", pos)
                                    }
                                ));
                            }
                            AggregateType::Avg => {} // Handled through Sum and Count
                        }
                    }
                }
            }
        }
    }

    // Generate the fold string
    let tuple_type = format!("({})", tuple_types.join(", "));
    let tuple_init = format!("({})", tuple_inits.join(", "));

    let mut fold_str = format!(".fold({}, |acc: &mut {}, x| {{\n", tuple_init, tuple_type);
    fold_str.push_str(&update_code);
    fold_str.push_str("\n})\n");

    fold_str
}
