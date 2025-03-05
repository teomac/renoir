use crate::dsl::ir::ir_ast_structure::{ComplexField, SelectColumn};
use crate::dsl::ir::AggregateFunction;
use crate::dsl::ir::{AggregateType, ColumnRef};
use crate::dsl::struct_object::object::QueryObject;
use indexmap::IndexMap;

// struct to store the accumulator value
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AccumulatorValue {
    Aggregate(AggregateType, ColumnRef),
    Column(ColumnRef),
    Literal(String),
}

#[derive(Debug)]
pub struct AccumulatorInfo {
    pub value_positions: IndexMap<AccumulatorValue, (usize, String)>, // (position, type)
}

impl AccumulatorInfo {
    pub fn new() -> Self {
        AccumulatorInfo {
            value_positions: IndexMap::new(),
        }
    }

    pub fn add_value(&mut self, value: AccumulatorValue, val_type: String) -> usize {
        if let Some((pos, _)) = self.value_positions.get(&value) {
            *pos
        } else {
            let pos = self.value_positions.len();
            self.value_positions.insert(value, (pos, val_type));
            pos
        }
    }

    pub fn add_avg(&mut self, column: ColumnRef, val_type: String) -> (usize, usize) {
        let sum_pos = self.add_value(
            AccumulatorValue::Aggregate(AggregateType::Sum, column.clone()),
            val_type,
        );
        let count_pos = self.add_value(
            AccumulatorValue::Aggregate(AggregateType::Count, column),
            "usize".to_string(),
        );
        (sum_pos, count_pos)
    }
}

// Recursive function to check for aggregates in ComplexField
pub fn has_aggregate_in_complex_field(field: &ComplexField) -> bool {
    // Check if this field has an aggregate
    if field.aggregate.is_some() {
        return true;
    }

    // Recursively check nested expressions
    if let Some(nested) = &field.nested_expr {
        let (left, _, right) = &**nested;
        // Check both sides of the nested expression
        return has_aggregate_in_complex_field(left) || has_aggregate_in_complex_field(right);
    }

    false
}

pub fn collect_sink_aggregates(query_object: &QueryObject) -> Vec<AggregateFunction> {
    let mut aggregates = Vec::new();

    for clause in query_object.ir_ast.clone().unwrap().select.select {
        match clause {
            SelectColumn::Aggregate(agg, _) => {
                aggregates.push(AggregateFunction {
                    function: agg.function.clone(),
                    column: agg.column.clone(),
                });
            }
            SelectColumn::ComplexValue(field, _) => {
                collect_aggregates_in_complex_field(&field, &mut aggregates);
            }
            _ => {}
        }
    }

    aggregates
}

pub fn collect_aggregates_in_complex_field(
    field: &ComplexField,
    aggregates: &mut Vec<AggregateFunction>,
) {
    if let Some(ref nested) = field.nested_expr {
        let (left, _, right) = &**nested;
        collect_aggregates_in_complex_field(left, aggregates);
        collect_aggregates_in_complex_field(right, aggregates);
    } else if let Some(ref agg) = field.aggregate {
        aggregates.push(AggregateFunction {
            function: agg.function.clone(),
            column: agg.column.clone(),
        });
    }
}