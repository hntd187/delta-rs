//! Constraints and generated column mappings
use crate::kernel::DataType;
use crate::table::DataCheck;
use crate::{DeltaResult, DeltaTableError};
use arrow_schema::Field;
use std::any::Any;
use std::collections::HashMap;

/// A constraint in a check constraint
#[derive(Eq, PartialEq, Debug, Default, Clone)]
pub struct Constraint {
    /// The full path to the field.
    pub name: String,
    /// The SQL string that must always evaluate to true.
    pub expr: String,
}

impl Constraint {
    /// Create a new invariant
    pub fn new(field_name: &str, invariant_sql: &str) -> Self {
        Self {
            name: field_name.to_string(),
            expr: invariant_sql.to_string(),
        }
    }
}

impl DataCheck for Constraint {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn get_expression(&self) -> &str {
        &self.expr
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A generated column
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct GeneratedColumn {
    /// The full path to the field.
    pub name: String,
    /// The SQL string that generate the column value.
    pub generation_expr: String,
    /// The SQL string that must always evaluate to true.
    pub validation_expr: String,
    /// Data Type
    pub data_type: DataType,
}

impl GeneratedColumn {
    /// Create a new invariant
    pub fn new(field_name: &str, sql_generation: &str, data_type: &DataType) -> Self {
        Self {
            name: field_name.to_string(),
            generation_expr: sql_generation.to_string(),
            validation_expr: format!("{field_name} <=> {sql_generation}"), // update to
            data_type: data_type.clone(),
        }
    }

    pub fn get_generation_expression(&self) -> &str {
        &self.generation_expr
    }
}

impl DataCheck for GeneratedColumn {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn get_expression(&self) -> &str {
        &self.validation_expr
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub struct IdentityColumn {
    name: String,
    start: Option<i64>,
    step: Option<i64>,
    high_watermark: Option<i64>,
    allow_explicit_insert: Option<bool>,
}

impl IdentityColumn {
    pub fn new(
        name: String,
        start: Option<i64>,
        step: Option<i64>,
        high_watermark: Option<i64>,
        allow_explicit_insert: Option<bool>,
    ) -> Self {
        Self {
            name,
            start,
            step,
            high_watermark,
            allow_explicit_insert,
        }
    }

    pub fn get_name(&self) -> &str {
        self.name.as_str()
    }

    pub fn generate_range(&mut self, num_rows: i64) -> DeltaResult<impl Iterator<Item = i64>> {
        if let Some(start) = self.start {
            if let Some(step) = self.step {
                // If we have a watermark, step once so we don't overlap the first and last value
                // of the last commit
                let low = self.high_watermark.map(|wm| wm + step).unwrap_or(start);
                let high = self.high_watermark.unwrap_or(0) + num_rows;
                dbg!(low, high, num_rows, self.high_watermark);
                self.high_watermark = Some(high);
                return Ok((low..=high).step_by(step as usize));
            }
        }
        Err(DeltaTableError::Generic(format!(
            "Invalid identity column: {:?}",
            self
        )))
    }

    pub fn is_empty(&self) -> bool {
        self.start.is_none()
            && self.step.is_none()
            && self.high_watermark.is_none()
            && self.allow_explicit_insert.is_none()
    }
}

impl Into<Field> for IdentityColumn {
    fn into(self) -> Field {
        let mut metadata = HashMap::new();
        if let Some(start) = self.start {
            metadata.insert("delta.identity.start".to_string(), start.to_string());
        }
        if let Some(step) = self.step {
            metadata.insert("delta.identity.step".to_string(), step.to_string());
        }
        if let Some(watermark) = self.high_watermark {
            metadata.insert(
                "delta.identity.highWaterMark".to_string(),
                watermark.to_string(),
            );
        }
        if let Some(allow_insert) = self.allow_explicit_insert {
            metadata.insert(
                "delta.identity.allowExplicitInsert".to_string(),
                allow_insert.to_string(),
            );
        }

        Field::new(self.name, arrow_schema::DataType::Int64, false).with_metadata(metadata)
    }
}
