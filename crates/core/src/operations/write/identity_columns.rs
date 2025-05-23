use crate::table::state::DeltaTableState;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::SchemaBuilder;
use std::sync::Arc;

use crate::table::IdentityColumn;
use crate::DeltaResult;

pub fn can_write_identity_column(snapshot: &DeltaTableState) -> DeltaResult<bool> {
    if let Some(features) = &snapshot.protocol().writer_features {
        if snapshot.protocol().min_writer_version < 6 {
            return Ok(false);
        }
        if snapshot.protocol().min_writer_version >= 6
            && !features.contains(&delta_kernel::table_features::WriterFeature::IdentityColumns)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn add_identity_to_batch(
    id_columns: &mut [IdentityColumn],
    batch: RecordBatch,
) -> DeltaResult<RecordBatch> {
    if id_columns.is_empty() {
        return Ok(batch);
    }
    let schema_fields = batch.schema().fields().clone();
    let mut sb = SchemaBuilder::from(schema_fields);
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(id_columns.len() + batch.num_columns());
    new_columns.extend_from_slice(batch.columns());
    for id_col in id_columns {
        let id_range = id_col.generate_range(batch.num_rows() as i64)?;
        let arrow_id_col = Int64Array::from_iter_values(id_range);
        new_columns.push(Arc::new(arrow_id_col));
        sb.push(Arc::new(id_col.clone().into()));
    }
    let new_schema = sb.finish();

    Ok(RecordBatch::try_new(Arc::new(new_schema), new_columns)?)
}

pub fn add_identity_to_batches(
    id_columns: &mut [IdentityColumn],
    batches: Vec<RecordBatch>,
) -> DeltaResult<Vec<RecordBatch>> {
    let mut new_batches = Vec::with_capacity(batches.len());
    for batch in batches {
        let new_batch = add_identity_to_batch(id_columns, batch)?;
        new_batches.push(new_batch);
    }
    Ok(new_batches)
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::table::IdentityColumn;
    use crate::DeltaResult;
    use arrow_array::{record_batch, RecordBatch};
    use arrow_cast::pretty::print_batches;

    fn make_batch() -> RecordBatch {
        record_batch!(("a", Int32, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])).unwrap()
    }

    #[test]
    pub fn test_id_col_additions() -> DeltaResult<()> {
        let batches = vec![make_batch(), make_batch(), make_batch(), make_batch()];
        let mut id_cols = vec![IdentityColumn::new(
            "id".to_string(),
            Some(1),
            Some(1),
            None,
            Some(false),
        )];

        let new_batches = add_identity_to_batches(&mut id_cols, batches)?;
        print_batches(&new_batches)?;
        dbg!(id_cols);

        Ok(())
    }
}
