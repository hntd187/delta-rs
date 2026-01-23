use crate::operations::create::CreateBuilder;
use crate::protocol::SaveMode;
use deltalake_spark_connect::proto::CreateDeltaTable;
use deltalake_spark_connect::proto::create_delta_table::Mode;

trait IntoConnectPlan {
    fn into_connect_plan(self);
}

impl Into<Mode> for SaveMode {
    fn into(self) -> Mode {
        match self {
            SaveMode::Overwrite => Mode::Replace,
            SaveMode::Ignore => Mode::CreateIfNotExists,
            _ => Mode::Create,
        }
    }
}

impl IntoConnectPlan for CreateBuilder {
    fn into_connect_plan(self) {
        let CreateBuilder {
            name,
            location,
            mode,
            comment,
            columns,
            partition_columns,
            ..
        } = self;
        let mode: Mode = mode.into();
        let create = CreateDeltaTable {
            mode: mode as i32,
            table_name: name,
            location,
            comment: None,
            columns: columns.into_iter().map(Into::into).collect(),
            partitioning_columns: partition_columns.unwrap_or_default(),
            properties: Default::default(),
            clustering_columns: vec![],
        };
    }
}
