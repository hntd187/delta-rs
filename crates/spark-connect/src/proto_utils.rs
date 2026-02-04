use crate::ConnectResult;
use crate::proto::delta_command::CommandType as DeltaCommandType;
use crate::proto::delta_relation::RelationType;
use crate::proto::delta_table::{AccessType, Path};
use crate::proto::spark::command::CommandType;
use crate::proto::spark::plan::OpType;
use crate::proto::spark::relation::RelType;
use crate::proto::spark::{Command, ExecutePlanRequest, Limit, LocalRelation, Plan, Relation};
use crate::proto::{
    DeltaCommand, DeltaRelation, DeltaTable, DescribeDetail, DescribeHistory, Scan,
};
use arrow::array::RecordBatch;
use prost_types::Any;
use std::collections::HashMap;

fn record_batch_to_bytes(mut result: LocalRelation, batch: RecordBatch) -> ConnectResult<()> {
    if let Some(data) = result.data.as_mut() {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(data, batch.schema_ref())?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    Ok(())
}

pub fn build_table(path: String) -> DeltaTable {
    let path = Path::builder()
        .path(path)
        .hadoop_conf(HashMap::new())
        .build();
    DeltaTable::builder()
        .access_type(AccessType::Path(path))
        .build()
}

pub fn build_describe_history(table: DeltaTable) -> ConnectResult<DescribeHistory> {
    Ok(DescribeHistory::builder().table(table).build())
}

pub fn build_describe_detail(table: DeltaTable) -> ConnectResult<DescribeDetail> {
    Ok(DescribeDetail::builder().table(table).build())
}

pub fn build_delta_scan(table: DeltaTable) -> ConnectResult<Scan> {
    Ok(Scan::builder().table(table).build())
}

pub fn build_delta_relation(msg: RelationType) -> ConnectResult<DeltaRelation> {
    Ok(DeltaRelation::builder().relation_type(msg).build())
}

pub fn build_delta_command(msg: DeltaCommandType) -> ConnectResult<DeltaCommand> {
    Ok(DeltaCommand::builder().command_type(msg).build())
}

pub fn with_limit(rel: Relation, limit: Option<i32>) -> OpType {
    if let Some(limit) = limit {
        let limit = Limit::builder().limit(limit).input(Box::new(rel)).build();
        let new_relation = Relation::builder()
            .rel_type(RelType::Limit(Box::new(limit)))
            .build();
        OpType::Root(new_relation)
    } else {
        OpType::Root(rel)
    }
}

pub fn build_spark_relation(
    session_id: String,
    delta_relation: DeltaRelation,
    limit: Option<i32>,
) -> ConnectResult<ExecutePlanRequest> {
    let rel_type = RelType::Extension(Any::from_msg(&delta_relation)?);
    let rel = Relation::builder().rel_type(rel_type).build();
    let op_type = with_limit(rel, limit);

    let plan = Plan::builder().op_type(op_type).build();
    Ok(ExecutePlanRequest::builder()
        .session_id(session_id)
        .plan(plan)
        .tags(vec![])
        .request_options(vec![])
        .build())
}

pub fn build_spark_command(
    session_id: String,
    delta_command: DeltaCommand,
) -> ConnectResult<ExecutePlanRequest> {
    let cmd_type = CommandType::Extension(Any::from_msg(&delta_command)?);
    let cmd = Command::builder().command_type(cmd_type).build();
    let plan = Plan::builder().op_type(OpType::Command(cmd)).build();

    Ok(ExecutePlanRequest::builder()
        .session_id(session_id)
        .plan(plan)
        .tags(vec![])
        .request_options(vec![])
        .build())
}
