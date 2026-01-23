use crate::proto::spark::*;
use arrow::array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use bytes::Bytes;
use tonic::body::Body;

use crate::proto::delta_relation::RelationType;
use crate::proto::spark::execute_plan_response::{ExecutionProgress, Metrics, ResponseType};
use tonic::client::GrpcService;
use tonic::codegen::StdError;
use tonic::{Status, Streaming};

pub mod proto {
    pub mod spark {
        tonic::include_proto!("spark.connect");
        pub use spark_connect_service_client::*;
    }
    tonic::include_proto!("delta.connect");
}

mod connect_error;
mod proto_utils;

use crate::connect_error::ConnectError;
use crate::proto::CreateDeltaTable;
use crate::proto::create_delta_table::{Column, Mode};
use proto_utils::*;

pub type ConnectResult<R> = Result<R, ConnectError>;

pub struct SparkSession<T>
where
    T: GrpcService<Body>,
    T::Error: Into<StdError>,
    T::ResponseBody: tonic::codegen::Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<StdError> + Send,
{
    session: SparkConnectServiceClient<T>,
    response_handler: ResponseHandler,
}

#[derive(Default, Debug, Clone)]
pub(crate) struct ResponseHandler {
    metrics: Option<Metrics>,
    batches: Vec<RecordBatch>,
    result_complete: bool,
    total_count: isize,
    execution_progress: Option<ExecutionProgress>,
    write_stream_operation_start_result: WriteStreamOperationStartResult,
}

impl<T> SparkSession<T>
where
    T: GrpcService<Body>,
    T::Error: Into<StdError>,
    T::ResponseBody: tonic::codegen::Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<StdError> + Send,
{
    pub fn new(session: SparkConnectServiceClient<T>) -> Self {
        Self {
            session,
            response_handler: ResponseHandler::default(),
        }
    }

    pub async fn describe_history(&mut self, path: String) -> ConnectResult<Vec<RecordBatch>> {
        let table = build_describe_history(build_table(path))?;
        let relation = build_delta_relation(RelationType::DescribeHistory(table))?;
        let request = build_spark_relation(relation)?;
        self.execute(request).await?;
        Ok(std::mem::take(&mut self.response_handler.batches))
    }

    pub async fn describe_detail(&mut self, path: String) -> ConnectResult<Vec<RecordBatch>> {
        let table = build_describe_detail(build_table(path))?;
        let relation = build_delta_relation(RelationType::DescribeDetail(table))?;
        let request = build_spark_relation(relation)?;
        self.execute(request).await?;
        Ok(std::mem::take(&mut self.response_handler.batches))
    }

    pub async fn scan(&mut self, path: String) -> ConnectResult<Vec<RecordBatch>> {
        let table = build_delta_scan(build_table(path))?;
        let relation = build_delta_relation(RelationType::Scan(table))?;
        let request = build_spark_relation(relation)?;
        self.execute(request).await?;
        Ok(std::mem::take(&mut self.response_handler.batches))
    }

    // pub async fn create_table<C: Into<Column>>(&mut self, create_delta_table: CreateDeltaTable) -> ConnectResult<Vec<RecordBatch>> {
    //
    //
    // }

    async fn execute(&mut self, plan: ExecutePlanRequest) -> ConnectResult<()> {
        let mut stream = self.session.execute_plan(plan).await?.into_inner();
        self.process_stream(&mut stream).await
    }

    async fn process_stream(
        &mut self,
        stream: &mut Streaming<ExecutePlanResponse>,
    ) -> ConnectResult<()> {
        while let Ok(Some(_resp)) = stream.message().await {
            self.handle_response(_resp)?;
        }
        Ok(())
    }

    fn handle_response(&mut self, mut resp: ExecutePlanResponse) -> ConnectResult<()> {
        self.response_handler.metrics = resp.metrics.take();

        if let Some(data) = resp.response_type {
            match data {
                ResponseType::ArrowBatch(res) => {
                    let reader = StreamReader::try_new(res.data.as_slice(), None)?;
                    for batch in reader {
                        let record = batch?;
                        if record.num_rows() != res.row_count as usize {
                            return Err(Status::data_loss("Rows not equal").into());
                        }
                        self.response_handler.batches.push(record);
                        self.response_handler.total_count += res.row_count as isize;
                    }
                }

                ResponseType::WriteStreamOperationStartResult(write_stream_op) => {
                    self.response_handler.write_stream_operation_start_result = write_stream_op;
                }
                ResponseType::ResultComplete(_) => self.response_handler.result_complete = true,
                ResponseType::ExecutionProgress(ep) => {
                    self.response_handler.execution_progress.replace(ep);
                }
                msg => {
                    panic!("Unknown response: {:?}", msg)
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use arrow::util::pretty::print_batches;

    #[tokio::test]
    pub async fn test_spark() -> ConnectResult<()> {
        let client = SparkConnectServiceClient::connect("sc://localhost:15002").await?;
        let mut session = SparkSession::new(client);
        let batches = session
            .scan("/mnt/c/Users/shcar/RustroverProjects/delta-rs/crates/test/tests/data/checkpoint-v2-table/".to_string())
            .await;
        if batches.is_err() {
            match batches.unwrap_err() {
                ConnectError::GrpcError(s) => {
                    println!("GrpcError: {:?}", String::from_utf8_lossy(s.details()));
                }
                _ => {}
            }
        } else {
            print_batches(&batches?)?;
        }
        Ok(())
    }
}
