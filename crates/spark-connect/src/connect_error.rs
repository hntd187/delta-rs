use arrow::error::ArrowError;
use prost::EncodeError;
use thiserror::Error;
use tonic::Status;
use tonic::transport::Error;

#[derive(Debug, Error)]
pub enum ConnectError {
    #[error("{0}")]
    GrpcError(#[from] Status),
    #[error("{0}")]
    EncodeError(#[from] EncodeError),
    #[error("{0}")]
    ArrowError(#[from] ArrowError),
    #[error("{0}")]
    TransportError(#[from] Error),
}
