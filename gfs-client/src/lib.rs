use std::net::SocketAddrV4;

use proto::master_chunkserver::ChunkWriteRequest;
use proto::master_client::ChunkLocation;
use proto::master_client::fs_client::FsClient;
use proto::master_client::write_response::Error as WriteResponseError;
use proto::master_client::{CreateRequest, CreateResponse, WriteRequest, WriteResponse};
use std::fmt;
use tonic::Response;

#[derive(Debug)]
pub enum WriteError {
    Response(WriteResponseError),
    Status(tonic::Status),
}

impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteError::Response(err) => write!(f, "write response error: {:?}", err),
            WriteError::Status(status) => write!(f, "RPC failed: {}", status),
        }
    }
}

impl std::error::Error for WriteError {}

impl From<tonic::Status> for WriteError {
    fn from(status: tonic::Status) -> Self {
        WriteError::Status(status)
    }
}

pub struct GfsClient {
    inner: FsClient<tonic::transport::Channel>,
}

impl GfsClient {
    pub async fn new(addr: SocketAddrV4) -> Result<Self, tonic::transport::Error> {
        let fs_client = FsClient::connect(format!("http://{}", addr).to_string()).await?;

        Ok(Self { inner: fs_client })
    }

    pub async fn create(
        &mut self,
        path: String,
        is_directory: bool,
    ) -> Result<Response<CreateResponse>, WriteError> {
        Ok(self
            .inner
            .create(CreateRequest { path, is_directory })
            .await?)
    }

    pub async fn write(
        &mut self,
        path: String,
        offset: u64,
        length: u64,
    ) -> Result<(), WriteError> {
        let response = self
            .inner
            .write(WriteRequest {
                path: path.clone(),
                offset,
                length,
            })
            .await?;

        let WriteResponse {
            success,
            chunk_locations,
            error,
        } = response.into_inner();

        if !success {
            return Err(match error {
                Some(value) => WriteError::Response(
                    WriteResponseError::try_from(value).unwrap_or(WriteResponseError::Other),
                ),
                None => {
                    // TODO: log this
                    WriteError::Status(tonic::Status::internal(
                        "Server failed to populate error field",
                    ))
                }
            });
        }

        self._write_to_chunkserver(path, offset, length, chunk_locations);

        Ok(())
    }

    fn _write_to_chunkserver(
        &self,
        path: String,
        offset: u64,
        length: u64,
        chunk_locations: Vec<ChunkLocation>,
    ) {
    }
}
