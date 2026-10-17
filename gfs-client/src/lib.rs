use std::collections::HashMap;
use std::net::SocketAddrV4;

use proto::chunkserver_client::chunk_fs_client::ChunkFsClient;
use proto::chunkserver_client::{WriteChunkRequest, WriteChunkResponse};
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
    Transport(tonic::transport::Error),
}

impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteError::Response(err) => write!(f, "write response error: {:?}", err),
            WriteError::Status(status) => write!(f, "RPC failed: {}", status),
            WriteError::Transport(err) => write!(f, "transport error: {}", err),
        }
    }
}

impl std::error::Error for WriteError {}

impl From<tonic::Status> for WriteError {
    fn from(status: tonic::Status) -> Self {
        WriteError::Status(status)
    }
}

impl From<tonic::transport::Error> for WriteError {
    fn from(err: tonic::transport::Error) -> Self {
        WriteError::Transport(err)
    }
}

pub struct GfsClient {
    fs: FsClient<tonic::transport::Channel>,
    cfs: HashMap<(String, u64), ChunkFsClient<tonic::transport::Channel>>,
}

impl GfsClient {
    pub async fn new(addr: SocketAddrV4) -> Result<Self, tonic::transport::Error> {
        let fs_client = FsClient::connect(format!("http://{}", addr).to_string()).await?;

        Ok(Self {
            fs: fs_client,
            cfs: HashMap::new(),
        })
    }

    pub async fn create(
        &mut self,
        path: String,
        is_directory: bool,
    ) -> Result<Response<CreateResponse>, WriteError> {
        Ok(self.fs.create(CreateRequest { path, is_directory }).await?)
    }

    pub async fn write(
        &mut self,
        path: String,
        offset: u64,
        length: u64,
    ) -> Result<(), WriteError> {
        let write_metadata = WriteRequest {
            path: path.clone(),
            offset,
            length,
        };

        let response = self.fs.write(write_metadata.clone()).await?;

        let WriteResponse {
            success,
            handles,
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

        for (handle, chunk_location) in handles.into_iter().zip(chunk_locations) {
            let ChunkLocation {
                primary,
                secondaries,
            } = chunk_location;

            let request = WriteChunkRequest {
                write_metadata: Some(write_metadata.clone()),
                secondaries,
            };

            let entry = (path.clone(), handle);

            let (client, is_miss) = match self.cfs.get(&entry) {
                Some(client) => (client, false),
                None => (
                    &ChunkFsClient::connect(format!("http://{}", primary)).await?,
                    true,
                ),
            };

            match client.clone().write_chunk(request).await {
                Ok(r) => {
                    // TODO: use success
                    let WriteChunkResponse { success: _ } = r.into_inner();

                    if is_miss {
                        self.cfs.insert(entry, client.clone());
                    }
                }

                // TODO: think about invalidation rules for leases
                Err(e) => {
                    self.cfs.remove(&entry);

                    eprintln!("Failed to send write request {e}");
                }
            }
        }

        Ok(())
    }
}
