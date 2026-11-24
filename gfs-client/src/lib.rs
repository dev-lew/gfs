use std::collections::HashMap;
use std::fmt;
use std::hash::{DefaultHasher, Hasher};
use std::iter;
use std::net::SocketAddrV4;

use bytes::Bytes;
use proto::chunkserver_client::chunk_fs_client::ChunkFsClient;
use proto::chunkserver_client::{PushRequest, PushResponse};
use proto::master_client::ChunkLocation;
use proto::master_client::fs_client::FsClient;
use proto::master_client::write_response::Error as WriteResponseError;
use proto::master_client::{CreateRequest, CreateResponse, WriteRequest, WriteResponse};
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
    cfs: HashMap<(String, u64), HashMap<String, ChunkFsClient<tonic::transport::Channel>>>,
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
        src: &[u8],
        dest: String, // Not a PathBuf because it isn't serializable by prost
        offset: u64,
        length: u64,
    ) -> Result<(), WriteError> {
        let write_request = WriteRequest {
            path: dest.clone(),
            offset,
            length,
        };

        let response = self.fs.write(write_request.clone()).await?;

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

        // Bytes allow us to cheaply clone data as opposed to a Vec<u8>, so when
        // we push to chunkservers we don't copy each time when forming the request
        let src = Bytes::copy_from_slice(src);

        for (handle, chunk_location) in handles.into_iter().zip(chunk_locations) {
            let ChunkLocation {
                primary,
                secondaries,
            } = chunk_location;

            let key = (dest.clone(), handle);

            let addresses: Vec<String> = iter::once(primary).chain(secondaries).collect();
            let mut missing_addresses = Vec::new();

            for address in &addresses {
                let is_cached = self.cfs.get(&key).is_some_and(|h| h.contains_key(address));

                if !is_cached {
                    missing_addresses.push(address);
                }
            }

            for address in missing_addresses {
                // TODO: Handle ?
                let client = ChunkFsClient::connect(format!("http://{}", address)).await?;

                self.cfs
                    .entry(key.clone())
                    .or_default()
                    .insert(address.clone(), client);
            }

            let request = PushRequest {
                hash: (|| {
                    let mut hasher = DefaultHasher::new();

                    hasher.write(&src);
                    hasher.finish()
                })(),
                data: src.clone(),
            };

            for address in &addresses {
                let mut client = self
                    .cfs
                    .get(&key)
                    .and_then(|h| h.get(address))
                    .cloned()
                    .ok_or_else(|| {
                        WriteError::Status(tonic::Status::internal("Cache unpopulated"))
                    })?;

                match client.push(request.clone()).await {
                    Ok(r) => {
                        // TODO: Use success
                        let PushResponse { success: _ } = r.into_inner();
                    }

                    Err(e) => {
                        // Think about invalidation rules for leases
                        if let Some(h) = self.cfs.get_mut(&key) {
                            h.remove(address);
                        }

                        eprintln!("Failed to send write request {e}");
                    }
                }
            }
        }

        Ok(())
    }
}
