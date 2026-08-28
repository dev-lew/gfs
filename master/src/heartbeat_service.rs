use std::net::SocketAddr;
use std::sync::Arc;

use dashmap::Entry;
use proto::master_chunkserver::status_server::Status;
use proto::master_chunkserver::{CollectStatusRequest, CollectStatusResponse};
use tokio::sync::Mutex;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::error::TryRecvError;
use tonic::{Request, Response, async_trait};

use crate::fs_service::ChunkNamespace;

pub struct HeartbeatServer {
    pub state: Arc<ChunkNamespace>,
    pub lease_rx: Mutex<Receiver<u64>>,
}

#[async_trait]
impl Status for HeartbeatServer {
    async fn collect_status(
        &self,
        request: Request<CollectStatusRequest>,
    ) -> Result<Response<CollectStatusResponse>, tonic::Status> {
        let addr = match request
            .remote_addr()
            .expect("Unable to get ip address of chunkserver")
        {
            SocketAddr::V4(addr) => addr,
            SocketAddr::V6(_) => panic!("Expected ipv4 address"),
        };

        let CollectStatusRequest { available_chunks } = request.into_inner();

        let mut deleted_chunks = Vec::new();

        for handle in available_chunks {
            match self.state.handle_map.entry(handle) {
                Entry::Vacant(_) => {
                    deleted_chunks.push(handle);
                }
                Entry::Occupied(v) => {
                    let filename = v.get();

                    if let Some(mut metadata) = self.state.file_namespace.get_mut(filename) {
                        if let Some(chunk) =
                            metadata.chunks.iter_mut().find(|c| c.handle == handle)
                        {
                            chunk.lease.secondaries.insert(addr);
                        } else {
                            return Err(tonic::Status::internal(
                                "Chunk exists in handle map but not filename namespace",
                            ));
                        }
                    }
                }
            }
        }

        let mut leased_chunks = Vec::new();
        let mut rx = self.lease_rx.lock().await;

        // TODO: investigate batching
        loop {
            match rx.try_recv() {
                Ok(h) => leased_chunks.push(h),
                Err(TryRecvError::Disconnected) => {
                    return Err(tonic::Status::internal(format!(
                        "Connection to chunkserver {} lost",
                        addr,
                    )));
                }
                Err(TryRecvError::Empty) => break,
            }
        }

        Ok(Response::new(CollectStatusResponse {
            deleted_chunks,
            leased_chunks,
        }))
    }
}
