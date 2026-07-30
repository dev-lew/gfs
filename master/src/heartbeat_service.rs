use std::net::IpAddr;

use dashmap::Entry;
use proto::master_chunkserver::status_server::Status;
use proto::master_chunkserver::{CollectStatusRequest, CollectStatusResponse};
use tonic::{Request, Response, async_trait};

use crate::fs_service::MasterFsServer;

#[async_trait]
impl Status for MasterFsServer {
    async fn collect_status(
        &self,
        request: Request<CollectStatusRequest>,
    ) -> Result<Response<CollectStatusResponse>, tonic::Status> {
        let addr = match request
            .remote_addr()
            .expect("Unable to get ip address of chunkserver")
            .ip()
        {
            IpAddr::V4(ip) => ip,
            IpAddr::V6(_) => panic!("Expected ipv4 address"),
        };

        let CollectStatusRequest { available_chunks } = request.into_inner();

        let mut deleted_chunks = Vec::new();

        for handle in available_chunks {
            match self.handle_map.entry(handle) {
                Entry::Vacant(_) => {
                    deleted_chunks.push(handle);
                }
                Entry::Occupied(v) => {
                    let filename = v.get();

                    if let Some(mut metadata) = self.file_namespace.get_mut(filename) {
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

        Ok(Response::new(CollectStatusResponse {
            deleted_chunks: Vec::new(),
            leased_chunks: Vec::new(),
        }))
    }
}
