use std::sync::Arc;

use proto::debug::debug_server;
use proto::debug::{ListFilesRequest, ListFilesResponse};
use tonic::{Request, Response, async_trait};

use crate::fs_service::ChunkNamespace;

pub struct DebugService {
    pub state: Arc<ChunkNamespace>,
}

#[async_trait]
impl debug_server::Debug for DebugService {
    async fn list_files(
        &self,
        request: Request<ListFilesRequest>,
    ) -> Result<Response<ListFilesResponse>, tonic::Status> {
        let files = self
            .state
            .file_namespace
            .iter()
            .map(|e| e.key().to_string_lossy().into_owned())
            .collect();

        Ok(Response::new(ListFilesResponse { files }))
    }
}
