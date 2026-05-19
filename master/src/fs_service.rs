use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;

use proto::client_master::fs_server::{Fs, FsServer};
use proto::client_master::{CreateRequest, CreateResponse, create_response};
use tonic::{Request, Response};

pub struct FileMetadata {
    pub is_directory: bool,
    pub size: u64,
}

#[derive(Default)]
pub struct MasterFsServer {
    file_metadata: RwLock<HashMap<PathBuf, FileMetadata>>,
}

#[tonic::async_trait]
impl Fs for MasterFsServer {
    async fn create(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, tonic::Status> {
        let CreateRequest { path, is_directory } = request.into_inner();
        let file = PathBuf::from(path);

        if self.file_metadata.read().unwrap().contains_key(&file) {
            return Ok(Response::new(CreateResponse {
                success: false,
                error: Some(create_response::Error::FileExists as i32),
            }));
        } else {
            self.file_metadata.write().unwrap().insert(
                file,
                FileMetadata {
                    is_directory,
                    size: 0,
                },
            );

            return Ok(Response::new(CreateResponse {
                success: true,
                error: None,
            }));
        }
    }
}
