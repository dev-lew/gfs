use std::collections::HashMap;
use std::path::{Component, PathBuf};
use std::sync::{Arc, RwLock};

use proto::client_master::create_response::Error;
use proto::client_master::fs_server::{Fs, FsServer};
use proto::client_master::{CreateRequest, CreateResponse, create_response};
use tonic::{Request, Response};

pub struct FileMetadata {
    pub is_directory: bool,
    pub size: u64,
}

pub struct NamespaceNode {
    pub metadata: FileMetadata,
    pub children: HashMap<String, Arc<RwLock<NamespaceNode>>>,
}

pub struct MasterFsServer {
    root: Arc<RwLock<NamespaceNode>>,
}

impl MasterFsServer {
    pub fn new() -> Self {
        let node = {
            NamespaceNode {
                metadata: FileMetadata {
                    is_directory: true,
                    size: 0,
                },
                children: HashMap::new(),
            }
        };

        Self {
            root: Arc::new(RwLock::new(node)),
        }
    }

    pub fn get(&self, path: PathBuf) -> Option<Arc<RwLock<NamespaceNode>>> {
        let components = path.components();
        let mut node = Some(self.root.clone());

        for component in components {
            match component {
                Component::Normal(p) => {
                    if let Some(n) = node {
                        node = n
                            .read()
                            .unwrap()
                            .children
                            .get(&p.to_string_lossy().into_owned())
                            .cloned();

                        if node.is_none() {
                            return None;
                        }
                    }
                }

                Component::CurDir | Component::RootDir => continue,

                _ => return None,
            }
        }

        node
    }
}

#[tonic::async_trait]
impl Fs for MasterFsServer {
    async fn create(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, tonic::Status> {
        let CreateRequest { path, is_directory } = request.into_inner();
        let file = PathBuf::from(path);

        let components = file.components();

        for component in components {
            match component {
                Component::Normal(p) => {
                    let k = self.root.write().unwrap();
                }
                _ => {
                    return Ok(Response::new(CreateResponse {
                        success: false,
                        error: Some(Error::InvalidPath as i32),
                    }));
                }
            }
        }
    }
}
