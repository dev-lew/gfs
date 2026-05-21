use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
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
    pub children: HashMap<OsString, Arc<RwLock<NamespaceNode>>>,
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

    pub fn get(&self, path: &PathBuf) -> Option<Arc<RwLock<NamespaceNode>>> {
        let components = path.components();
        let mut node = self.root.clone();

        for component in components {
            match component {
                Component::Normal(p) => {
                    let next = node.read().unwrap().children.get(p).cloned()?;
                    node = next;
                }

                Component::CurDir | Component::RootDir => continue,

                _ => return None,
            }
        }

        Some(node)
    }
}

#[tonic::async_trait]
impl Fs for MasterFsServer {
    async fn create(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, tonic::Status> {
        let CreateRequest { path, is_directory } = request.into_inner();

        // TODO: Sanitize this
        let file = PathBuf::from(path);

        if self.get(&file).is_some() {
            return Ok(Response::new(CreateResponse {
                success: false,
                error: Some(Error::FileExists as i32),
            }));
        } else {
            let components: Vec<_> = file.components().collect();
            let mut current_node = self.root.clone();

            let n = components.len() - 1;

            for (i, component) in components.iter().enumerate() {
                match component {
                    Component::Normal(p) => {
                        let node = NamespaceNode {
                            metadata: FileMetadata {
                                is_directory: if !is_directory && i != n {
                                    true
                                } else {
                                    is_directory
                                },
                                size: 0,
                            },
                            children: HashMap::new(),
                        };

                        let new_node = Arc::new(RwLock::new(node));

                        current_node
                            .write()
                            .unwrap()
                            .children
                            .insert(p.into(), new_node.clone());

                        current_node = new_node;
                    }
                    Component::CurDir | Component::RootDir => continue,
                    _ => {
                        return Ok(Response::new(CreateResponse {
                            success: false,
                            error: Some(Error::InvalidPath as i32),
                        }));
                    }
                }
            }

            Ok(Response::new(CreateResponse {
                success: true,
                error: None,
            }))
        }
    }
}
