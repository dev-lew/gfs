use std::collections::HashMap;
use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::{Component, PathBuf};
use std::sync::Arc;
use std::vec;

use dashmap::DashMap;
use parking_lot::lock_api;
use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, RawRwLock, RwLock};
use proto::client_master::create_response::Error as CreateResponseError;
use proto::client_master::fs_server::{Fs, FsServer};
use proto::client_master::{CreateRequest, CreateResponse, create_response};
use tonic::{Request, Response};

#[derive(Debug)]
pub enum NamespaceError {
    InvalidPath(PathBuf),
    NotFound(PathBuf),
}

impl fmt::Display for NamespaceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NamespaceError::InvalidPath(p) => {
                write!(f, "invalid path: {:?}", p)
            }

            NamespaceError::NotFound(p) => {
                write!(f, "path {:?} does not exist", p)
            }
        }
    }
}

impl Error for NamespaceError {}

pub struct FileMetadata {
    pub is_directory: bool,
    pub size: u64,
}

pub struct LockNode {
    pub children: HashMap<OsString, Arc<RwLock<LockNode>>>,
}

pub struct MasterFsServer {
    namespace: DashMap<PathBuf, FileMetadata>,
    namespace_locks: Arc<RwLock<LockNode>>,
}

pub enum ArcRwLockGuard<R, T>
where
    R: lock_api::RawRwLock,
{
    Read(ArcRwLockReadGuard<R, T>),
    Write(ArcRwLockWriteGuard<R, T>),
}

impl Default for MasterFsServer {
    fn default() -> Self {
        let root_ns = DashMap::new();

        root_ns.insert(
            PathBuf::from("/"),
            FileMetadata {
                is_directory: true,
                size: 0,
            },
        );

        let root_lock = LockNode {
            children: HashMap::new(),
        };

        Self {
            namespace: root_ns,
            namespace_locks: Arc::new(RwLock::new(root_lock)),
        }
    }
}

impl MasterFsServer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn read(
        &self,
        path: &PathBuf,
    ) -> Result<Vec<ArcRwLockReadGuard<RawRwLock, LockNode>>, NamespaceError> {
        let mut current = self.namespace_locks.clone();
        let mut guards = vec![RwLock::read_arc(&current)];

        for component in path.components() {
            match component {
                Component::Normal(p) => {
                    let next = current
                        .read_arc()
                        .children
                        .get(p)
                        .cloned()
                        .ok_or_else(|| NamespaceError::NotFound(path.clone()))?;

                    guards.push(RwLock::read_arc(&next));
                    current = next;
                }

                Component::CurDir | Component::RootDir => continue,

                _ => return Err(NamespaceError::InvalidPath(path.clone())),
            }
        }

        Ok(guards)
    }

    pub fn write(
        &self,
        path: &PathBuf,
    ) -> Result<Vec<ArcRwLockGuard<RawRwLock, LockNode>>, NamespaceError> {
        let mut current = self.namespace_locks.clone();
        let mut write_guard = None;

        let file = path
            .file_name()
            .ok_or_else(|| NamespaceError::InvalidPath(path.clone()))?;

        for component in path.components() {
            match component {
                Component::Normal(p) => {
                    if p == file {
                        write_guard = Some(RwLock::write_arc(&current));
                        break;
                    }

                    let next = current
                        .read_arc()
                        .children
                        .get(p)
                        .cloned()
                        .ok_or_else(|| NamespaceError::NotFound(path.clone()))?;

                    current = next;
                }

                Component::CurDir | Component::RootDir => continue,

                _ => return Err(NamespaceError::InvalidPath(path.clone())),
            }
        }

        let parent = path.parent().expect("Invalid path");
        let read_guards = self.read(&PathBuf::from(parent))?;

        let mut all_guards = read_guards
            .into_iter()
            .map(ArcRwLockGuard::Read)
            .collect::<Vec<ArcRwLockGuard<_, _>>>();

        all_guards.push(ArcRwLockGuard::Write(write_guard.expect("Invalid path")));

        Ok(all_guards)
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
        let path = PathBuf::from(path);

        let first = match path.components().nth(1) {
            Some(Component::Normal(p)) => p,
            _ => {
                return Ok(Response::new(CreateResponse {
                    success: false,
                    error: Some(CreateResponseError::InvalidPath as i32),
                }));
            }
        };

        if self.namespace.get(&path).is_some() {
            return Ok(Response::new(CreateResponse {
                success: false,
                error: Some(CreateResponseError::FileExists as i32),
            }));
        }

        let root_children = Arc::new(RwLock::new(LockNode {
            children: HashMap::new(),
        }));
        let mut current_node = Arc::clone(&root_children);

        for component in path.components() {
            match component {
                Component::Normal(p) => {
                    let next = Arc::new(RwLock::new(LockNode {
                        children: HashMap::new(),
                    }));

                    current_node
                        .write_arc()
                        .children
                        .insert(p.into(), Arc::clone(&next));

                    current_node = next;
                }

                Component::CurDir | Component::RootDir => continue,
                _ => {
                    return Ok(Response::new(CreateResponse {
                        success: false,
                        error: Some(CreateResponseError::InvalidPath as i32),
                    }));
                }
            }
        }

        self.namespace_locks
            .write_arc()
            .children
            .insert(first.into(), root_children);

        Ok(Response::new(CreateResponse {
            success: true,
            error: None,
        }))
    }
}

// if self.get(&file).is_some() {
//     return Ok(Response::new(CreateResponse {
//         success: false,
//         error: Some(Error::FileExists as i32),
//     }));
// } else {
//     let components: Vec<_> = file.components().collect();
//     let mut current_node = self.root.clone();

//     let n = components.len() - 1;

//     for (i, component) in components.iter().enumerate() {
//         match component {
//             Component::Normal(p) => {
//                 let node = NamespaceNode {
//                     metadata: FileMetadata {
//                         is_directory: if !is_directory && i != n {
//                             true
//                         } else {
//                             is_directory
//                         },
//                         size: 0,
//                     },
//                     children: DashMap::new(),
//                 };

//                 let new_node = Arc::new(RwLock::new(node));

//                 current_node
//                     .write()
//                     .unwrap()
//                     .children
//                     .insert(p.into(), new_node.clone());

//                 current_node = new_node;
//             }
//             Component::CurDir | Component::RootDir => continue,
//             _ => {
//                 return Ok(Response::new(CreateResponse {
//                     success: false,
//                     error: Some(Error::InvalidPath as i32),
//                 }));
//             }
//         }
//     }

//     Ok(Response::new(CreateResponse {
//         success: true,
//         error: None,
//     }))
// }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn get_returns_none_for_missing_path() {
        let fs = MasterFsServer::new();
        let node = fs.get(&PathBuf::from("/foo/bar/baz"));

        assert!(node.is_none());
    }

    #[tokio::test]
    async fn it_creates_absolute_file() {
        let fs = MasterFsServer::new();
        let path = String::from("/foo/bar");

        let req = Request::new(CreateRequest {
            is_directory: false,
            path: path.clone(),
        });

        let _ = fs.create(req).await;

        let foo_ns = fs.get(&PathBuf::from(path.clone()).parent().unwrap().to_owned());
        assert!(foo_ns.unwrap().read().unwrap().metadata.is_directory);

        let bar_ns = fs.get(&PathBuf::from(path.clone()).to_owned());
        assert_eq!(bar_ns.unwrap().read().unwrap().metadata.is_directory, false);
    }

    #[tokio::test]
    async fn it_creates_absolute_directory() {
        let fs = MasterFsServer::new();
        let path = String::from("/foo/bar");

        let req = Request::new(CreateRequest {
            is_directory: true,
            path: path.clone(),
        });

        let _ = fs.create(req).await;

        let foo_ns = fs.get(&PathBuf::from(path.clone()).parent().unwrap().to_owned());
        assert!(foo_ns.unwrap().read().unwrap().metadata.is_directory);

        let bar_ns = fs.get(&PathBuf::from(path.clone()).to_owned());
        assert!(bar_ns.unwrap().read().unwrap().metadata.is_directory);
    }

    #[tokio::test]
    async fn it_rejects_duplicate_paths() {
        let fs = MasterFsServer::new();
        let path = String::from("/foo");

        let req = Request::new(CreateRequest {
            is_directory: true,
            path: path.clone(),
        });

        let _ = fs.create(req).await.unwrap().into_inner();

        let req = Request::new(CreateRequest {
            is_directory: true,
            path,
        });

        let second = fs.create(req).await.unwrap().into_inner();

        assert!(!second.success);
        assert_eq!(second.error, Some(Error::FileExists as i32));
    }
}
