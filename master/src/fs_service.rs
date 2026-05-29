use std::collections::HashMap;
use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::vec;

use dashmap::{DashMap, Entry};
use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, RawRwLock, RwLock, lock_api};
use proto::client_master::create_response::Error as CreateResponseError;
use proto::client_master::fs_server::{Fs, FsServer};
use proto::client_master::{CreateRequest, CreateResponse, create_response};
use tonic::{Request, Response};

pub enum ArcRwLockGuard<R, T>
where
    R: lock_api::RawRwLock,
{
    Read(ArcRwLockReadGuard<R, T>),
    Write(ArcRwLockWriteGuard<R, T>),
}
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

pub struct MasterFsServer {
    file_namespace: DashMap<PathBuf, FileMetadata>,
    lock_namespace: DashMap<PathBuf, Arc<RwLock<()>>>,
}

impl Default for MasterFsServer {
    fn default() -> Self {
        let file_namespace = DashMap::new();

        file_namespace.insert(
            PathBuf::from("/"),
            FileMetadata {
                is_directory: true,
                size: 0,
            },
        );

        let lock_namespace = DashMap::new();

        lock_namespace.insert(PathBuf::from("/"), Arc::new(RwLock::new(())));

        Self {
            file_namespace,
            lock_namespace,
        }
    }
}

impl MasterFsServer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_empty_file(&self, path: &Path) -> bool {
        match self.file_namespace.entry(path.to_path_buf()) {
            Entry::Vacant(v) => {
                v.insert(FileMetadata {
                    is_directory: false,
                    size: 0,
                });

                true
            }
            Entry::Occupied(_) => false,
        }
    }

    pub fn create_empty_directory(&self, path: &Path) -> bool {
        match self.file_namespace.entry(path.to_path_buf()) {
            Entry::Vacant(v) => {
                v.insert(FileMetadata {
                    is_directory: true,
                    size: 0,
                });

                true
            }
            Entry::Occupied(_) => false,
        }
    }
}

impl Fs for MasterFsServer {
    async fn create(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, tonic::Status> {
        let CreateRequest { path, is_directory } = request.into_inner();

        // TODO: Sanitize this
        let path = PathBuf::from(path);

        if self.file_namespace.get(&path).is_some() {
            return Ok(Response::new(CreateResponse {
                success: false,
                error: Some(CreateResponseError::FileExists as i32),
            }));
        }

        let Some(file) = path.file_name() else {
            return Ok(Response::new(CreateResponse {
                success: false,
                error: Some(CreateResponseError::InvalidPath as i32),
            }));
        };

        let root_guard = self
            .lock_namespace
            .get(&PathBuf::from("/"))
            .expect("file namespace has no root")
            .read_arc();
        let mut guards = vec![ArcRwLockGuard::Read(root_guard)];

        let mut current = PathBuf::from("/");

        for component in path.components() {
            match component {
                Component::Normal(p) => {
                    current = current.join(p);

                    // There is no need to hold the lock before insertion,
                    // because we do not enforce a global ordering of events.
                    // The following serialization is valid:
                    // T1 inserts lock, T2 finds the lock, T2 creates
                    let handle = Arc::clone(
                        &self
                            .lock_namespace
                            .entry(current.clone())
                            .or_insert_with(|| Arc::new(RwLock::new(()))),
                    );

                    if file == p {
                        guards.push(ArcRwLockGuard::Write(handle.write_arc()));
                    } else {
                        guards.push(ArcRwLockGuard::Read(handle.read_arc()));
                    }
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

        if is_directory {
            if !self.create_empty_directory(&path) {
                return Ok(Response::new(CreateResponse {
                    success: false,
                    error: Some(CreateResponseError::FileExists as i32),
                }));
            }
        } else {
            if !self.create_empty_file(&path) {
                return Ok(Response::new(CreateResponse {
                    success: false,
                    error: Some(CreateResponseError::FileExists as i32),
                }));
            }
        }

        Ok(Response::new(CreateResponse {
            success: true,
            error: None,
        }))
    }
}
