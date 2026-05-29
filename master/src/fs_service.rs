use core::panic;
use std::error::Error;
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::vec;

use dashmap::{DashMap, Entry};
use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, RawRwLock, RwLock, lock_api};
use proto::client_master::create_response::Error as CreateResponseError;
use proto::client_master::fs_server::Fs;
use proto::client_master::{CreateRequest, CreateResponse};
use tonic::{Request, Response, async_trait};

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
    FileExists(PathBuf),
    FileWithoutLock(PathBuf),
}

impl fmt::Display for NamespaceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NamespaceError::InvalidPath(p) => {
                write!(f, "invalid path: {:?}", p)
            }

            NamespaceError::FileExists(p) => {
                write!(f, "file {:?} exists", p)
            }

            NamespaceError::FileWithoutLock(p) => {
                write!(
                    f,
                    "the file {:?} cannot exist without its associated lock",
                    p
                )
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

    fn create_locks(
        &self,
        path: &Path,
    ) -> Result<Vec<ArcRwLockGuard<RawRwLock, ()>>, NamespaceError> {
        let Some(file) = path.file_name() else {
            return Err(NamespaceError::InvalidPath(path.to_path_buf()));
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
                    return Err(NamespaceError::InvalidPath(path.to_path_buf()));
                }
            }
        }

        Ok(guards)
    }

    fn create_empty_file(&self, path: &Path) -> Result<(), NamespaceError> {
        if !self.lock_namespace.contains_key(path) {
            return Err(NamespaceError::FileWithoutLock(path.to_path_buf()));
        }

        match self.file_namespace.entry(path.to_path_buf()) {
            Entry::Vacant(v) => {
                v.insert(FileMetadata {
                    is_directory: false,
                    size: 0,
                });

                Ok(())
            }
            Entry::Occupied(_) => Err(NamespaceError::FileExists(path.to_path_buf())),
        }
    }

    fn create_empty_directory(&self, path: &Path) -> Result<(), NamespaceError> {
        if !self.lock_namespace.contains_key(path) {
            return Err(NamespaceError::FileWithoutLock(path.to_path_buf()));
        }

        match self.file_namespace.entry(path.to_path_buf()) {
            Entry::Vacant(v) => {
                v.insert(FileMetadata {
                    is_directory: true,
                    size: 0,
                });

                Ok(())
            }
            Entry::Occupied(_) => Err(NamespaceError::FileExists(path.to_path_buf())),
        }
    }
}

#[async_trait]
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

        let _guards = match self.create_locks(&path) {
            Ok(g) => g,

            Err(NamespaceError::InvalidPath(_)) => {
                return Ok(Response::new(CreateResponse {
                    success: false,
                    error: Some(CreateResponseError::InvalidPath as i32),
                }));
            }

            // Other variants are not returned
            _ => panic!(),
        };

        if is_directory {
            if let Err(e) = self.create_empty_directory(&path) {
                match e {
                    NamespaceError::FileExists(_) => {
                        return Ok(Response::new(CreateResponse {
                            success: false,
                            error: Some(CreateResponseError::FileExists as i32),
                        }));
                    }

                    // TODO: Explain why this cannot happen
                    NamespaceError::FileWithoutLock(_) => panic!(),

                    // Other variants are not returned
                    _ => panic!(),
                }
            }
        } else {
            if let Err(e) = self.create_empty_file(&path) {
                match e {
                    NamespaceError::FileExists(_) => {
                        return Ok(Response::new(CreateResponse {
                            success: false,
                            error: Some(CreateResponseError::FileExists as i32),
                        }));
                    }

                    // TODO: Explain why this cannot happen
                    NamespaceError::FileWithoutLock(_) => panic!(),

                    // Other variants are not returned
                    _ => panic!(),
                }
            }
        }

        Ok(Response::new(CreateResponse {
            success: true,
            error: None,
        }))
    }
}
