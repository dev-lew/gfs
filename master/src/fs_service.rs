use core::panic;
use std::error::Error;
use std::fmt;
use std::net::Ipv4Addr;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::vec;

use dashmap::{DashMap, Entry};
use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, RawRwLock, RwLock, lock_api};
use proto::client_master::create_response::Error as CreateResponseError;
use proto::client_master::fs_server::Fs;
use proto::client_master::write_response::Error as WriteResponseError;
use proto::client_master::{CreateRequest, CreateResponse, WriteRequest, WriteResponse};
use tonic::{Request, Response, async_trait};

mod lease;
use lease::Lease;

use crate::config::Config;

static NEXT_HANDLE: AtomicU64 = AtomicU64::new(0);

pub enum ArcRwLockGuard<R, T>
where
    R: lock_api::RawRwLock,
{
    Read(ArcRwLockReadGuard<R, T>),
    Write(ArcRwLockWriteGuard<R, T>),
}

#[derive(Debug)]
pub enum NamespaceError {
    FileExists(PathBuf),
    FileWithoutLock(PathBuf),
    InvalidPath(PathBuf),
}

impl fmt::Display for NamespaceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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

            NamespaceError::InvalidPath(p) => {
                write!(f, "invalid path: {:?}", p)
            }
        }
    }
}

impl Error for NamespaceError {}

pub struct Chunk {
    handle: u64,
    lease: Lease,
}

impl Chunk {
    pub fn new(primary: Ipv4Addr, secondaries: Vec<Ipv4Addr>) -> Self {
        Self {
            handle: NEXT_HANDLE.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            lease: Lease::new(primary, secondaries),
        }
    }
}

pub struct FileMetadata {
    pub is_directory: bool,
    pub size: u64,
    pub chunks: Vec<Chunk>,
}

pub struct MasterFsServer {
    file_namespace: DashMap<PathBuf, FileMetadata>,
    lock_namespace: DashMap<PathBuf, Arc<RwLock<()>>>,
    chunkservers: Vec<Ipv4Addr>,
    chunk_size: u64,
}

impl MasterFsServer {
    pub fn new(config: Config) -> Self {
        let file_namespace = DashMap::new();

        file_namespace.insert(
            PathBuf::from("/"),
            FileMetadata {
                is_directory: true,
                size: 0,
                chunks: Vec::new(),
            },
        );

        let lock_namespace = DashMap::new();

        lock_namespace.insert(PathBuf::from("/"), Arc::new(RwLock::new(())));

        let Config {
            chunkservers,
            chunk_size,
        } = config;

        Self {
            file_namespace,
            lock_namespace,
            chunkservers,
            chunk_size,
        }
    }

    fn write(&self, path: &Path) -> Result<Vec<ArcRwLockGuard<RawRwLock, ()>>, NamespaceError> {
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

    fn read(&self, path: &Path) -> Result<Vec<ArcRwLockReadGuard<RawRwLock, ()>>, NamespaceError> {
        let root_guard = self
            .lock_namespace
            .get(&PathBuf::from("/"))
            .expect("file namespace has no root")
            .read_arc();
        let mut guards = vec![root_guard];

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

                    guards.push(handle.read_arc());
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
                    chunks: Vec::new(),
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
                    chunks: Vec::new(),
                });

                Ok(())
            }
            Entry::Occupied(_) => Err(NamespaceError::FileExists(path.to_path_buf())),
        }
    }

    fn create_chunk(&self) -> Chunk {
        let mut chunkservers = self.chunkservers.clone();

        let primary = chunkservers.remove(0);
        let secondaries = chunkservers;

        return Chunk::new(primary, secondaries);
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

        if self.file_namespace.contains_key(&path) {
            return Ok(Response::new(CreateResponse {
                error: Some(CreateResponseError::FileExists as i32),
                ..Default::default()
            }));
        }

        let _guards = match self.write(&path) {
            Ok(g) => g,

            Err(NamespaceError::InvalidPath(_)) => {
                return Ok(Response::new(CreateResponse {
                    error: Some(CreateResponseError::InvalidPath as i32),
                    ..Default::default()
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
                            error: Some(CreateResponseError::FileExists as i32),
                            ..Default::default()
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
                            error: Some(CreateResponseError::FileExists as i32),
                            ..Default::default()
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

    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, tonic::Status> {
        let WriteRequest {
            path,
            offset,
            length,
        } = request.into_inner();

        let path = PathBuf::from(path);

        if !self.file_namespace.contains_key(&path) {
            return Ok(Response::new(WriteResponse {
                error: Some(WriteResponseError::FileDoesNotExist as i32),
                ..Default::default()
            }));
        }

        let _guards = match self.write(&path) {
            Ok(g) => g,

            Err(NamespaceError::InvalidPath(_)) => {
                return Ok(Response::new(WriteResponse {
                    error: Some(WriteResponseError::InvalidPath as i32),
                    ..Default::default()
                }));
            }

            // Other variants are not returned
            _ => panic!(),
        };

        let mut metadata = self
            .file_namespace
            .get_mut(&path)
            .expect("file does not exist");

        if metadata.is_directory {
            return Ok(Response::new(WriteResponse {
                error: Some(WriteResponseError::PathIsDirectory as i32),
                ..Default::default()
            }));
        }

        if metadata.chunks.is_empty() {
            // TODO: Implement padding
            if offset != 0 {
                return Ok(Response::new(WriteResponse {
                    error: Some(WriteResponseError::InvalidOffset as i32),
                    ..Default::default()
                }));
            }

            for _ in 0..length % self.chunk_size {
                metadata.chunks.push(self.create_chunk())
            }

            // Each lease is identical, so only create a Vec of length 1
            // even if the number of chunks > 1 so as not to waste space
            let lease = &metadata.chunks.first().unwrap().lease;
            let mut chunk_locations = Vec::new();

            chunk_locations.push(lease.try_into().unwrap());

            return Ok(Response::new(WriteResponse {
                success: true,
                chunk_locations,
                error: None,
            }));
        }

        let chunk_locations = metadata.chunks.iter().fold(Vec::new(), |mut acc, c| {
            acc.push((&c.lease).try_into().unwrap());
            acc
        });

        Ok(Response::new(WriteResponse {
            success: true,
            chunk_locations,
            error: None,
        }))
    }
}
