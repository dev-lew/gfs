use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use proto::master_chunkserver::heartbeat_server::Heartbeat;
use proto::master_chunkserver::{
    AssignLeaseRequest, AssignLeaseResponse, CollectStatusRequest, CollectStatusResponse,
};
use tonic::{Request, Response, async_trait};

use crate::config::Config;

pub struct Lease {
    pub expiration: Instant,
}

impl Lease {
    pub fn new(expiration: Instant) -> Self {
        Self { expiration }
    }

    pub fn is_expired(&self) -> bool {
        return Instant::now() > self.expiration;
    }

    pub fn renew(&mut self) {
        todo!();
    }
}

pub struct Chunkserver {
    chunk_dir: PathBuf,
    chunks: HashSet<u64>,
    active_leases: HashMap<u64>,
}

pub struct Chunk {
    handle: u64,
    path: PathBuf,
}

impl Chunkserver {
    pub fn new(config: Config, handles: HashSet<u64>) -> Self {
        let Config { chunk_dir } = config;

        Self { chunk_dir, handles }
    }
}

#[async_trait]
impl Heartbeat for Chunkserver {
    async fn collect_status(
        &self,
        request: Request<CollectStatusRequest>,
    ) -> Result<Response<CollectStatusResponse>, tonic::Status> {
        todo!();
    }

    async fn assign_lease(
        &self,
        request: Request<AssignLeaseRequest>,
    ) -> Result<Response<AssignLeaseResponse>, tonic::Status> {
        let AssignLeaseRequest { handles } = request.into_inner();

        todo!();
    }
}
