use std::fs;
use std::fs::File;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};

use dashmap::{DashMap, DashSet, Entry};
use prost_types::{Timestamp, TimestampError};
use proto::master_chunkserver::heartbeat_server::Heartbeat;
use proto::master_chunkserver::{
    AssignLeaseRequest, AssignLeaseResponse, CollectStatusRequest, CollectStatusResponse,
};
use tonic::{Request, Response, async_trait};

use crate::config::Config;

static LEASE_DURATION: Duration = Duration::from_secs(60);

pub struct Lease {
    pub expiration: Instant,
}

impl Lease {
    pub fn is_expired(&self) -> bool {
        return Instant::now() > self.expiration;
    }
}

impl TryFrom<Timestamp> for Lease {
    type Error = TimestampError;

    fn try_from(timestamp: Timestamp) -> Result<Self, Self::Error> {
        let systime = SystemTime::try_from(timestamp)?;

        match systime.duration_since(SystemTime::now()) {
            Ok(duration) => Ok(Lease {
                expiration: Instant::now() + duration,
            }),
            Err(_) => Ok(Lease {
                expiration: Instant::now(),
            }),
        }
    }
}

pub struct Chunkserver {
    chunk_dir: PathBuf,
    chunks: DashSet<u64>,
    active_leases: DashMap<u64, Lease>,
}

impl Chunkserver {
    pub fn new(config: Config, chunks: DashSet<u64>) -> Self {
        let Config { chunk_dir } = config;

        Self {
            chunk_dir,
            chunks,
            active_leases: DashMap::new(),
        }
    }

    fn create_chunk(&self, handle: u64) {
        let path = self.chunk_dir.join(handle.to_string());

        if let Err(e) = File::create_new(path) {
            if e.kind() != std::io::ErrorKind::AlreadyExists {
                panic!("Failed to create chunk {handle}: {e}");
            }
        }

        self.chunks.insert(handle);
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
        let AssignLeaseRequest {
            handles,
            expirations,
        } = request.into_inner();

        if handles.len() != expirations.len() {
            return Ok(Response::new(AssignLeaseResponse { success: false }));
        }

        for (handle, expiration) in handles.into_iter().zip(expirations) {
            self.create_chunk(handle);

            let lease = Lease::try_from(expiration).map_err(|_| {
                tonic::Status::failed_precondition("Timestamp already expired")
            })?;

            self.active_leases.insert(handle, lease);
        }

        Ok(Response::new(AssignLeaseResponse { success: true }))
    }
}
