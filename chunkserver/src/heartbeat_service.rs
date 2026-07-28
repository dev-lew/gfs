use std::fs::File;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use dashmap::{DashMap, DashSet};
use prost_types::{Timestamp, TimestampError};
use proto::master_chunkserver::lease_server::Lease as OtherLease;
use proto::master_chunkserver::status_client::StatusClient;
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
    master_addr: Ipv4Addr,
    chunks: Arc<DashSet<u64>>,
    active_leases: DashMap<u64, Lease>,
}

impl Chunkserver {
    pub fn new(config: Config, chunks: DashSet<u64>) -> Self {
        let Config {
            chunk_dir,
            master_addr,
        } = config;

        let chunks = Arc::new(chunks);

        Self {
            chunk_dir,
            master_addr,
            chunks,
            active_leases: DashMap::new(),
        }
    }

    async fn send_status(&self) {
        let chunks = self.chunks.clone();
        let master_addr = self.master_addr.clone();

        tokio::spawn(async move {
            loop {
                match StatusClient::connect(master_addr.to_string()).await {
                    Ok(mut client) => {
                        let available_chunks = chunks.iter().map(|c| *c).collect();

                        if let Err(e) = client
                            .collect_status(CollectStatusRequest { available_chunks })
                            .await
                        {
                            eprintln!("Failed to send status {e}");
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to connect to client at {}: {e}", master_addr);
                    }
                }

                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
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
impl OtherLease for Chunkserver {
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
