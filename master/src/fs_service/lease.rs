use std::collections::HashSet;
use std::net::SocketAddrV4;
use std::time::{Duration, Instant};

use proto::master_client::ChunkLocation;

static LEASE_DURATION: Duration = Duration::from_secs(60);

pub struct Lease {
    pub primary: Option<SocketAddrV4>,
    pub secondaries: HashSet<SocketAddrV4>,
    pub expiration: Instant,
}

impl Lease {
    pub fn new(primary: SocketAddrV4, secondaries: HashSet<SocketAddrV4>) -> Self {
        Self {
            primary: Some(primary),
            secondaries,
            expiration: Instant::now() + LEASE_DURATION,
        }
    }
    pub fn is_granted(&self) -> bool {
        return self.primary.is_some();
    }

    pub fn is_expired(&self) -> bool {
        return Instant::now() > self.expiration;
    }

    pub fn promote_secondary(&mut self) {
        if let Some(new_primary) = self.secondaries.iter().next().copied() {
            self.secondaries.take(&new_primary);

            if let Some(p) = self.primary {
                self.secondaries.insert(p);
            }

            self.primary = Some(new_primary);
        }
    }
}

impl TryFrom<&Lease> for ChunkLocation {
    type Error = ();

    fn try_from(lease: &Lease) -> Result<Self, Self::Error> {
        if lease.primary.is_none() {
            return Err(());
        }

        Ok(Self {
            primary: lease.primary.unwrap().to_string(),
            secondaries: lease
                .secondaries
                .iter()
                .map(SocketAddrV4::to_string)
                .collect(),
        })
    }
}
