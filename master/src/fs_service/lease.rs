use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use proto::client_master::write_response::ChunkLocation;

static LEASE_DURATION: Duration = Duration::from_secs(60);

pub struct Lease {
    pub primary: Option<Ipv4Addr>,
    pub secondaries: HashSet<Ipv4Addr>,
    pub expiration: Instant,
}

impl Lease {
    pub fn new(primary: Ipv4Addr, secondaries: HashSet<Ipv4Addr>) -> Self {
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
            secondaries: lease.secondaries.iter().map(Ipv4Addr::to_string).collect(),
        })
    }
}
