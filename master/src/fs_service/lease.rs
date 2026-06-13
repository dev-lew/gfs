use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

static LEASE_DURATION: Duration = Duration::from_secs(60);

pub struct Lease {
    primary: Option<Ipv4Addr>,
    secondaries: Vec<Ipv4Addr>,
    expiration: Instant,
}

impl Lease {
    pub fn new(primary: Ipv4Addr, secondaries: Vec<Ipv4Addr>) -> Self {
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
}
