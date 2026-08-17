use std::collections::HashSet;
use std::error::Error;
use std::fs;
use std::net::SocketAddrV4;
use std::path::Path;

pub struct Config {
    pub chunkservers: HashSet<SocketAddrV4>,
    pub chunk_size: u64,
}

impl Config {
    pub fn new(path: &Path, chunk_size: u64) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            chunkservers: Self::parse(path)?,
            chunk_size,
        })
    }

    fn parse(path: &Path) -> Result<HashSet<SocketAddrV4>, Box<dyn Error>> {
        let contents = fs::read_to_string(path)?;

        let chunkservers = contents
            .split_whitespace()
            .map(str::parse::<SocketAddrV4>)
            .collect::<Result<HashSet<_>, _>>()?;

        if chunkservers.len() < 1 {
            return Err("Malformed config file".into());
        }

        Ok(chunkservers)
    }
}
