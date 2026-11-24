use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::net::SocketAddrV4;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

static DEFAULT_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(128).expect("128 is nonzero");

pub struct Config {
    pub chunk_dir: PathBuf,
    pub master_addr: SocketAddrV4,
    pub cache_size: NonZeroUsize,
}

impl Config {
    pub fn new(path: &Path) -> Result<Self, Box<dyn Error>> {
        let cfg = Self::parse(path)?;

        let chunk_dir = cfg
            .get("chunk_dir")
            .map(PathBuf::from)
            .unwrap_or(PathBuf::from("/chunks"));

        let master_addr = cfg
            .get("master_addr")
            .expect("master_addr missing in config file")
            .parse()?;

        let cache_size = cfg
            .get("cache_size")
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_CACHE_SIZE);

        Ok(Self {
            chunk_dir,
            master_addr,
            cache_size,
        })
    }

    fn parse(path: &Path) -> Result<HashMap<String, String>, Box<dyn Error>> {
        let contents = fs::read_to_string(path)?;

        Ok(contents
            .lines()
            .filter_map(|line| {
                let (k, v) = line.split_once("=")?;

                Some((k.trim().to_owned(), v.trim().to_owned()))
            })
            .collect())
    }
}
