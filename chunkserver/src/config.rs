use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};

pub struct Config {
    pub chunk_dir: PathBuf,
    pub master_addr: Ipv4Addr,
}

impl Config {
    pub fn new(path: &Path) -> Result<Self, Box<dyn Error>> {
        let cfg = Self::parse(path)?;

        let chunk_dir = PathBuf::from(
            cfg.get("chunk_dir")
                .expect("chunk_dir missing in config file"),
        );

        let master_addr = cfg
            .get("master_addr")
            .expect("master_addr missing in config file")
            .parse()?;

        Ok(Self {
            chunk_dir,
            master_addr,
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
