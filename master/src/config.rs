use std::error::Error;
use std::fs;
use std::net::Ipv4Addr;
use std::path::Path;

pub struct Config {
    pub chunkservers: Vec<Ipv4Addr>,
}

impl Config {
    pub fn new(path: &Path) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            chunkservers: Self::parse(path)?,
        })
    }

    fn parse(path: &Path) -> Result<Vec<Ipv4Addr>, Box<dyn Error>> {
        let contents = fs::read_to_string(path)?;

        let chunkservers = contents
            .split_whitespace()
            .map(|ip| ip.parse::<Ipv4Addr>())
            .collect::<Result<_, _>>()?;

        Ok(chunkservers)
    }
}
