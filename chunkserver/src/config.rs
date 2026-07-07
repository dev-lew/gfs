use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

pub struct Config {
    pub chunk_dir: PathBuf,
}

impl Config {
    pub fn new(path: &Path) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            chunk_dir: Self::parse(path)?,
        })
    }

    fn parse(path: &Path) -> Result<PathBuf, Box<dyn Error>> {
        let contents = fs::read_to_string(path)?;

        Ok(PathBuf::from(contents))
    }
}
