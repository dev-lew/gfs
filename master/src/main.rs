use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs;
use std::io::{Error as IOError, ErrorKind};
use std::path::Path;

mod config;
use config::Config;

fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<String>>();
    let config_path = &args[1];

    let config = Config::new(Path::new(config_path));

    println!("Hello, world!");
    Ok(())
}
