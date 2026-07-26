use std::env;
use std::error::Error;
use std::fs;
use std::path::Path;

use dashmap::DashSet;
use proto::master_chunkserver::heartbeat_server::HeartbeatServer;

use tonic::transport::Server;

mod config;
use config::Config;

mod heartbeat_service;

fn scan_chunks(chunk_dir: &Path) -> Result<DashSet<u64>, Box<dyn Error>> {
    let chunks = DashSet::new();

    for f in fs::read_dir(chunk_dir)? {
        let chunk = f?;

        chunks.insert(
            chunk
                .file_name()
                .into_string()
                .expect("Invalid file name")
                .parse()?,
        );
    }

    Ok(chunks)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<String>>();
    let config_path = &args[1];

    let cfg = Config::new(Path::new(config_path))?;
    let chunk_dir = &cfg.chunk_dir;

    if let Err(e) = fs::create_dir(chunk_dir) {
        if e.kind() != std::io::ErrorKind::AlreadyExists {
            panic!("Failed to create chunk dir: {e}");
        }
    }

    let chunks = scan_chunks(chunk_dir)?;

    Server::builder()
        .add_service(HeartbeatServer::new(heartbeat_service::Chunkserver::new(
            cfg, chunks,
        )))
        .serve("[::1]:50501".parse()?)
        .await?;

    Ok(())
}
