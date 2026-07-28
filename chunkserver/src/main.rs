use std::env;
use std::error::Error;
use std::fs;
use std::path::Path;

use dashmap::DashSet;
use proto::master_chunkserver::CollectStatusRequest;
use proto::master_chunkserver::lease_client::LeaseClient;
use proto::master_chunkserver::lease_server::LeaseServer;
use proto::master_chunkserver::status_client::StatusClient;
use proto::master_chunkserver::status_server::StatusServer;

use tonic::Request;
use tonic::transport::Server;

mod config;
use config::Config;

mod heartbeat_service;
use heartbeat_service::Chunkserver;

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
    let chunkserver = Chunkserver::new(cfg, chunks);

    chunkserver.spawn_status_task();

    Server::builder()
        .add_service(LeaseServer::new(chunkserver))
        .serve("[::1]:50501".parse()?)
        .await?;

    Ok(())
}
