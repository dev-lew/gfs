use std::env;
use std::error::Error;
use std::fs;
use std::path::Path;

use proto::master_chunkserver::heartbeat_server::HeartbeatServer;

use tonic::transport::Server;

mod config;
use config::Config;

mod heartbeat_service;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<String>>();
    let config_path = &args[1];

    let cfg = Config::new(Path::new(config_path))?;

    if let Err(e) = fs::create_dir(&cfg.chunk_dir) {
        if e.kind() != std::io::ErrorKind::AlreadyExists {
            panic!("Failed to create chunk dir: {e}");
        }
    }

    Server::builder()
        .add_service(HeartbeatServer::new(heartbeat_service::Chunkserver::new(
            cfg,
        )))
        .serve("[::1]:50501".parse()?)
        .await?;

    Ok(())
}
