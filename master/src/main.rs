use std::error::Error;
use std::path::Path;
use std::{env, sync::Arc};

use heartbeat_service::HeartbeatService;
use proto::client_master::fs_server::FsServer;
use proto::master_chunkserver::status_server::StatusServer;
use tonic::transport::Server;

mod config;
use config::Config;

#[cfg(debug_assertions)]
mod debug_service;

mod fs_service;
use fs_service::{ChunkNamespace, MasterFsServer};

mod heartbeat_service;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<String>>();
    let config_path = &args[1];

    let cfg = Config::new(Path::new(config_path), 1 << 26)?;
    let state = Arc::new(ChunkNamespace::new());

    let master = MasterFsServer::new(cfg, state.clone());

    Server::builder()
        .add_service(FsServer::new(master))
        .add_service(StatusServer::new(HeartbeatService {
            state: state.clone(),
        }))
        .serve("0.0.0.0:50501".parse()?)
        .await?;

    Ok(())
}
