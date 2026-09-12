use std::error::Error;
use std::path::Path;
use std::{env, sync::Arc};

use heartbeat_service::HeartbeatServer;
#[cfg(debug_assertions)]
use proto::debug::debug_server::DebugServer;
use proto::master_chunkserver::status_server::StatusServer;
use proto::master_client::fs_server::FsServer;
use tokio::sync::Mutex;
use tonic::transport::Server;

mod config;
use config::Config;

#[cfg(debug_assertions)]
mod debug_service;
#[cfg(debug_assertions)]
use debug_service::DebugService;

mod fs_service;
use fs_service::{ChunkNamespace, MasterFsServer};

mod heartbeat_service;

static NUM_CHANNELS: usize = 32;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<String>>();
    let config_path = &args[1];

    let cfg = Config::new(Path::new(config_path), 1 << 26)?;
    let state = Arc::new(ChunkNamespace::new());
    let (lease_tx, lease_rx) = tokio::sync::mpsc::channel(NUM_CHANNELS);

    let master = MasterFsServer::new(cfg, state.clone(), lease_tx);

    let dbg_service = {
        #[cfg(debug_assertions)]
        {
            Some(DebugServer::new(DebugService {
                state: state.clone(),
            }))
        }

        #[cfg(not(debug_assertions))]
        {
            None
        }
    };

    Server::builder()
        .add_service(FsServer::new(master))
        .add_service(StatusServer::new(HeartbeatServer {
            state: state.clone(),
            lease_rx: Mutex::new(lease_rx),
        }))
        .add_optional_service(dbg_service)
        .serve("0.0.0.0:50501".parse()?)
        .await?;

    Ok(())
}
