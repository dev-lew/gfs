use std::env;
use std::error::Error;
use std::path::Path;

use proto::client_master::fs_server::FsServer;
use tonic::transport::Server;

mod config;
use config::Config;

mod fs_service;
use fs_service::MasterFsServer;

mod heartbeat_service;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<String>>();
    let config_path = &args[1];

    let cfg = Config::new(Path::new(config_path), 1 << 26)?;

    Server::builder()
        .add_service(FsServer::new(MasterFsServer::new(cfg)))
        .serve("[::1]:50501".parse()?)
        .await?;

    Ok(())
}
