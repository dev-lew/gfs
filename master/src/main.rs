use std::env;
use std::error::Error;
use std::path::Path;

use proto::client_master::fs_server::FsServer;
use tonic::transport::Server;

mod config;
use config::Config;

mod fs_service;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<String>>();
    let config_path = &args[1];

    let cfg = Config::new(Path::new(config_path))?;

    Server::builder()
        .add_service(FsServer::new(fs_service::MasterFsServer::default()))
        .serve("[::1]:50501".parse()?)
        .await?;

    Ok(())
}
