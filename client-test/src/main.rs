use std::error::Error;
use std::net::{Ipv4Addr, SocketAddrV4};

use gfs_client::GfsClient;

#[cfg(debug_assertions)]
mod debug_client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = SocketAddrV4::new(Ipv4Addr::new(192, 168, 71, 1), 50501);

    let mut client = match GfsClient::new(addr).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{}", e);
            return Err(e.into());
        }
    };

    if let Err(e) = client.create("test.txt".to_string(), false).await {
        eprintln!("Failed to create file: {}", e);
    }

    Ok(())
}
