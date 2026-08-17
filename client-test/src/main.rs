use std::net::{Ipv4Addr, SocketAddrV4};

use gfs_client::GfsClient;

fn main() {
    let addr = SocketAddrV4::new(Ipv4Addr::new(192, 168, 71, 1), 50501);
    let client = GfsClient::new(addr);
}
