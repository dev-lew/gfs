pub mod master_chunkserver {
    tonic::include_proto!("master_chunkserver");
}

pub mod master_client {
    tonic::include_proto!("master_client");
}

pub mod chunkserver_client {
    tonic::include_proto!("chunkserver_client");
}

#[cfg(debug_assertions)]
pub mod debug {
    tonic::include_proto!("debug");
}
