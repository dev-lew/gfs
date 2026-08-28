use std::net::SocketAddrV4;

use proto::debug::debug_client::DebugClient;
use proto::debug::{ListFilesRequest, ListFilesResponse};
use tonic::Response;

pub struct GfsDebugClient {
    inner: DebugClient<tonic::transport::Channel>,
}

impl GfsDebugClient {
    pub async fn new(addr: SocketAddrV4) -> Result<Self, tonic::transport::Error> {
        let debug_client =
            DebugClient::connect(format!("http://{}", addr).to_string()).await?;

        Ok(Self {
            inner: debug_client,
        })
    }

    pub async fn list_files(
        &mut self,
    ) -> Result<Response<ListFilesResponse>, tonic::Status> {
        Ok(self.inner.list_files(ListFilesRequest {}).await?)
    }
}
