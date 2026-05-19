use proto::client_master::fs_client::FsClient;
use proto::client_master::{CreateRequest, CreateResponse};
use tonic::Response;

pub struct GfsClient {
    inner: FsClient<tonic::transport::Channel>,
}

impl GfsClient {
    pub async fn connect(address: String) -> Result<Self, tonic::transport::Error> {
        let fs_client = FsClient::connect(address).await?;

        Ok(Self { inner: fs_client })
    }

    pub async fn create(
        &mut self,
        path: String,
    ) -> Result<Response<CreateResponse>, tonic::Status> {
        Ok(self.inner.create(CreateRequest { path }).await?)
    }
}
