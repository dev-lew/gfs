use proto::client_master::CreateRequest;
use proto::client_master::fs_client::FsClient;

pub struct GfsClient {
    inner: FsClient<tonic::transport::Channel>,
}

impl GfsClient {
    pub async fn connect(address: String) -> Result<Self, tonic::transport::Error> {
        let fs_client = FsClient::connect(address).await?;

        Ok(Self { inner: fs_client })
    }
}

pub fn create(file_path: &str) {}
