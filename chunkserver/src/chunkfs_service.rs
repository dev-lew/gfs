use std::fs::OpenOptions;
use std::io::Result as IoResult;
use std::os::unix::fs::FileExt;
use std::path::Path;

use proto::chunkserver_client::chunk_fs_server::ChunkFs;
use proto::chunkserver_client::{
    PushRequest, PushResponse, WriteChunkRequest, WriteChunkResponse,
};
use tonic::{Request, Response, async_trait};

use crate::Chunkserver;

#[async_trait]
impl ChunkFs for Chunkserver {
    async fn push(
        &self,
        request: Request<PushRequest>,
    ) -> Result<Response<PushResponse>, tonic::Status> {
        let PushRequest { data } = request.into_inner();

        Ok(Response::new(PushResponse { success: true }))
    }

    async fn write_chunk(
        &self,
        request: Request<WriteChunkRequest>,
    ) -> Result<Response<WriteChunkResponse>, tonic::Status> {
        let WriteChunkRequest { handle, offset } = request.into_inner();

        // TODO: use LRU
        self._write_chunk(data, handle, offset)?;

        Ok(Response::new(WriteChunkResponse { success: true }))
    }
}

impl Chunkserver {
    fn _write_chunk(&self, data: Vec<u8>, handle: u64, offset: u64) -> IoResult<()> {
        let path = Path::new(&self.chunk_dir).join(handle.to_string());

        let file = match OpenOptions::new().create(true).write(true).open(path) {
            Ok(file) => file,
            Err(e) => match e.kind() {
                // TODO: Complete match
                _ => return Err(e),
            },
        };

        if let Err(e) = file.write_at(&data, offset) {
            match e.kind() {
                // TODO: Complete match
                _ => return Err(e),
            }
        }

        if let Err(e) = file.sync_all() {
            match e.kind() {
                // TODO: Complete match
                _ => return Err(e),
            }
        }

        Ok(())
    }
}
