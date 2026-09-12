fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("proto/master-chunkserver.proto")?;
    tonic_prost_build::compile_protos("proto/master-client.proto")?;
    tonic_prost_build::compile_protos("proto/chunkserver-client.proto")?;

    #[cfg(debug_assertions)]
    tonic_prost_build::compile_protos("proto/debug.proto")?;

    Ok(())
}
