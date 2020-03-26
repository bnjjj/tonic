fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::prost::configure()
        .build_server(true)
        .build_client(false)
        .format(true)
        .compile(&["proto/health.proto"], &["proto/"])?;

    Ok(())
}
