mod service;

use std::net::SocketAddr;
use tonic::transport::Server;
use tracing::info;

pub mod delta_proto {
    tonic::include_proto!("delta");
}

use delta_proto::delta_service_server::DeltaServiceServer;
use service::DeltaServiceImpl;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let port: u16 = std::env::var("DELTA_SERVER_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(50051);

    let addr: SocketAddr = format!("0.0.0.0:{port}").parse()?;
    let svc = DeltaServiceImpl;

    info!("delta-server listening on {addr}");

    Server::builder()
        .add_service(DeltaServiceServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
