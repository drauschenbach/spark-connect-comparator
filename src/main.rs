use crate::server::MySparkConnectService;
use crate::spark_connect::spark_connect_service_server::SparkConnectServiceServer;
use tonic::transport::Server;

mod server;

pub mod spark_connect {
    tonic::include_proto!("spark.connect");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50055".parse()?;
    let server = MySparkConnectService::default();

    let reflection_service = tonic_reflection::server::Builder::configure()
        //.register_encoded_file_descriptor_set(spark_connect::FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    Server::builder()
        .add_service(SparkConnectServiceServer::new(server))
        .add_service(reflection_service)
        .serve(addr)
        .await?;

    Ok(())
}
