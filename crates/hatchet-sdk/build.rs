fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = "proto";
    let protos = [
        format!("{proto_root}/v1/dispatcher.proto"),
        format!("{proto_root}/v1/workflows.proto"),
        format!("{proto_root}/v1/streams.proto"),
        format!("{proto_root}/v1/shared/condition.proto"),
        format!("{proto_root}/v1/shared/trigger.proto"),
        format!("{proto_root}/dispatcher/dispatcher.proto"),
        format!("{proto_root}/events/events.proto"),
        format!("{proto_root}/workflows/workflows.proto"),
    ];

    let mut prost = tonic_prost_build::Config::new();
    prost.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);

    tonic_prost_build::configure()
        .build_server(false)
        .compile_with_config(prost, &protos, &[proto_root.to_owned()])?;

    Ok(())
}
