fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "grpc")]
    {
        let proto_root = "../../sources/hatchet-main/api-contracts";

        tonic_build::configure()
            .build_server(false)
            .compile_protos(
                &[
                    &format!("{proto_root}/v1/dispatcher.proto"),
                    &format!("{proto_root}/v1/workflows.proto"),
                    &format!("{proto_root}/v1/shared/condition.proto"),
                    &format!("{proto_root}/v1/shared/trigger.proto"),
                    &format!("{proto_root}/dispatcher/dispatcher.proto"),
                    &format!("{proto_root}/events/events.proto"),
                    &format!("{proto_root}/workflows/workflows.proto"),
                ],
                &[proto_root],
            )?;
    }

    Ok(())
}
