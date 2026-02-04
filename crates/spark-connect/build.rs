use tonic_prost_build::Config;

pub const BON_ATTR: &str =
    "#[derive(bon::Builder)] #[builder(on(::prost::alloc::string::String, into))]";

fn base_proto_config() -> Config {
    let mut cfg = Config::default();
    cfg.enable_type_names();
    cfg.message_attribute(".", BON_ATTR);
    cfg
}

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let delta_file_path = "protobuf/delta/connect";
    let spark_file_path = "protobuf/spark/connect";
    let delta_files = std::fs::read_dir(&delta_file_path)?;
    let spark_files = std::fs::read_dir(&spark_file_path)?;

    let mut delta_file_paths = vec![];
    let mut spark_file_paths = vec![];

    for file in delta_files {
        let entry = file?.path().to_string_lossy().to_string();
        delta_file_paths.push(entry);
    }

    for file in spark_files {
        let entry = file?.path().to_string_lossy().to_string();
        spark_file_paths.push(entry);
    }

    let mut delta_cfg = base_proto_config();
    delta_cfg.extern_path(".spark.connect", "crate::proto::spark");

    tonic_prost_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .generate_default_stubs(true)
        .build_server(false)
        .build_client(true)
        .build_transport(true)
        .compile_with_config(
            delta_cfg,
            delta_file_paths.as_ref(),
            &["protobuf".to_string()],
        )?;

    tonic_prost_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .generate_default_stubs(true)
        .build_server(false)
        .build_client(true)
        .build_transport(true)
        .compile_with_config(
            base_proto_config(),
            spark_file_paths.as_ref(),
            &["protobuf".to_string()],
        )?;

    Ok(())
}
