pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let delta_file_path = "protobuf/delta/connect";
    let spark_file_path = "protobuf/spark/connect";
    let delta_files = std::fs::read_dir(&delta_file_path)?;
    let spark_files = std::fs::read_dir(&spark_file_path)?;

    let mut delta_file_paths: Vec<String> = vec![];

    for file in delta_files {
        let entry = file?.path();
        delta_file_paths.push(entry.to_str().unwrap().to_string());
    }

    let mut spark_file_paths: Vec<String> = vec![];

    for file in spark_files {
        let entry = file?.path();
        spark_file_paths.push(entry.to_str().unwrap().to_string());
    }
    let mut cfg = tonic_prost_build::Config::default();
    cfg.enable_type_names();
    cfg.extern_path(".spark.connect", "crate::proto::spark");

    tonic_prost_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .generate_default_stubs(true)
        .build_server(false)
        .build_client(true)
        .build_transport(true)
        .compile_with_config(cfg, delta_file_paths.as_ref(), &["protobuf".to_string()])?;

    let mut cfg = tonic_prost_build::Config::default();
    cfg.enable_type_names();

    tonic_prost_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .generate_default_stubs(true)
        .build_server(false)
        .build_client(true)
        .build_transport(true)
        .compile_with_config(cfg, spark_file_paths.as_ref(), &["protobuf".to_string()])?;

    Ok(())
}
