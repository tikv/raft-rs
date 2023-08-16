use std::io::Result;

fn main() -> Result<()> {
    let mut prost_build = prost_build::Config::new();
    prost_build.btree_map(["."]);
    prost_build.compile_protos(
        &["proto/counter.proto", "proto/endpoint.proto"],
        &["proto/"],
    )?;
    Ok(())
}
