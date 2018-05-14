extern crate protobuf_codegen_pure;

fn main() {
    protobuf_codegen_pure::run(protobuf_codegen_pure::Args {
        out_dir: "src",
        input: &["proto/eraftpb.proto"],
        includes: &["proto"],
        ..Default::default()
    }).expect("protoc");
}
