#!/usr/bin/env bash
protoc proto/eraftpb.proto --rust_out=src/
# TODO: remove this once stepancheg/rust-protobuf#233 is resolved.
python <<EOF
import re
with open("src/eraftpb.rs") as reader:
    src = reader.read()

res = re.sub('::protobuf::rt::read_proto3_enum_with_unknown_fields_into\(([^,]+), ([^,]+), &mut ([^,]+), [^\)]+\)\?', 'if \\\\1 == ::protobuf::wire_format::WireTypeVarint {\\\\3 = \\\\2.read_enum()?;} else { return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type)); }', src)

with open("src/eraftpb.rs", "w") as writer:
    writer.write(res)
EOF
