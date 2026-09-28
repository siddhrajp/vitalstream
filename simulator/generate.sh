#!/bin/sh
# Generate Python code from the shared .proto contract into simulator/generated/.
# Re-run whenever schemas/proto changes. The output is not committed, just like the Java classes
# Maven generates into target/.
set -e
cd "$(dirname "$0")"
rm -rf generated
mkdir -p generated
.venv/bin/python -m grpc_tools.protoc \
  --proto_path=../schemas/proto \
  --python_out=generated \
  --pyi_out=generated \
  --grpc_python_out=generated \
  vitalstream/ingest/v1/ingest.proto
echo "Generated:"
find generated -name '*.py*' | sort
