#!/bin/sh

mkdir -p /shared/compiled_protobufs

ls /shared/protocol_buffers | \
grep .proto | \
xargs python3 -m grpc_tools.protoc \
          --proto_path=/shared/protocol_buffers/ \
          --python_out=/shared/compiled_protobufs \
          --grpc_python_out=/shared/compiled_protobufs \
          --mypy_out=quiet:/shared/compiled_protobufs \
          --mypy_grpc_out=quiet:/shared/compiled_protobufs

echo "Built Services and TaskMap protobufs"
