#!/usr/bin/env bash
# Generate Go gRPC code from the proto definition.
#
# Prerequisites:
#   brew install protobuf
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
#
# Make sure $GOPATH/bin (usually ~/go/bin) is in your PATH.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROTO_DIR="$SCRIPT_DIR/../../../src/rfq_test/proto"

for NAME in injective_rfq_rpc injective_rfq_pricer_rpc; do
  OUT_DIR="$SCRIPT_DIR/$NAME"
  mkdir -p "$OUT_DIR"

  protoc \
    --proto_path="$PROTO_DIR" \
    --go_out="$OUT_DIR" \
    --go_opt=paths=source_relative \
    --go_opt=M$NAME.proto=mm-scripts-go/proto/$NAME \
    --go-grpc_out="$OUT_DIR" \
    --go-grpc_opt=paths=source_relative \
    --go-grpc_opt=M$NAME.proto=mm-scripts-go/proto/$NAME \
    "$NAME.proto"

  echo "✅ Generated Go gRPC code in $OUT_DIR"
done
