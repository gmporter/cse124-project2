#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"  # Directory the script is in
python -m grpc_tools.protoc -I$PUBLIC/lib/protoc/include --proto_path=$DIR --python_out=$DIR --grpc_python_out=$DIR $DIR/SurfStoreBasic.proto