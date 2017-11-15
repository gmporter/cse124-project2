#!/usr/bin/env python
import argparse
import os.path

import grpc

import SurfStoreBasic_pb2
import SurfStoreBasic_pb2_grpc

from config_reader import SurfStoreConfigReader


def parse_args():
    parser = argparse.ArgumentParser(description="SurfStore client")
    parser.add_argument("config_file", type=str,
                        help="Path to configuration file")
    return parser.parse_args()


def get_metadata_stub(config):
    channel = grpc.insecure_channel('localhost:%d' % config.metadata_ports[1])
    stub = SurfStoreBasic_pb2_grpc.MetadataStoreStub(channel)
    return stub


def get_block_stub(config):
    channel = grpc.insecure_channel('localhost:%d' % config.block_port)
    stub = SurfStoreBasic_pb2_grpc.BlockStoreStub(channel)
    return stub


def run(config):
    metadata_stub = get_metadata_stub(config)
    block_stub = get_block_stub(config)

    metadata_stub.Ping(SurfStoreBasic_pb2.Empty())
    print("Successfully pinged the Metadata server")

    block_stub.Ping(SurfStoreBasic_pb2.Empty())
    print("Successfully pinged the Blockstore server")

    # Implement your client here


if __name__ == "__main__":
    args = parse_args()
    config = SurfStoreConfigReader(args.config_file)

    run(config)
