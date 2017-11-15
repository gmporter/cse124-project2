#!/usr/bin/env python
import argparse
import time
from concurrent import futures

import grpc

import SurfStoreBasic_pb2
import SurfStoreBasic_pb2_grpc

from config_reader import SurfStoreConfigReader

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class BlockStore(SurfStoreBasic_pb2_grpc.BlockStoreServicer):
    def __init__(self):
        super(BlockStore, self).__init__()

    def Ping(self, request, context):
        return SurfStoreBasic_pb2.Empty()

    # TODO: Implement the other RPCs!


def parse_args():
    parser = argparse.ArgumentParser(description="BlockStore server for SurfStore")
    parser.add_argument("config_file", type=str,
                        help="Path to configuration file")
    parser.add_argument("-t", "--threads", type=int, default=10,
                        help="Maximum number of concurrent threads")
    return parser.parse_args()


def serve(args, config):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=args.threads))
    SurfStoreBasic_pb2_grpc.add_BlockStoreServicer_to_server(BlockStore(), server)
    server.add_insecure_port("127.0.0.1:%d" % config.block_port)
    server.start()
    print("Server started on 127.0.0.1:%d" % config.block_port)
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    args = parse_args()
    config = SurfStoreConfigReader(args.config_file)
    serve(args, config)
