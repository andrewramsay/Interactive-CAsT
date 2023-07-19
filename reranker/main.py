import sys

sys.path.insert(0, '/shared')
sys.path.insert(0, '/shared/compiled_protobufs')

import grpc
from concurrent import futures

from rerankers import PygaggleReranker as RerankerServicer
from reranker_pb2_grpc import add_RerankerServicer_to_server

MAX_MESSAGE_LENGTH = 32 * 1024 * 1024
opts = [
    ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
    ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
    ]

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), options=opts)
    add_RerankerServicer_to_server(RerankerServicer(), server)

    server.add_insecure_port("[::]:8000")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
