import grpc
from concurrent import futures
from grpc_reflection.v1alpha import reflection
from generated.rag.v1 import rag_pb2
from generated.rag.v1 import rag_pb2_grpc


class RagEngineService(rag_pb2_grpc.RagEngineServiceServicer):
    def Ask(self, request, context):
        # Implementation goes here
        pass

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    rag_pb2_grpc.add_RagEngineServiceServicer_to_server(RagEngineService(), server)

    # 2. Define the services you want to expose via reflection
    SERVICE_NAMES = (
        rag_pb2.DESCRIPTOR.services_by_name['RagEngine'].full_name,
        reflection.SERVICE_NAME,
    )
    # 3. Enable reflection on the server
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server started on port 50051 with reflection enabled.")
    server.wait_for_termination()
