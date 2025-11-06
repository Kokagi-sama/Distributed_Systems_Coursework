import grpc
import argparse
import sys
import os
from typing import List

# Add project root to path
# Assumes this script is in a subdirectory (like 'client')
# If it's in the root, change this to os.path.dirname
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Import generated proto
try:
    from generated import fileserver_pb2
    from generated import fileserver_pb2_grpc
except ImportError:
    print("Error: Could not import generated protobuf files.")
    print("Please ensure you have run the protoc compiler and your PYTHONPATH is set correctly.")
    sys.exit(1)


def check_server(port: int):
    """Connects to a server and prints its file list."""
    
    print(f"\n--- Checking Server on port {port} ---")
    
    try:
        with grpc.insecure_channel(f"localhost:{port}") as channel:
            stub = fileserver_pb2_grpc.FileServerStub(channel)
            
            # Create the simple request
            request = fileserver_pb2.RetrieveFileListRequest()
            
            # Make the RPC call (with a short timeout)
            response = stub.RetrieveFileList(request, timeout=2.0)
            
            if not response.file_list:
                print("Server has no files.")
                return

            print("Files on server:")
            # response.file_list is a map. We just want the values (MetaData)
            for metadata in response.file_list.values():
                print(f"  - {metadata.filename}{metadata.extension}")
    
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.UNAVAILABLE:
            print(f"Error: Server on port {port} is offline or unreachable.")
        else:
            print(f"gRPC Error connecting to port {port}: {e.details()}")
    except Exception as e:
        print(f"An unexpected error occurred for port {port}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Check file lists on servers.")
    parser.add_argument(
        "--ports",
        type=int,
        nargs='+', # Accepts one or more port numbers
        required=True,
        help="The port(s) of the server(s) to check."
    )
    args = parser.parse_args()
    
    for port in args.ports:
        check_server(port)

if __name__ == "__main__":
    main()