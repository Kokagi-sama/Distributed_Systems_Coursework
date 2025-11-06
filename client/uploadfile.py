import grpc
import argparse
import sys
import os
from typing import List, Iterator

# Add project root to path
# Assumes this script is in a subdirectory (like 'client')
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

CHUNK_SIZE = 1024 * 1024  # 1MB

def _generate_upload_requests(
    filename: str, content: bytes
) -> Iterator[fileserver_pb2.UploadRequest]:
    """Yields metadata and then file chunks for client-streaming upload."""
    
    # 1. Send metadata first
    base_name = os.path.basename(filename)
    name, ext = os.path.splitext(base_name)
    metadata = fileserver_pb2.MetaData(filename=name, extension=ext)
    yield fileserver_pb2.UploadRequest(metadata=metadata)

    # 2. Send file data in chunks
    for i in range(0, len(content), CHUNK_SIZE):
        chunk = content[i : i + CHUNK_SIZE]
        yield fileserver_pb2.UploadRequest(file_data=chunk)


def main():
    parser = argparse.ArgumentParser(
        description="Uploads a file to the leader of the cluster."
    )
    parser.add_argument(
        "--servers",
        type=str,
        nargs='+',
        required=True,
        help="A list of all server addresses (e.g., localhost:50051 localhost:50052)"
    )
    parser.add_argument(
        "--filename",
        type=str,
        required=True,
        help="The name of the file to create (e.g., my_file.txt)"
    )
    parser.add_argument(
        "--content",
        type=str,
        default="This is a test file.",
        help="Optional: The string content for the file."
    )
    
    args = parser.parse_args()
    
    file_content_bytes = args.content.encode('utf-8')
    upload_successful = False

    # Loop through all provided servers until we find the leader
    for server_addr in args.servers:
        print(f"Attempting to upload to {server_addr}...")
        
        try:
            with grpc.insecure_channel(server_addr) as channel:
                stub = fileserver_pb2_grpc.FileServerStub(channel)
                
                # Create the request generator
                request_iterator = _generate_upload_requests(
                    args.filename, file_content_bytes
                )
                
                # Make the RPC call
                response = stub.UploadFile(request_iterator, timeout=5.0)
                
                if response.success:
                    print(f"\nUpload successful to leader at {server_addr}!")
                    upload_successful = True
                    break # Exit the loop, we are done
                else:
                    # This is the important part: check *why* it failed
                    if "Not leader" in response.error_message:
                        print(f"  > Failed: {server_addr} is a follower. Trying next...")
                    else:
                        print(f"  > Upload failed at {server_addr}: {response.error_message}")
        
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                print(f"  > Failed: Server {server_addr} is offline.")
            else:
                print(f"  > gRPC Error at {server_addr}: {e.details()}")
        except Exception as e:
            print(f"  > An unexpected error occurred for {server_addr}: {e}")

    if not upload_successful:
        print("\nUpload failed. Could not find the leader or all servers are down.")
        sys.exit(1)

if __name__ == "__main__":
    main()