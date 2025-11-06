import grpc
import argparse
import os
import sys
from typing import Iterator

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Import generated proto (adjust path as needed)
from generated import fileserver_pb2
from generated import fileserver_pb2_grpc

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


def upload_file(stub: fileserver_pb2_grpc.FileServerStub, filename: str):
    """Uploads a file to the server."""
    print(f"Uploading '{filename}'...")
    
    # Create some dummy content
    file_content = f"This is the content for {filename}.".encode("utf-8")
    
    # Create the request generator
    request_iterator = _generate_upload_requests(filename, file_content)
    
    try:
        response = stub.UploadFile(request_iterator)
        if response.success:
            print(f"Upload successful: {filename}")
        else:
            print(f"Upload failed: {response.error_message}")
    except grpc.RpcError as e:
        print(f"gRPC Error during upload: {e.details()}")

def fetch_file(stub: fileserver_pb2_grpc.FileServerStub, filename: str):
    """Fetches a file from the server."""
    print(f"Fetching '{filename}'...")
    name, ext = os.path.splitext(filename)
    metadata = fileserver_pb2.MetaData(filename=name, extension=ext)
    
    file_chunks = []
    try:
        response_iterator = stub.FetchFile(metadata)
        for response in response_iterator:
            if not response.success:
                print(f"Fetch failed: {response.error_message}")
                return
            file_chunks.append(response.file_data)
        
        file_content = b"".join(file_chunks)
        print(f"Fetch successful. Content: {file_content.decode('utf-8')}")
    
    except grpc.RpcError as e:
        print(f"gRPC Error during fetch: {e.details()}")

def list_files(stub: fileserver_pb2_grpc.FileServerStub):
    """Lists all files on the server."""
    print("Listing files...")
    try:
        response = stub.RetrieveFileList(
            fileserver_pb2.RetrieveFileListRequest()
        )
        if not response.file_list:
            print("No files found on server.")
            return

        print("Files on server:")
        for file_id, metadata in response.file_list.items():
            print(f"  - {metadata.filename}{metadata.extension}")
    except grpc.RpcError as e:
        print(f"gRPC Error during list: {e.details()}")

def main():
    parser = argparse.ArgumentParser(description="FileServer Test Client")
    parser.add_argument(
        "--port", type=int, required=True, help="Port of the server to connect to."
    )
    
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Upload command
    upload_parser = subparsers.add_parser("upload", help="Upload a file")
    upload_parser.add_argument("filename", type=str, help="Name of the file to upload")
    
    # Fetch command
    fetch_parser = subparsers.add_parser("fetch", help="Fetch a file")
    fetch_parser.add_argument("filename", type=str, help="Name of the file to fetch")
    
    # List command
    subparsers.add_parser("list", help="List files on the server")
    
    args = parser.parse_args()
    
    channel = grpc.insecure_channel(f"localhost:{args.port}")
    stub = fileserver_pb2_grpc.FileServerStub(channel)
    
    if args.command == "upload":
        upload_file(stub, args.filename)
    elif args.command == "fetch":
        fetch_file(stub, args.filename)
    elif args.command == "list":
        list_files(stub)

if __name__ == "__main__":
    main()