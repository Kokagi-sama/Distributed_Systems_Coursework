# client.py
import grpc
import os
import sys
import uuid

# Add project root to path (adjust as needed)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Import generated proto
from generated import fileserver_pb2
from generated import fileserver_pb2_grpc

# Chunk size for streaming
CHUNK_SIZE = 1024 * 1024  # 1MB

def generate_chunks(filename: str):
    """
    Helper generator to stream a file in chunks.
    The first message is metadata, the rest are file data.
    """
    
    # 1. Send metadata first
    file_stem = os.path.splitext(os.path.basename(filename))[0]
    file_ext = os.path.splitext(filename)[1]
    
    metadata = fileserver_pb2.MetaData(
        filename=file_stem, extension=file_ext
    )
    yield fileserver_pb2.UploadRequest(metadata=metadata)

    # 2. Send file data in chunks
    with open(filename, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            yield fileserver_pb2.UploadRequest(file_data=chunk)

def upload_file(stub, test_file_path: str):
    """Tests the UploadFile (client-streaming) RPC"""
    print(f"\n--- Uploading {test_file_path} ---")
    
    # Create the generator for streaming
    chunk_generator = generate_chunks(test_file_path)
    
    try:
        response = stub.UploadFile(chunk_generator)
        if response.success:
            print(f"Upload successful!")
        else:
            print(f"Upload failed: {response.error_message}")
    except grpc.RpcError as e:
        print(f"Upload RPC failed: {e.details()}")

def list_files(stub):
    """Tests the RetrieveFileList (unary) RPC"""
    print("\n--- Listing Files ---")
    try:
        response = stub.RetrieveFileList(
            fileserver_pb2.RetrieveFileListRequest()
        )
        if not response.file_list:
            print("No files found on server.")
            return

        print(f"Found {len(response.file_list)} files:")
        for file_id, metadata in response.file_list.items():
            print(f"  ID {file_id}: {metadata.filename}{metadata.extension}")
            
    except grpc.RpcError as e:
        print(f"ListFiles RPC failed: {e.details()}")

def fetch_file(stub, filename: str, save_as: str):
    """Tests the FetchFile (server-streaming) RPC"""
    print(f"\n--- Fetching {filename} (saving as {save_as}) ---")
    
    # Build the request
    file_stem = os.path.splitext(os.path.basename(filename))[0]
    file_ext = os.path.splitext(filename)[1]
    metadata = fileserver_pb2.MetaData(filename=file_stem, extension=file_ext)

    try:
        # This returns a generator-like object
        response_iterator = stub.FetchFile(metadata)
        
        with open(save_as, "wb") as f:
            for response in response_iterator:
                if not response.success:
                    print(f"Fetch failed: {response.error_message}")
                    # Clean up the partially written file
                    f.close()
                    os.remove(save_as)
                    return
                
                f.write(response.file_data)
        
        print(f"Fetch successful! File saved to {save_as}")
        
    except grpc.RpcError as e:
        print(f"Fetch RPC failed: {e.details()}")

def initiate_snapshot(stub):
    """Tests initiating a snapshot"""
    snapshot_id = f"snap-{uuid.uuid4()}"
    print(f"\n--- Initiating Snapshot {snapshot_id} ---")
    try:
        response = stub.InitiateSnapshot(
            fileserver_pb2.SnapshotRequest(
                snapshot_id=snapshot_id,
                initiator_id="client"
            )
        )
        print(f"Snapshot initiation response: {response.success}")
        return snapshot_id
    except grpc.RpcError as e:
        print(f"Snapshot RPC failed: {e.details()}")

def restore_snapshot(stub, snapshot_id: str):
    """Tests restoring from a snapshot"""
    print(f"\n--- Restoring Snapshot {snapshot_id} ---")
    try:
        response = stub.RestoreFromSnapshot(
            fileserver_pb2.RestoreRequest(snapshot_id=snapshot_id)
        )
        if response.success:
            print("Restore successful!")
        else:
            print(f"Restore failed: {response.error}")
    except grpc.RpcError as e:
        print(f"Restore RPC failed: {e.details()}")


def run():
    # --- Test Setup ---
    # Create a dummy file to upload
    test_file = "sample.txt"
    with open(test_file, "w") as f:
        f.write("This is the content of sample.txt!\n" * 10)
        
    test_file_large = "large_file.dat"
    with open(test_file_large, "wb") as f:
        f.write(os.urandom(5 * 1024 * 1024)) # 5MB file
    
    test_file_3 = "another_file.txt"
    with open(test_file_3, "w") as f:
        f.write("This file is for testing the restore.")

    # Connect to the server
    # Assumes server is running on localhost:50051
    server_address = "localhost:50051"
    
    with grpc.insecure_channel(server_address) as channel:
        stub = fileserver_pb2_grpc.FileServerStub(channel)

        # --- Run Tests ---
        
        # 1. List files (should be empty)
        list_files(stub)

        # 2. Upload file
        upload_file(stub, test_file)
        
        # 3. List files (should show 1 file)
        list_files(stub)
        
        # 4. Fetch the file back
        fetch_file(stub, "sample.txt", "downloaded_sample.txt")

        # 5. Upload large file
        upload_file(stub, test_file_large)

        # 6. List files (should show 2 files)
        list_files(stub)

        # 7. Fetch large file
        fetch_file(stub, "large_file.dat", "downloaded_large_file.dat")
        
        # 8. Test snapshot
        snap_id = initiate_snapshot(stub)
        
        # 9. Upload a new file *after* snapshot
        upload_file(stub, "another_file.txt")
        
        # 10. List (should show 3 files)
        list_files(stub)
        
        # 11. Restore
        if snap_id:
            # Note: This test assumes the snapshot file is
            # saved before this runs. May need a time.sleep(5).
            print("Waiting 5s for snapshot to complete...")
            import time
            time.sleep(5)
            
            restore_snapshot(stub, snap_id)
        
        # 12. List (should show 2 files again)
        print("\n--- Listing files after restore ---")
        list_files(stub)

    # --- Cleanup ---
    #os.remove(test_file)
    #os.remove(test_file_large)
    #os.remove("downloaded_sample.txt")
    #os.remove("downloaded_large_file.dat")
    #os.remove("another_file.txt") # Created by client
    #print("\nTest complete. Cleaned up files.")

if __name__ == "__main__":
    run()