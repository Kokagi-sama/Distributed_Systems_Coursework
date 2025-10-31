import grpc
import sys
import os

# This line adds your project's 'root' directory (one level up from 'server')
# to Python's list of places to look for modules.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# --- Now your other imports can work ---
from proto import fs_pb2
from proto import fs_pb2_grpc
import argparse
import time
from datetime import datetime

def run_replication_test(primary_addr, follower_addr):
    
    # 1. Define our unique test data
    test_filename = "replication_test_file.txt"
    test_data = f"Hello from the Primary at {datetime.now()}"
    test_data_bytes = test_data.encode('utf-8')
    
    print("--- 1. WRITE TO PRIMARY ---")
    print(f"Connecting to Primary at {primary_addr}...")
    
    try:
        # 2. Connect to Primary and write the data
        with grpc.insecure_channel(primary_addr) as channel:
            stub = fs_pb2_grpc.FileServiceStub(channel)
            
            # Open (or create) the file
            open_resp = stub.Open(fs_pb2.OpenRequest(
                filename=test_filename, 
                create_if_missing=True
            ))
            if open_resp.error:
                print(f"[FAIL] Error opening file on Primary: {open_resp.error}")
                return
            
            handle = open_resp.handle_id
            print(f"Got handle {handle} from Primary.")
            
            # Write the data
            write_resp = stub.Write(fs_pb2.WriteRequest(
                handle_id=handle, 
                offset=0, 
                data=test_data_bytes
            ))
            if write_resp.error:
                print(f"[FAIL] Error writing file on Primary: {write_resp.error}")
                return
            
            print(f"Wrote {write_resp.bytes_written} bytes to Primary.")
            
            # Close the handle
            stub.Close(fs_pb2.CloseRequest(handle_id=handle))
            print("Write to Primary complete.\n")

    except grpc.RpcError as e:
        print(f"\n[FATAL] Could not connect to Primary at {primary_addr}.")
        print(f"Error: {e.details()}. Is PrimaryServer.py running?")
        return

    # 3. Wait for replication
    print("--- 2. REPLICATION ---")
    print("Waiting 0.5 seconds for replication to complete...\n")
    time.sleep(0.5)

    print("--- 3. READ FROM FOLLOWER ---")
    print(f"Connecting to Follower at {follower_addr}...")
    read_data = b""

    try:
        # 4. Connect to Follower and read the data back
        with grpc.insecure_channel(follower_addr) as channel:
            stub = fs_pb2_grpc.FileServiceStub(channel)
            
            # Open the file (it MUST exist, so create_if_missing=False)
            open_resp = stub.Open(fs_pb2.OpenRequest(
                filename=test_filename, 
                create_if_missing=False
            ))
            if open_resp.error:
                print(f"[FAIL] Error opening file on Follower: {open_resp.error}")
                if open_resp.error == "ENOENT":
                    print("File not found on follower. Replication failed.")
                return

            handle = open_resp.handle_id
            print(f"Got handle {handle} from Follower.")
            
            # Read the data
            read_resp = stub.Read(fs_pb2.ReadRequest(
                handle_id=handle, 
                offset=0, 
                size=len(test_data_bytes) + 100 # Read more than expected
            ))
            if read_resp.error:
                print(f"[FAIL] Error reading file from Follower: {read_resp.error}")
                return
            
            read_data = read_resp.data
            
            # Close the handle
            stub.Close(fs_pb2.CloseRequest(handle_id=handle))
            print("Read from Follower complete.\n")
            
    except grpc.RpcError as e:
        print(f"\n[FATAL] Could not connect to Follower at {follower_addr}.")
        print(f"Error: {e.details()}. Is 2ndserver.py running?")
        return

    # 5. Verification
    print("--- 4. VERIFICATION ---")
    print(f"   Wrote to Primary:   '{test_data}'")
    print(f"   Read from Follower: '{read_data.decode('utf-8')}'")
    
    if read_data == test_data_bytes:
        print("\n*** ✅ TEST PASSED ***")
        print("Data on follower matches data written to primary.")
    else:
        print("\n*** ❌ TEST FAILED ***")
        print("Data on follower does not match.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test primary-follower replication.")
    parser.add_argument("--primary", type=str, default="localhost:50051",
                        help="Address of the Primary server")
    parser.add_argument("--follower", type=str, default="localhost:50052",
                        help="Address of the Follower server")
    args = parser.parse_args()

    run_replication_test(args.primary, args.follower)