import grpc
import fs_pb2
import fs_pb2_grpc

def main():
    # connect to the primary
    with grpc.insecure_channel("localhost:50051") as ch:
        stub = fs_pb2_grpc.FileServiceStub(ch)

        # 1. Create a new file on the primary
        stub.Create(fs_pb2.CreateRequest(filename="test.txt"))
        print("[Client] Created test.txt on primary")

        # 2. Open it
        open_resp = stub.Open(fs_pb2.OpenRequest(filename="test.txt", create_if_missing=True))
        print(f"[Client] Opened handle {open_resp.handle_id}")

        # 3. Write data
        write_resp = stub.Write(
            fs_pb2.WriteRequest(handle_id=open_resp.handle_id, offset=0, data=b"Hello replication!")
        )
        print(f"[Client] Wrote {write_resp.bytes_written} bytes")

        # 4. Close it
        stub.Close(fs_pb2.CloseRequest(handle_id=open_resp.handle_id))
        print("[Client] Closed file")

if __name__ == "__main__":
    main()
