import argparse
import grpc
from concurrent import futures
import threading
import sys
import os

# This line adds your project's 'root' directory (one level up from 'server')
# to Python's list of places to look for modules.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# --- Now your other imports can work ---
from proto import fs_pb2
from proto import fs_pb2_grpc

class FollowerServicer(fs_pb2_grpc.FileServiceServicer):
    def __init__(self):
        self.files = {}
        self.handles = {}
        self.next_handle = 1
        self.lock = threading.Lock()

    def Create(self, request, context):
        # Replica servers should not be able to create
        return fs_pb2.CreateResponse(error="read-only replica")

    def Open(self, request, context):
        with self.lock:
            if request.create_if_missing:
                return fs_pb2.OpenResponse(handle_id=0,
                                            error="read-only replica")
            if request.filename not in self.files:
                return fs_pb2.OpenResponse(handle_id=0,
                                            error="ENOENT")
            hid = self.next_handle
            self.next_handle += 1
            self.handles[hid] = request.filename
            return fs_pb2.OpenResponse(handle_id=hid, error="")

    def Read(self, request, context):
        with self.lock:
            fname = self.handles.get(request.handle_id)
            if fname is None:
                return fs_pb2.ReadResponse(data=b"", error="bad handle")
            data = self.files[fname]["data"]
            off = request.offset
            end = off + request.size
            if off < 0 or off > len(data):
                return fs_pb2.ReadResponse(data=b"", error="")
            chunk = data[off:end]
            return fs_pb2.ReadResponse(data=chunk, error="")

    def Write(self, request, context):
        # Replica servers should not be able to create
        return fs_pb2.WriteResponse(bytes_written=0,
                                     error="read-only replica")

    def Close(self, request, context):
        with self.lock:
            self.handles.pop(request.handle_id, None)
        return fs_pb2.CloseResponse(error="")

    def ReplicateWrite(self, request, context):
        with self.lock:
            cur = self.files.get(request.filename)
            if (cur is None) or (request.version >= cur["version"]):
                self.files[request.filename] = {
                    "data": request.contents,
                    "version": request.version,
                }
        return fs_pb2.ReplicateWriteResponse(error="")


def run_follower(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    fs_pb2_grpc.add_FileServiceServicer_to_server(
        FollowerServicer(),
        server
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[Replica] on {port}")
    server.wait_for_termination()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=50052)
    args = p.parse_args()

    run_follower(args.port)
