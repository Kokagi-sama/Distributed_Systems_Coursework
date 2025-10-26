import argparse
import grpc
from concurrent import futures
import threading

import fs_pb2
import fs_pb2_grpc


class PrimaryServicer(fs_pb2_grpc.FileServiceServicer):
    def __init__(self, replicas):
        self.files = {}
        self.handles = {}
        self.next_handle = 1
        self.lock = threading.Lock()
        self.replicas = replicas

    def _replicate_all_followers(self, filename):
        f = self.files[filename]
        req = fs_pb2.ReplicateWriteRequest(
            filename=filename,
            contents=f["data"],
            version=f["version"],
        )
        for addr in self.replicas:
            try:
                with grpc.insecure_channel(addr) as ch:
                    stub = fs_pb2_grpc.FileServiceStub(ch)
                    stub.ReplicateWrite(req)
            except Exception as e:
                print(f"Couldn't replicate to {addr}: {e}")

    def Create(self, request, context):
        with self.lock:
            old_ver = self.files.get(request.filename, {"version": 0}) ["version"]
            self.files[request.filename] = {"data": b"", "version": old_ver + 1}
        self._replicate_all_followers(request.filename)
        return fs_pb2.CreateResponse(error="")

    def Open(self, request, context):
        created = False
        with self.lock:
            if request.create_if_missing and request.filename not in self.files:
                self.files[request.filename] = {"data": b"", "version": 1}
                created = True

            if request.filename not in self.files:
                return fs_pb2.OpenResponse(handle_id=0, error="ENOENT")

            hid = self.next_handle
            self.next_handle += 1
            self.handles[hid] = request.filename

        if created:
            self._replicate_all_followers(request.filename)

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
        with self.lock:
            fname = self.handles.get(request.handle_id)
            if fname is None:
                return fs_pb2.WriteResponse(bytes_written=0, error="bad handle")

            f = self.files[fname]
            buf = bytearray(f["data"])

            off = request.offset
            newbytes = request.data

            if off < 0:
                return fs_pb2.WriteResponse(bytes_written=0, error="bad offset")
            if off > len(buf):
                off = len(buf)

            end = off + len(newbytes)
            if end > len(buf):
                buf.extend(b"\x00" * (end - len(buf)))
            buf[off:end] = newbytes

            # write back
            f["data"] = bytes(buf)
            f["version"] += 1
            changed_filename = fname  # remember so we can replicate after lock

        # replicate after we update local state
        self._replicate_all_followers(changed_filename)

        return fs_pb2.WriteResponse(bytes_written=len(newbytes), error="")

    def Close(self, request, context):
        with self.lock:
            self.handles.pop(request.handle_id, None)
        return fs_pb2.CloseResponse(error="")

    def ReplicateWrite(self, request, context):
        # leader should not receive replication from others
        return fs_pb2.ReplicateWriteResponse(error="not follower")


def run_leader(port, replicas):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    fs_pb2_grpc.add_FileServiceServicer_to_server(PrimaryServicer(replicas), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[Primary] on {port}, replicas={replicas}")
    server.wait_for_termination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--replicas", type=str, default="")
    args = parser.parse_args()

    replica_list = [addr.strip() for addr in args.replicas.split(",") if addr.strip()]
    run_leader(args.port, replica_list)
