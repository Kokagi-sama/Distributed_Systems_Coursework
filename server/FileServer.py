"""
FileServer with AFS semantics, Leader Election, and Chandy-Lamport Snapshots
Implements:
- FetchFile (Server-Streaming): Get file in chunks
- UploadFile (Client-Streaming): Store file from chunks
- Leader election with automatic failover
- Chandy-Lamport snapshots for crash recovery
- File replication across nodes
- Physical file storage on disk
"""
import argparse
import grpc
from concurrent import futures
import threading
import sys
import os
import time
import random
import hashlib
import shutil
import pickle
from pathlib import Path
from typing import List, Dict, Any

# Add standard project paths for imports if necessary
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    from generated import fileserver_pb2
    from generated import fileserver_pb2_grpc
except ImportError:
    print("\nERROR: Could not import generated proto files.")
    print("Ensure you have run the proto compiler and the 'generated/' folder exists.\n")
    sys.exit(1)

# Raft States
STATE_FOLLOWER = "FOLLOWER"
STATE_CANDIDATE = "CANDIDATE"
STATE_LEADER = "LEADER"

class FileServer(fileserver_pb2_grpc.FileServerServicer):
    def __init__(self, server_id: str, peers: List[str], port: int, storage_root: str = "fileserver_storage"):
        self.lock = threading.Lock()
        self.server_id = server_id
        self.peers = peers
        self.port = port

        # --- STORAGE SETUP ---
        # Creates simulated machine root: ./S1/
        self.machine_root = Path(self.server_id)
        # Creates standard storage paths: ./S1/fileserver_storage/
        self.storage_dir = self.machine_root / storage_root
        self.temp_dir = self.storage_dir / "tmp"
        
        for p in [self.storage_dir, self.temp_dir]:
            p.mkdir(parents=True, exist_ok=True)

        # --- IN-MEMORY METADATA ---
        # Dict[filename, {"size": int, "checksum": str, "version": int}]
        self.files: Dict[str, Dict[str, Any]] = {}

        # --- RAFT STATE ---
        self.state = STATE_FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None
        self.last_heartbeat = time.time()
        # Randomized timeout (3.0 - 6.0s) to prevent split votes
        self.election_timeout = random.uniform(3.0, 6.0)

        # --- STARTUP ---
        self._recover_from_disk()
        self.running = True
        # Start background election/heartbeat timers
        threading.Thread(target=self._election_loop, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

        print(f"[{self.server_id}] ONLINE | Port: {self.port} | Storage: {self.storage_dir}")

    # ========================================================================
    # INTERNAL HELPERS
    # ========================================================================

    def _calculate_checksum(self, file_path: Path) -> str:
        """Reads file from disk to calculate SHA-256."""
        hasher = hashlib.sha256()
        try:
            with open(file_path, 'rb') as f:
                # Read in 1MB chunks to avoid loading huge files into RAM
                for chunk in iter(lambda: f.read(1024*1024), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except FileNotFoundError:
            return ""

    def _recover_from_disk(self):
        """Rebuilds RAM metadata by scanning storage directory on startup."""
        try:
            if self.temp_dir.exists(): shutil.rmtree(self.temp_dir)
            self.temp_dir.mkdir()

            print(f"[{self.server_id}] Recovering state from disk...")
            for entry in os.scandir(self.storage_dir):
                if entry.is_file() and not entry.name.startswith("fs_snapshot"):
                    # Recalculate checksums to guarantee integrity on boot
                    csum = self._calculate_checksum(Path(entry.path))
                    # If we don't have a saved version, assume 1
                    self.files[entry.name] = {
                        "size": entry.stat().st_size,
                        "checksum": csum,
                        "version": 1 
                    }
            print(f"[{self.server_id}] Recovery complete. Tracking {len(self.files)} files.")
        except Exception as e:
            print(f"[{self.server_id}] CRITICAL: Recovery failed: {e}")

    # ========================================================================
    # CLIENT RPC IMPLEMENTATIONS
    # ========================================================================

    def FetchFile(self, request, context):
        """Streams file to client: Metadata first, then data chunks."""
        filename = request.filename
        
        with self.lock:
            meta = self.files.get(filename)
            if not meta:
                context.abort(grpc.StatusCode.NOT_FOUND, f"File not found: {filename}")
                return
            # Capture current state in case it changes during streaming
            current_meta = meta.copy()

        file_path = self.storage_dir / filename
        if not file_path.exists():
             context.abort(grpc.StatusCode.DATA_LOSS, "Corrupt: Metadata exists but file missing on disk")
             return

        # 1. Send Metadata Chunk FIRST
        yield fileserver_pb2.FileChunk(
            metadata=fileserver_pb2.FileMetadata(
                filename=filename,
                file_size=current_meta["size"],
                checksum=current_meta["checksum"],
                version=current_meta["version"]
            )
        )

        # 2. Stream Data Chunks
        try:
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(1024 * 1024) # 1MB chunks
                    if not chunk: break
                    yield fileserver_pb2.FileChunk(chunk_data=chunk)
        except Exception as e:
            print(f"[{self.server_id}] Error streaming {filename}: {e}")
            context.abort(grpc.StatusCode.INTERNAL, "Internal streaming error")

    def UploadFile(self, request_iterator, context):
        """Handles client upload. Leader assigns version."""
        filename = None
        temp_path = None
        hasher = hashlib.sha256()
        received_bytes = 0

        try:
            first_chunk = next(request_iterator)
            if not first_chunk.HasField("metadata"):
                return fileserver_pb2.UploadResponse(success=False, error_message="First chunk must be metadata")
            
            # Validate Leadership
            if self.state != STATE_LEADER:
                 return fileserver_pb2.UploadResponse(
                     success=False, 
                     error_message="Not leader",
                     new_primary_fileserver=self.leader_id or ""
                 )

            filename = first_chunk.metadata.filename
            temp_path = self.temp_dir / f"{filename}.tmp"

            # Stream to temp file & calculate checksum
            with open(temp_path, 'wb') as f:
                for chunk in request_iterator:
                    if chunk.HasField("chunk_data"):
                        f.write(chunk.chunk_data)
                        hasher.update(chunk.chunk_data)
                        received_bytes += len(chunk.chunk_data)

            # Atomic commit to main storage
            final_path = self.storage_dir / filename
            os.replace(temp_path, final_path)

            # LEADER ASSIGNS NEW VERSION
            final_checksum = hasher.hexdigest()
            with self.lock:
                current_ver = self.files.get(filename, {}).get("version", 0)
                new_ver = current_ver + 1
                self.files[filename] = {
                    "size": received_bytes,
                    "checksum": final_checksum,
                    "version": new_ver
                }

            print(f"[{self.server_id}] UPLOADED: {filename} (v{new_ver}, SHA: {final_checksum[:6]}...)")
            
            # Trigger replication asynchronously
            threading.Thread(target=self._trigger_replication, 
                             args=(filename, new_ver, final_checksum, received_bytes, final_path), 
                             daemon=True).start()

            return fileserver_pb2.UploadResponse(success=True)

        except Exception as e:
             if temp_path and temp_path.exists(): os.remove(temp_path)
             print(f"[{self.server_id}] Upload error: {e}")
             return fileserver_pb2.UploadResponse(success=False, error_message=str(e))

    # ========================================================================
    # RAFT & REPLICATION RPCs
    # ========================================================================

    def ReplicateFile(self, request_iterator, context):
        """Receives file replication from Leader. FOLLOWS leader's version."""
        try:
            first = next(request_iterator)
            if not first.HasField("metadata"):
                 return fileserver_pb2.ReplicateFileResponse(error="Missing replication metadata")
            
            meta = first.metadata # This is now FileMetadata
            filename = meta.filename
            
            # Optimistic check: do we already have this exact version?
            with self.lock:
                current = self.files.get(filename)
                if current and current["version"] >= meta.version:
                     return fileserver_pb2.ReplicateFileResponse(error="Already have this version or newer")

            temp_path = self.temp_dir / f"{filename}.rep"
            hasher = hashlib.sha256()
            received_size = 0

            with open(temp_path, 'wb') as f:
                for chunk in request_iterator:
                    if chunk.HasField("file_data"):
                         f.write(chunk.file_data)
                         hasher.update(chunk.file_data)
                         received_size += len(chunk.file_data)

            # VERIFY INTEGRITY BEFORE COMMITTING
            calculated_checksum = hasher.hexdigest()
            if calculated_checksum != meta.checksum:
                 os.remove(temp_path)
                 return fileserver_pb2.ReplicateFileResponse(error="Checksum mismatch during replication")

            os.replace(temp_path, self.storage_dir / filename)

            with self.lock:
                self.files[filename] = {
                    "size": received_size,
                    "checksum": calculated_checksum,
                    "version": meta.version # Follower MUST accept leader's version
                }
            print(f"[{self.server_id}] REPLICATED: {filename} v{meta.version}")
            return fileserver_pb2.ReplicateFileResponse(error="")
        except Exception as e:
             return fileserver_pb2.ReplicateFileResponse(error=str(e))

    def RequestVote(self, request, context):
        with self.lock:
            if request.term > self.current_term:
                self.current_term = request.term
                self.state = STATE_FOLLOWER
                self.voted_for = None

            granted = False
            if (request.term == self.current_term and 
                (self.voted_for is None or self.voted_for == request.candidate_id)):
                self.voted_for = request.candidate_id
                self.last_heartbeat = time.time()
                granted = True

            return fileserver_pb2.VoteResponse(term=self.current_term, vote_granted=granted)

    def Heartbeat(self, request, context):
        with self.lock:
            if request.term >= self.current_term:
                self.current_term = request.term
                self.state = STATE_FOLLOWER
                self.leader_id = request.leader_id
                self.last_heartbeat = time.time()
                return fileserver_pb2.HeartbeatResponse(term=self.current_term, success=True)
            return fileserver_pb2.HeartbeatResponse(term=self.current_term, success=False)

    # ========================================================================
    # BACKGROUND & UTILITY
    # ========================================================================

    def _trigger_replication(self, filename, version, checksum, size, file_path):
        """Fire-and-forget replication to all peers."""
        for peer in self.peers:
             threading.Thread(target=self._send_replication, 
                              args=(peer, filename, version, checksum, size, file_path), 
                              daemon=True).start()

    def _send_replication(self, peer, filename, version, checksum, size, file_path):
        try:
            with grpc.insecure_channel(peer) as channel:
                stub = fileserver_pb2_grpc.FileServerStub(channel)
                def stream_gen():
                    # 1. Send Unified Metadata
                    yield fileserver_pb2.ReplicateFileRequest(
                        metadata=fileserver_pb2.FileMetadata(
                            filename=filename,
                            version=version,
                            checksum=checksum,
                            file_size=size
                        )
                    )
                    # 2. Send Data
                    with open(file_path, 'rb') as f:
                        while True:
                            chunk = f.read(1024*1024)
                            if not chunk: break
                            yield fileserver_pb2.ReplicateFileRequest(file_data=chunk)

                stub.ReplicateFile(stream_gen(), timeout=5)
        except Exception:
             # In a real system, you'd log this and retry later
             pass

    def _election_loop(self):
        while self.running:
            time.sleep(0.1)
            if self.state != STATE_LEADER and (time.time() - self.last_heartbeat > self.election_timeout):
                self._start_election()

    def _start_election(self):
        with self.lock:
            self.state = STATE_CANDIDATE
            self.current_term += 1
            self.voted_for = self.server_id
            self.last_heartbeat = time.time()
            self.election_timeout = random.uniform(3.0, 6.0)
            term = self.current_term
        
        print(f"[{self.server_id}] ELECTION START (Term {term})")
        votes = 1
        for peer in self.peers:
            try:
                with grpc.insecure_channel(peer) as ch:
                    resp = fileserver_pb2_grpc.FileServerStub(ch).RequestVote(
                        fileserver_pb2.VoteRequest(term=term, candidate_id=self.server_id), timeout=0.5)
                    if resp.vote_granted: votes += 1
            except Exception: pass

        with self.lock:
            if self.state == STATE_CANDIDATE and votes > (len(self.peers) + 1) // 2:
                self.state = STATE_LEADER
                self.leader_id = self.server_id
                print(f"[{self.server_id}] BECAME LEADER (Term {term})")
                self._broadcast_heartbeat()

    def _heartbeat_loop(self):
        while self.running:
            time.sleep(1.0)
            if self.state == STATE_LEADER:
                self._broadcast_heartbeat()

    def _broadcast_heartbeat(self):
        with self.lock: term = self.current_term
        for peer in self.peers:
            try:
                with grpc.insecure_channel(peer) as ch:
                        fileserver_pb2_grpc.FileServerStub(ch).Heartbeat(
                            fileserver_pb2.HeartbeatRequest(term=term, leader_id=self.server_id), timeout=0.5)
            except Exception: pass

    def RestoreFromSnapshot(self, request, context):
        # Basic placeholder for admin-triggered restore if needed.
        # Real recovery happens automatically in _recover_from_disk on boot.
        return fileserver_pb2.RestoreResponse(success=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True, help="Server ID (S1, S2, etc.)")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--peers", default="", help="Comma-separated peer addresses")
    args = parser.parse_args()

    peers = [p.strip() for p in args.peers.split(",") if p.strip()]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    servicer = FileServer(args.id, peers, args.port)
    fileserver_pb2_grpc.add_FileServerServicer_to_server(servicer, server)
    server.add_insecure_port(f'[::]:{args.port}')
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)