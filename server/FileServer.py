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
import json
from pathlib import Path
from typing import List, Dict, Any, Optional, Iterator

# --- PATH SETUP ---
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    from generated import fileserver_pb2
    from generated import fileserver_pb2_grpc
except ImportError:
    print("\nERROR: Could not import generated proto files.")
    sys.exit(1)

# --- RAFT STATES ---
STATE_FOLLOWER: str = "FOLLOWER"
STATE_CANDIDATE: str = "CANDIDATE"
STATE_LEADER: str = "LEADER"

class FileServer(fileserver_pb2_grpc.FileServerServicer):
    """
    Distributed File Server implementing Raft for consensus.
    """

    def __init__(
        self, server_id: str, peers: List[str], port: int, storage_root: str = "fileserver_storage"
    ) -> None:
        self.lock: threading.Lock = threading.Lock()
        self.server_id: str = server_id
        self.peers: List[str] = peers
        self.port: int = port

        # --- PERSISTENT NETWORK CHANNELS ---
        # Initialize channels once. gRPC handles reconnection internally.
        self.peer_stubs: Dict[str, fileserver_pb2_grpc.FileServerStub] = {}
        for peer in self.peers:
            channel = grpc.insecure_channel(peer)
            self.peer_stubs[peer] = fileserver_pb2_grpc.FileServerStub(channel)

        # --- STORAGE SETUP ---
        self.machine_root: Path = Path(self.server_id)
        self.storage_dir: Path = self.machine_root / storage_root
        self.temp_dir: Path = self.storage_dir / "tmp"
        for p in [self.storage_dir, self.temp_dir]:
            p.mkdir(parents=True, exist_ok=True)

        # --- IN-MEMORY STATE ---
        self.files: Dict[str, Dict[str, Any]] = {}
        # Raft Log Proxy: Track highest version seen to judge candidates
        self.max_version: int = 0 

        # --- RAFT STATE ---
        self.state: str = STATE_FOLLOWER
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.leader_id: Optional[str] = None
        self.last_heartbeat: float = time.time()
        self.election_timeout: float = random.uniform(3.0, 6.0)

        # --- STARTUP ---
        self._recover_state_from_disk()
        self.running: bool = True
        
        # Start background Raft threads
        threading.Thread(target=self._consensus_election_monitor, daemon=True).start()
        threading.Thread(target=self._consensus_heartbeat_emitter, daemon=True).start()

        print(f"[{self.server_id}] ONLINE | Port: {self.port} | Max Version: {self.max_version}")

    # ========================================================================
    # HELPER FUNCTIONS (INTERNAL)
    # ========================================================================

    def _calculate_file_checksum(self, file_path: Path) -> str:
        """Calculates SHA-256 checksum of a file on disk."""
        hasher = hashlib.sha256()
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(1024*1024), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except FileNotFoundError: return ""

    def _save_metadata(self, filename: str, metadata: Dict[str, Any]) -> None:
        """Persists file metadata to a JSON file on disk."""
        meta_path = self.storage_dir / f"{filename}.meta"
        with open(meta_path, 'w') as f:
            json.dump(metadata, f)

    def _recover_state_from_disk(self) -> None:
        """Scans storage directory on startup to rebuild in-memory file map from .meta files."""
        if self.temp_dir.exists(): shutil.rmtree(self.temp_dir)
        self.temp_dir.mkdir()

        for entry in os.scandir(self.storage_dir):
            if entry.is_file() and entry.name.endswith(".meta"):
                try:
                    with open(entry.path, 'r') as f:
                        meta = json.load(f)
                    
                    filename = entry.name[:-5] # remove .meta extension
                    # Verify the actual data file still exists
                    if (self.storage_dir / filename).exists():
                        self.files[filename] = meta
                        if meta['version'] > self.max_version:
                            self.max_version = meta['version']
                except Exception as e:
                    print(f"[{self.server_id}] Corrupt metadata for {entry.name}: {e}")

    # ========================================================================
    # CLIENT-FACING RPCs
    # ========================================================================

    def UploadFile(
        self, request_iterator: Iterator[fileserver_pb2.FileChunk], context: grpc.ServicerContext
    ) -> fileserver_pb2.UploadResponse:
        """Handles file upload from client. Only accepted if LEADER."""
        try:
            first = next(request_iterator)
            if not first.HasField("metadata"): 
                return fileserver_pb2.UploadResponse(success=False, error_message="Missing metadata")
            
            if self.state != STATE_LEADER:
                 return fileserver_pb2.UploadResponse(
                        success=False, 
                        error_message="Not leader", 
                        new_primary_fileserver=self.leader_id or ""
                 )

            filename = first.metadata.filename
            temp_path = self.temp_dir / f"{filename}.tmp"
            hasher = hashlib.sha256()
            received_bytes = 0

            # Stream to temp file first
            with open(temp_path, 'wb') as f:
                for chunk in request_iterator:
                    if chunk.HasField("chunk_data"):
                        f.write(chunk.chunk_data)
                        hasher.update(chunk.chunk_data)
                        received_bytes += len(chunk.chunk_data)

            # Commit file to storage
            final_path = self.storage_dir / filename
            os.replace(temp_path, final_path)
            final_csum = hasher.hexdigest()

            with self.lock:
                current_ver = self.files.get(filename, {}).get("version", 0)
                new_ver = current_ver + 1
                self.files[filename] = {"size": received_bytes, "checksum": final_csum, "version": new_ver}
                
                self._save_metadata(filename, self.files[filename])
                
                # Update log proxy for Raft safety
                if new_ver > self.max_version: self.max_version = new_ver

            print(f"[{self.server_id}] UPLOAD COMMIT: {filename} (v{new_ver})")
            
            # Trigger async replication to followers
            threading.Thread(target=self._async_replication_manager, 
                             args=(filename, new_ver, final_csum, received_bytes, final_path), 
                             daemon=True).start()
            return fileserver_pb2.UploadResponse(success=True)
        except Exception as e:
             return fileserver_pb2.UploadResponse(success=False, error_message=str(e))

    def FetchFile(
        self, request: fileserver_pb2.FetchRequest, context: grpc.ServicerContext
    ) -> Iterator[fileserver_pb2.FileChunk]:
         """Streams file download to client."""
         filename = request.filename
         with self.lock:
            meta = self.files.get(filename)
            if not meta:
                context.abort(grpc.StatusCode.NOT_FOUND, "File not found")
                return
            current_meta = meta.copy()

         file_path = self.storage_dir / filename
         if not file_path.exists():
             context.abort(grpc.StatusCode.DATA_LOSS, "File missing on disk")
             return

         # Send metadata first
         yield fileserver_pb2.FileChunk(metadata=fileserver_pb2.FileMetadata(
                filename=filename, file_size=current_meta["size"],
                checksum=current_meta["checksum"], version=current_meta["version"]))
         
         # Stream file data
         with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk: break
                yield fileserver_pb2.FileChunk(chunk_data=chunk)

    # ========================================================================
    # REPLICATION SERVICES
    # ========================================================================

    def ReplicateFile(
        self, request_iterator: Iterator[fileserver_pb2.ReplicateFileRequest], context: grpc.ServicerContext
    ) -> fileserver_pb2.ReplicateFileResponse:
        """Handles incoming replication stream from Leader."""
        try:
            first = next(request_iterator)
            if not first.HasField("metadata"): 
                return fileserver_pb2.ReplicateFileResponse(error="No metadata")
            meta = first.metadata
            
            with self.lock:
                # Idempotency: Don't overwrite if we already have this version
                if self.files.get(meta.filename, {}).get("version", 0) >= meta.version:
                    return fileserver_pb2.ReplicateFileResponse(error="Already have version")

            temp_path = self.temp_dir / f"{meta.filename}.rep"
            hasher = hashlib.sha256()
            with open(temp_path, 'wb') as f:
                for chunk in request_iterator:
                    if chunk.HasField("file_data"):
                        f.write(chunk.file_data)
                        hasher.update(chunk.file_data)

            if hasher.hexdigest() != meta.checksum:
                os.remove(temp_path)
                return fileserver_pb2.ReplicateFileResponse(error="Checksum mismatch")

            os.replace(temp_path, self.storage_dir / meta.filename)
            with self.lock:
                self.files[meta.filename] = {"size": meta.file_size, "checksum": meta.checksum, "version": meta.version}
                
                self._save_metadata(meta.filename, self.files[meta.filename])
                
                if meta.version > self.max_version: self.max_version = meta.version
            
            print(f"[{self.server_id}] REPLICATED: {meta.filename} (v{meta.version})")
            return fileserver_pb2.ReplicateFileResponse(error="")
        except Exception as e: 
            return fileserver_pb2.ReplicateFileResponse(error=str(e))

    def _async_replication_manager(
        self, filename: str, version: int, checksum: str, size: int, file_path: Path
    ) -> None:
        """Spawns threads to replicate a file to all peers."""
        for peer in self.peers:
             threading.Thread(target=self._perform_peer_replication, 
                              args=(peer, filename, version, checksum, size, file_path), daemon=True).start()

    def _perform_peer_replication(
        self, peer: str, filename: str, version: int, checksum: str, size: int, file_path: Path
    ) -> None:
        """Worker: Replicates file to a single peer."""
        try:
            stub = self.peer_stubs.get(peer)
            if not stub: return
            def stream() -> Iterator[fileserver_pb2.ReplicateFileRequest]:
                yield fileserver_pb2.ReplicateFileRequest(metadata=fileserver_pb2.FileMetadata(
                        filename=filename, version=version, checksum=checksum, file_size=size))
                with open(file_path, 'rb') as f:
                    while True:
                        c = f.read(1024*1024)
                        if not c: break
                        yield fileserver_pb2.ReplicateFileRequest(file_data=c)
            # Long timeout for large files
            stub.ReplicateFile(stream(), timeout=60)
        except Exception as e: 
            print(f"[{self.server_id}] Replication FAILED to {peer}: {e}")

    # ========================================================================
    # RAFT CONSENSUS (VOTING & HEARTBEATS)
    # ========================================================================

    def RequestVote(
        self, request: fileserver_pb2.VoteRequest, context: grpc.ServicerContext
    ) -> fileserver_pb2.VoteResponse:
        """Raft: Handles vote requests from candidates."""
        with self.lock:
            # 1. Step down if term is higher
            if request.term > self.current_term:
                print(f"[{self.server_id}] STEP DOWN: Higher term {request.term} in RequestVote")
                self.current_term = request.term
                self.state = STATE_FOLLOWER
                self.voted_for = None

            # 2. Ignore old terms
            if request.term < self.current_term:
                return fileserver_pb2.VoteResponse(term=self.current_term, vote_granted=False)

            # 3. Log Matching Safety Check: Deny if candidate is stale
            if request.last_log_index < self.max_version:
                 print(f"[{self.server_id}] VOTE DENIED: Candidate {request.candidate_id} is stale")
                 return fileserver_pb2.VoteResponse(term=self.current_term, vote_granted=False)

            # 4. Grant vote if eligible
            if self.voted_for is None or self.voted_for == request.candidate_id:
                self.voted_for = request.candidate_id
                self.last_heartbeat = time.time()
                print(f"[{self.server_id}] VOTE GRANTED to {request.candidate_id} (Term {self.current_term})")
                return fileserver_pb2.VoteResponse(term=self.current_term, vote_granted=True)
            
            return fileserver_pb2.VoteResponse(term=self.current_term, vote_granted=False)

    def Heartbeat(
        self, request: fileserver_pb2.HeartbeatRequest, context: grpc.ServicerContext
    ) -> fileserver_pb2.HeartbeatResponse:
        """Raft: Handles heartbeats from Leader to maintain authority."""
        with self.lock:
            if request.term < self.current_term:
                return fileserver_pb2.HeartbeatResponse(term=self.current_term, success=False)

            if request.term >= self.current_term:
                # If we thought WE were leader, step down now.
                if self.state == STATE_LEADER and request.leader_id != self.server_id:
                       print(f"[{self.server_id}] STEP DOWN: Discovered valid leader {request.leader_id}")
                self.current_term = request.term
                self.state = STATE_FOLLOWER
                self.leader_id = request.leader_id
                self.last_heartbeat = time.time()
                self.voted_for = None

            return fileserver_pb2.HeartbeatResponse(term=self.current_term, success=True)

    def _trigger_election(self) -> None:
        """Starts a new election when heartbeat timeout occurs."""
        with self.lock:
            self.state = STATE_CANDIDATE
            self.current_term += 1
            self.voted_for = self.server_id
            self.last_heartbeat = time.time()
            self.election_timeout = random.uniform(3.0, 6.0)
            term = self.current_term
            last_index = self.max_version
        
        print(f"[{self.server_id}] ELECTION STARTED (Term {term}, LastVer {last_index})")
        votes = 1
        
        def request_vote_worker(peer: str, stub: fileserver_pb2_grpc.FileServerStub) -> None:
            nonlocal votes
            try:
                resp = stub.RequestVote(
                    fileserver_pb2.VoteRequest(
                        term=term, candidate_id=self.server_id,
                        last_log_index=last_index, last_log_term=0
                    ), timeout=2.0)
                with self.lock:
                    # If peer has higher term, abort election immediately
                    if resp.term > self.current_term:
                        self.current_term = resp.term
                        self.state = STATE_FOLLOWER
                        self.voted_for = None
                        return
                    if resp.vote_granted and self.state == STATE_CANDIDATE and self.current_term == term:
                        votes += 1
            except Exception: pass

        # Send vote requests in parallel
        with futures.ThreadPoolExecutor(max_workers=len(self.peers)) as ex:
            futures.wait([ex.submit(request_vote_worker, p, self.peer_stubs[p]) for p in self.peers], timeout=2.5)

        with self.lock:
            if self.state == STATE_CANDIDATE and self.current_term == term and votes > (len(self.peers) + 1) // 2:
                self.state = STATE_LEADER
                self.leader_id = self.server_id
                print(f"[{self.server_id}] *** ASSUMED LEADERSHIP (Term {term}) ***")
                # Send immediate heartbeat to assert authority
                threading.Thread(target=self._emit_heartbeats, daemon=True).start()

    def _emit_heartbeats(self) -> None:
        """Leader only: Sends heartbeats to all peers."""
        with self.lock: term = self.current_term
        for peer, stub in self.peer_stubs.items():
            try:
                resp = stub.Heartbeat(fileserver_pb2.HeartbeatRequest(term=term, leader_id=self.server_id), timeout=0.5)
                with self.lock:
                    # If a peer replies with a higher term, step down.
                    if resp.term > self.current_term:
                         self.current_term = resp.term
                         self.state = STATE_FOLLOWER
                         return
            except Exception: pass

    def _consensus_election_monitor(self) -> None:
        """Background loop: checks if we need to start an election."""
        while self.running:
            time.sleep(0.1)
            if self.state != STATE_LEADER and (time.time() - self.last_heartbeat > self.election_timeout):
                self._trigger_election()

    def _consensus_heartbeat_emitter(self) -> None:
        """Background loop: if Leader, sends periodic heartbeats."""
        while self.running:
            time.sleep(1.0)
            if self.state == STATE_LEADER: self._emit_heartbeats()

    # ========================================================================
    # SNAPSHOT SERVICES
    # ========================================================================

    def RestoreFromSnapshot(
        self, request: fileserver_pb2.RestoreRequest, context: grpc.ServicerContext
    ) -> fileserver_pb2.RestoreResponse:
        """Restores system state from a specific snapshot ID."""
        # Placeholder: Implement actual snapshot restoration logic here if needed.
        return fileserver_pb2.RestoreResponse(success=True, error="")

# ========================================================================
# MAIN ENTRY POINT
# ========================================================================

def run_server(server_id: str, port: int, peers: List[str], storage_dir: str) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    servicer = FileServer(server_id, peers, port, storage_dir)
    fileserver_pb2_grpc.add_FileServerServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[{server_id}] FileServer started on port {port}")
    try: server.wait_for_termination()
    except KeyboardInterrupt: server.stop(0)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=str, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--peers", type=str, default="")
    parser.add_argument("--storage", type=str, default="./fileserver_storage")
    args = parser.parse_args()
    run_server(args.id, args.port, [p for p in args.peers.split(",") if p.strip()], args.storage)