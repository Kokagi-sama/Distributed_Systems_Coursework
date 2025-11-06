"""
FileServer with AFS semantics, Leader Election, and Chandy-Lamport Snapshots

Implements:
- FetchFile (Server-Streaming): Get file in chunks
- UploadFile (Client-Streaming): Store file from chunks
- Leader election with automatic failover
- Chandy-Lamport snapshots for crash recovery
- File replication across nodes
"""

import argparse
import grpc
from concurrent import futures
import threading
import sys
import os
import time
import random
import pickle
from pathlib import Path
# Added typing imports
from typing import List, Dict, Optional, Set, Any, Iterator

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Import generated proto (adjust path as needed)
from generated import fileserver_pb2
from generated import fileserver_pb2_grpc

# Server states
STATE_FOLLOWER: str = "FOLLOWER"
STATE_CANDIDATE: str = "CANDIDATE"
STATE_LEADER: str = "LEADER"


class FileServer(fileserver_pb2_grpc.FileServerServicer):
    """
    FileServer with Leader Election and Snapshots
    """
    
    def __init__(
        self,
        server_id: str,
        peers: List[str],
        port: int,
        storage_dir: str = "./fileserver_storage",
    ) -> None:
        # File storage
        # Type: Dict[filename, {"data": bytes, "version": int}]
        self.files: Dict[str, Dict[str, Any]] = {}
        self.lock: threading.Lock = threading.Lock()
        
        # Storage directory for persistence
        self.storage_dir: Path = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        
        # Server identity
        self.server_id: str = server_id
        self.peers: List[str] = peers
        self.port: int = port
        
        # Leader election state
        self.state: str = STATE_FOLLOWER
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.leader_id: Optional[str] = None
        
        # Timing
        self.last_heartbeat: float = time.time()
        self.election_timeout: float = self._random_election_timeout()
        self.heartbeat_interval: float = 1.0
        
        # Snapshot state
        self.snapshots: Dict[str, FileServer.SnapshotState] = {}
        self.snapshot_lock: threading.Lock = threading.Lock()
        
        # Start background threads
        self.running: bool = True
        self.election_thread: threading.Thread = threading.Thread(
            target=self._election_timer, daemon=True
        )
        self.heartbeat_thread: threading.Thread = threading.Thread(
            target=self._send_heartbeats, daemon=True
        )
        self.election_thread.start()
        self.heartbeat_thread.start()
        
        print(f"[{server_id}] FileServer initialized")
        print(f"[{server_id}] Storage: {self.storage_dir}")
        print(f"[{server_id}] Peers: {peers}")
    
    def _random_election_timeout(self) -> float:
        return random.uniform(5.0, 10.0)
    
    def _is_leader(self) -> bool:
        return self.state == STATE_LEADER
    
    # ========================================================================
    # CLIENT-FACING FILE OPERATIONS
    # ========================================================================
    
    def FetchFile(
        self, request: fileserver_pb2.MetaData, context: grpc.ServicerContext
    ) -> Iterator[fileserver_pb2.FetchResponse]:
        """
        Fetch file in chunks (Server-Streaming RPC).
        """
        # Reconstruct filename from MetaData
        filename = f"{request.filename}{request.extension}"
        
        print(f"[{self.server_id}] FetchFile (streaming) request: {filename}")
        
        with self.lock:
            if filename not in self.files:
                print(f"[{self.server_id}] File not found: {filename}")
                # Send one error message and stop
                yield fileserver_pb2.FetchResponse(
                    success=False,
                    error_message=f"File not found: {filename}",
                )
                return  # Stop the generator
            
            # Get file data from internal storage
            file_data = self.files[filename]["data"]
        
        # Send the file in 1MB chunks
        chunk_size = 1024 * 1024  # 1MB
        for i in range(0, len(file_data), chunk_size):
            chunk = file_data[i : i + chunk_size]
            yield fileserver_pb2.FetchResponse(
                success=True,
                file_data=chunk
            )
        
        print(f"[{self.server_id}] Finished streaming {filename}")
    
    def UploadFile(
        self, request_iterator: Iterator[fileserver_pb2.UploadRequest], context: grpc.ServicerContext
    ) -> fileserver_pb2.UploadResponse:
        """
        Upload/Store file from chunks (Client-Streaming RPC).
        Only leader can accept writes!
        """
        print(f"[{self.server_id}] UploadFile (streaming) request received...")
        
        metadata: Optional[fileserver_pb2.MetaData] = None
        file_chunks = []
        
        try:
            # 1. Loop over the client's stream
            for request in request_iterator:
                # Check the 'oneof' field to see what kind of data this is
                if request.HasField("metadata"):
                    if metadata is not None:
                        raise ValueError("Client sent metadata twice")
                    metadata = request.metadata
                elif request.HasField("file_data"):
                    if metadata is None:
                        raise ValueError("Client sent file data before metadata")
                    file_chunks.append(request.file_data)
            
            if not metadata:
                raise ValueError("Client sent no metadata")

        except Exception as e:
            print(f"[{self.server_id}] Error in upload stream: {e}")
            return fileserver_pb2.UploadResponse(
                success=False, error_message=str(e)
            )

        # 2. Reassemble the file
        filename = f"{metadata.filename}{metadata.extension}"
        file_data = b"".join(file_chunks)
        
        print(f"[{self.server_id}] Stream complete for {filename} ({len(file_data)} bytes)")
        
        # 3. Acquire lock and perform the write (if leader)
        with self.lock:
            # Check if we're the leader
            if not self._is_leader():
                error_msg = (
                    f"Not leader. Current leader: {self.leader_id or 'unknown'}"
                )
                print(f"[{self.server_id}] Rejected upload: {error_msg}")
                return fileserver_pb2.UploadResponse(
                    success=False, error_message=error_msg
                )
            
            # Internal versioning logic is maintained for replication,
            # even though the client-facing API no longer uses it.
            if filename in self.files:
                # Update existing file
                self.files[filename]["data"] = file_data
                self.files[filename]["version"] += 1
                new_version = self.files[filename]["version"]
            else:
                # Create new file
                self.files[filename] = {"data": file_data, "version": 1}
                new_version = 1
            
            print(f"[{self.server_id}] Stored {filename} (version {new_version})")
        
        # 4. Replicate to followers
        self._replicate_to_followers(filename)
        
        # 5. Send the single response
        return fileserver_pb2.UploadResponse(
            success=True, error_message=""
        )

    def RetrieveFileList(
            self, request: fileserver_pb2.RetrieveFileListRequest, context: grpc.ServicerContext
        ) -> fileserver_pb2.RetrieveFileListResponse:
            """
            Returns a map of <int, MetaData> for all files stored on the server.
            This implementation ignores the request.data_type field for now.
            """
            print(f"[{self.server_id}] RetrieveFileList request")
            
            file_list_map = {}
            file_id_counter = 0
            
            with self.lock:
                # Get all filenames (keys) from the files dictionary
                all_filenames = list(self.files.keys())
            
            for f in all_filenames:
                try:
                    # Split the full filename back into stem and extension
                    p = Path(f)
                    filename_stem = p.stem
                    extension = p.suffix  # This will include the dot, e.g., ".txt"
                    
                    # Populate the map <int32, MetaData>
                    file_list_map[file_id_counter] = fileserver_pb2.MetaData(
                        filename=filename_stem,
                        extension=extension
                    )
                    file_id_counter += 1
                    
                except Exception as e:
                    # This might happen if a filename is unusual, e.g., ".something"
                    print(f"[{self.server_id}] Warning: could not parse filename '{f}': {e}")

            print(f"[{self.server_id}] Returning {len(file_list_map)} filenames.")
            
            # This matches the 'map<int32, MetaData> file_list = 1;' in your .proto
            return fileserver_pb2.RetrieveFileListResponse(
                file_list=file_list_map
            )
    # ========================================================================
    # LEADER ELECTION
    # ========================================================================
    
    def _election_timer(self) -> None:
        """Monitor heartbeats and trigger elections"""
        while self.running:
            time.sleep(0.5)
            
            with self.lock:
                if self.state == STATE_LEADER:
                    continue
                
                elapsed = time.time() - self.last_heartbeat
                if elapsed > self.election_timeout:
                    print(
                        f"[{self.server_id}] Election timeout! Starting election..."
                    )
                    self._start_election()
    
    def _start_election(self) -> None:
        """Start new election"""
        self.state = STATE_CANDIDATE
        self.current_term += 1
        self.voted_for = self.server_id
        self.election_timeout = self._random_election_timeout()
        self.last_heartbeat = time.time()
        
        term = self.current_term
        candidate_id = self.server_id
        
        self.lock.release()
        
        try:
            print(f"[{self.server_id}] Starting election for term {term}")
            votes_received = 1
            votes_needed = (len(self.peers) + 1) // 2 + 1
            
            for peer in self.peers:
                try:
                    with grpc.insecure_channel(peer) as channel:
                        stub = fileserver_pb2_grpc.FileServerStub(channel)

                        request = fileserver_pb2.VoteRequest(
                            term=term,
                            candidate_id=candidate_id,
                            last_log_index=0,
                            last_log_term=0,
                        )
                        response = stub.RequestVote(request, timeout=2.0)
                        
                        if response.vote_granted:
                            votes_received += 1
                            print(f"[{self.server_id}] Vote from {peer}")
                        
                        if response.term > term:
                            with self.lock:
                                if response.term > self.current_term:
                                    self.current_term = response.term
                                    self.state = STATE_FOLLOWER
                                    self.voted_for = None
                            return
                except Exception as e:
                    print(
                        f"[{self.server_id}] Vote request failed to {peer}: {e}"
                    )
            
            with self.lock:
                if (
                    self.state == STATE_CANDIDATE
                    and votes_received >= votes_needed
                ):
                    print(
                        f"[{self.server_id}] WON election for term {term}!"
                    )
                    self.state = STATE_LEADER
                    self.leader_id = self.server_id
                else:
                    print(
                        f"[{self.server_id}] LOST election ({votes_received}/{votes_needed})"
                    )
                    self.state = STATE_FOLLOWER
        finally:
            self.lock.acquire()
    
    def RequestVote(
        self, request: fileserver_pb2.VoteRequest, context: grpc.ServicerContext
    ) -> fileserver_pb2.VoteResponse:
        """Handle vote request"""
        with self.lock:
            if request.term < self.current_term:
                return fileserver_pb2.VoteResponse(
                    term=self.current_term, vote_granted=False
                )
            
            if request.term > self.current_term:
                self.current_term = request.term
                self.state = STATE_FOLLOWER
                self.voted_for = None
                self.leader_id = None
            
            vote_granted = False
            if self.voted_for is None or self.voted_for == request.candidate_id:
                self.voted_for = request.candidate_id
                self.last_heartbeat = time.time()
                vote_granted = True
            
            return fileserver_pb2.VoteResponse(
                term=self.current_term, vote_granted=vote_granted
            )
    
    def Heartbeat(
        self, request: fileserver_pb2.HeartbeatRequest, context: grpc.ServicerContext
    ) -> fileserver_pb2.HeartbeatResponse:
        """Handle heartbeat from leader"""
        with self.lock:
            if request.term < self.current_term:
                return fileserver_pb2.HeartbeatResponse(
                    term=self.current_term, success=False
                )
            
            if request.term >= self.current_term:
                self.current_term = request.term
                self.state = STATE_FOLLOWER
                self.leader_id = request.leader_id
                self.last_heartbeat = time.time()
                self.voted_for = None
            
            return fileserver_pb2.HeartbeatResponse(
                term=self.current_term, success=True
            )
    
    def _send_heartbeats(self) -> None:
        """Send heartbeats when leader"""
        while self.running:
            time.sleep(self.heartbeat_interval)
            
            with self.lock:
                if self.state != STATE_LEADER:
                    continue
                term = self.current_term
                leader_id = self.server_id
            
            for peer in self.peers:
                try:
                    with grpc.insecure_channel(peer) as channel:
                        stub = fileserver_pb2_grpc.FileServerStub(channel)

                        request = fileserver_pb2.HeartbeatRequest(
                            term=term, leader_id=leader_id
                        )
                        stub.Heartbeat(request, timeout=1.0)
                except Exception:
                    pass
    
    # ========================================================================
    # REPLICATION
    # ========================================================================
    
    def _generate_replication_requests(
        self, filename: str, file_data: bytes, version: int
    ) -> Iterator[fileserver_pb2.ReplicateFileRequest]:
        """Generator for streaming file replication."""
        
        # 1. Send metadata
        metadata = fileserver_pb2.ReplicateFileMetadata(
            filename=filename, version=version
        )
        yield fileserver_pb2.ReplicateFileRequest(metadata=metadata)

        # 2. Send file in 1MB chunks
        chunk_size = 1024 * 1024  # 1MB
        for i in range(0, len(file_data), chunk_size):
            chunk = file_data[i : i + chunk_size]
            yield fileserver_pb2.ReplicateFileRequest(file_data=chunk)
    
    def _replicate_to_followers(self, filename: str) -> None:
        """Replicate file to all followers"""
        with self.lock:
            if filename not in self.files:
                return
            f = self.files[filename]
            file_data = f["data"] 
            version = f["version"]
        
        for peer in self.peers:
            try:
                
                with grpc.insecure_channel(peer) as channel:
                    stub = fileserver_pb2_grpc.FileServerStub(channel)
                    
                    
                    request_iterator = self._generate_replication_requests(
                        filename, file_data, version
                    )

                    stub.ReplicateFile(request_iterator, timeout=30.0) 
            
            except Exception as e:
                print(
                    f"[{self.server_id}] Replication to {peer} failed: {e}"
                )
    
    def ReplicateFile(
        self,
        request_iterator: Iterator[fileserver_pb2.ReplicateFileRequest],
        context: grpc.ServicerContext,
    ) -> fileserver_pb2.ReplicateFileResponse:
        """Receive replicated file from leader (client-streaming)"""
        
        metadata: Optional[fileserver_pb2.ReplicateFileMetadata] = None
        file_chunks = []
        filename = ""
        version = 0

        try:
            for request in request_iterator:
                if request.HasField("metadata"):
                    if metadata is not None:
                        raise ValueError("Replication client sent metadata twice")
                    metadata = request.metadata
                    filename = metadata.filename
                    version = metadata.version
                elif request.HasField("file_data"):
                    if metadata is None:
                        raise ValueError("Replication client sent file data before metadata")
                    file_chunks.append(request.file_data)
            
            if not metadata:
                raise ValueError("Replication client sent no metadata")

        except Exception as e:
            print(f"[{self.server_id}] Error in replication stream: {e}")
            return fileserver_pb2.ReplicateFileResponse(error=str(e))

        file_data = b"".join(file_chunks)

        with self.lock:
            cur = self.files.get(filename)
            if cur is None or version > cur["version"]:
                self.files[filename] = {
                    "data": file_data,
                    "version": version,
                }
                print(
                    f"[{self.server_id}] Replicated {filename} v{version} ({len(file_data)} bytes)"
                )
        
        return fileserver_pb2.ReplicateFileResponse(error="")
    
    # ========================================================================
    # CHANDY-LAMPORT SNAPSHOT
    # ========================================================================
    
    class SnapshotState:
        """Represents the state of an ongoing snapshot"""
        def __init__(self, snapshot_id: str, initiator_id: str) -> None:
            self.snapshot_id: str = snapshot_id
            self.initiator_id: str = initiator_id
            self.local_state: Optional[Dict[str, Dict[str, Any]]] = None
            self.channel_states: Dict[str, List[Any]] = {}
            self.markers_received: Set[str] = set()
            self.recording_channels: Set[str] = set()
            self.is_complete: bool = False
            self.timestamp: float = time.time()
    
    def InitiateSnapshot(
        self,
        request: fileserver_pb2.SnapshotRequest,
        context: grpc.ServicerContext,
    ) -> fileserver_pb2.SnapshotResponse:
        """Initiate a Chandy-Lamport snapshot"""
        snapshot_id = request.snapshot_id
        initiator_id = request.initiator_id
        
        print(f"[{self.server_id}] Initiating snapshot {snapshot_id}")
        
        with self.snapshot_lock:
            if snapshot_id in self.snapshots:
                return fileserver_pb2.SnapshotResponse(success=True, error="")
            
            snapshot_state = self.SnapshotState(snapshot_id, initiator_id)
            
            with self.lock:
                snapshot_state.local_state = {
                    fname: {"data": fdata["data"], "version": fdata["version"]}
                    for fname, fdata in self.files.items()
                }
            
            print(
                f"[{self.server_id}] Recorded local state: {len(snapshot_state.local_state)} files"
            )
            
            for peer in self.peers:
                channel_id = f"{peer}->{self.server_id}"
                snapshot_state.recording_channels.add(channel_id)
                snapshot_state.channel_states[channel_id] = []
            
            self.snapshots[snapshot_id] = snapshot_state
        
        self._send_markers(snapshot_id)
        
        return fileserver_pb2.SnapshotResponse(success=True, error="")
    
    def _send_markers(self, snapshot_id: str) -> None:
        """Send marker messages to all peers"""
        for peer in self.peers:
            try:
                with grpc.insecure_channel(
                    peer,
                    options=[
                        ("grpc.max_send_message_length", -1),
                        ("grpc.max_receive_message_length", -1),
                    ],
                ) as channel:
                    stub = fileserver_pb2_grpc.FileServerStub(channel)
                    marker = fileserver_pb2.MarkerMessage(
                        snapshot_id=snapshot_id,
                        sender_id=self.server_id,
                        channel_id=f"{self.server_id}->{peer}",
                    )
                    response = stub.Marker(marker, timeout=2.0)
                    print(
                        f"[{self.server_id}] Sent marker to {peer} for snapshot {snapshot_id}"
                    )
            except Exception as e:
                print(
                    f"[{self.server_id}] Failed to send marker to {peer}: {e}"
                )
    
    def Marker(
        self, request: fileserver_pb2.MarkerMessage, context: grpc.ServicerContext
    ) -> fileserver_pb2.MarkerResponse:
        """Receive a marker message"""
        snapshot_id = request.snapshot_id
        sender_id = request.sender_id
        
        print(
            f"[{self.server_id}] Received marker from {sender_id} for snapshot {snapshot_id}"
        )
        
        with self.snapshot_lock:
            if snapshot_id not in self.snapshots:
                print(
                    f"[{self.server_id}] First marker for snapshot {snapshot_id}"
                )
                snapshot_state = self.SnapshotState(snapshot_id, sender_id)
                with self.lock:
                    snapshot_state.local_state = {
                        fname: {"data": fdata["data"], "version": fdata["version"]}
                        for fname, fdata in self.files.items()
                    }
                for peer in self.peers:
                    ch_id = f"{peer}->{self.server_id}"
                    snapshot_state.recording_channels.add(ch_id)
                    snapshot_state.channel_states[ch_id] = []
                self.snapshots[snapshot_id] = snapshot_state
                self._send_markers(snapshot_id)
            
            snapshot_state = self.snapshots[snapshot_id]
            
            incoming_channel = f"{sender_id}->{self.server_id}"
            if incoming_channel in snapshot_state.recording_channels:
                snapshot_state.recording_channels.remove(incoming_channel)
                print(
                    f"[{self.server_id}] Stopped recording channel {incoming_channel}"
                )
            
            snapshot_state.markers_received.add(sender_id)
            
            if len(snapshot_state.markers_received) == len(self.peers):
                snapshot_state.is_complete = True
                print(
                    f"[{self.server_id}] Snapshot {snapshot_id} is COMPLETE"
                )
                self._save_snapshot(snapshot_id)
        
        return fileserver_pb2.MarkerResponse(success=True)
    
    def _save_snapshot(self, snapshot_id: str) -> None:
        """Save snapshot to disk"""
        try:
            snapshot_state = self.snapshots[snapshot_id]
            snapshot_data = {
                "snapshot_id": snapshot_id,
                "server_id": self.server_id,
                "timestamp": snapshot_state.timestamp,
                "local_state": snapshot_state.local_state,
                "channel_states": snapshot_state.channel_states,
            }
            
            filename = f"snapshot_{snapshot_id}_{self.server_id}.pkl"
            with open(filename, "wb") as f:
                pickle.dump(snapshot_data, f)
            
            print(f"[{self.server_id}] Saved snapshot to {filename}")
        except Exception as e:
            print(f"[{self.server_id}] Error saving snapshot: {e}")
    
    def GetSnapshot(
        self,
        request: fileserver_pb2.GetSnapshotRequest,
        context: grpc.ServicerContext,
    ) -> fileserver_pb2.GetSnapshotResponse:
        """Get the state of a specific snapshot"""
        snapshot_id = request.snapshot_id
        
        with self.snapshot_lock:
            if snapshot_id not in self.snapshots:
                return fileserver_pb2.GetSnapshotResponse(
                    snapshot_id=snapshot_id, is_complete=False
                )
            
            snapshot_state = self.snapshots[snapshot_id]
            local_state_pb = []
            if snapshot_state.local_state:
                for fname, fdata in snapshot_state.local_state.items():
                    local_state_pb.append(
                        fileserver_pb2.FileState(
                            filename=fname,
                            contents=fdata["data"],
                            version=fdata["version"],
                        )
                    )
            
            channel_states_pb = []
            for ch_id, messages in snapshot_state.channel_states.items():
                channel_state = fileserver_pb2.ChannelState(
                    channel_id=ch_id,
                    messages=[],
                )
                channel_states_pb.append(channel_state)
            
            return fileserver_pb2.GetSnapshotResponse(
                snapshot_id=snapshot_id,
                local_state=local_state_pb,
                channel_states=channel_states_pb,
                is_complete=snapshot_state.is_complete,
            )
    
    def RestoreFromSnapshot(
        self,
        request: fileserver_pb2.RestoreRequest,
        context: grpc.ServicerContext,
    ) -> fileserver_pb2.RestoreResponse:
        """Restore system state from a snapshot"""
        snapshot_id = request.snapshot_id
        
        try:
            filename = f"snapshot_{snapshot_id}_{self.server_id}.pkl"
            with open(filename, "rb") as f:
                snapshot_data = pickle.load(f)
            
            print(f"[{self.server_id}] Restoring from snapshot {snapshot_id}")
            
            with self.lock:
                self.files = {}
                for fname, fdata in snapshot_data["local_state"].items():
                    self.files[fname] = {
                        "data": fdata["data"],
                        "version": fdata["version"],
                    }
            
            print(
                f"[{self.server_id}] Restored {len(self.files)} files from snapshot"
            )
            
            return fileserver_pb2.RestoreResponse(success=True, error="")
        
        except FileNotFoundError:
            error_msg = f"Snapshot {snapshot_id} not found"
            print(f"[{self.server_id}] {error_msg}")
            return fileserver_pb2.RestoreResponse(
                success=False, error=error_msg
            )
        except Exception as e:
            error_msg = f"Error restoring snapshot: {str(e)}"
            print(f"[{self.server_id}] {error_msg}")
            return fileserver_pb2.RestoreResponse(
                success=False, error=error_msg
            )

def run_server(
    server_id: str, port: int, peers: List[str], storage_dir: str
) -> None:
    """Run the FileServer"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    servicer = FileServer(server_id, peers, port, storage_dir)
    fileserver_pb2_grpc.add_FileServerServicer_to_server(servicer, server)
    
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    
    print(f"[{server_id}] FileServer started on port {port}")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        servicer.running = False
        server.stop(0)


if __name__ == "__main__":
    # This __main__ block is from your existing FileServer.py
    parser = argparse.ArgumentParser(description="FileServer with Leader Election")
    parser.add_argument("--id", type=str, required=True, help="Server ID")
    parser.add_argument("--port", type=int, required=True, help="Port")
    parser.add_argument(
        "--peers", type=str, default="", help="Comma-separated peers"
    )
    parser.add_argument(
        "--storage",
        type=str,
        default="./fileserver_storage",
        help="Storage directory",
    )
    args = parser.parse_args()
    
    peer_list: List[str] = [
        addr.strip() for addr in args.peers.split(",") if addr.strip()
    ]
    run_server(args.id, args.port, peer_list, args.storage)