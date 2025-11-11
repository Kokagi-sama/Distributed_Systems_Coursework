import argparse
import hashlib
import random
import threading
import time
import json
import sys
from enum import Enum
from pathlib import Path

# Add parent directory to path so we can import from 'generated'
# This allows running the script from anywhere: python client/client.py
sys.path.insert(0, str(Path(__file__).parent.parent))

import grpc
from sympy import isprime

from generated import (
    coordinator_pb2,
    coordinator_pb2_grpc,
    fileserver_pb2,
    fileserver_pb2_grpc,
)


# =============================== SNAPSHOT ===============================
# Simply client-end snapshot implementation.
# Upon trigger from coordinator received via heartbeat response, client captures the process states and starts channel recording.
# Channel recording takes a simplistic approach in recording before the request is sent and after the response is received rather than monitoring and intercepting rpc calls - since client dont ever receive rpc requests in this design. This is to simulate the idea of capturing in flight calls - you get the idea :)
# Upon receiving snapshot marker via heartbeat response from coordinator, client stops channel recording.
# Client then sends the full snapshot - process states + channel states to coordinator.
# This completes the snapshot process at the client's end.


class TaskStage(Enum):
    """Enums for stages of task used for client's state to facilitate snapshot."""
    IDLE = "IDLE"
    DOWNLOAD = "DOWNLOAD"
    UPLOAD = "UPLOAD"
    FIND_PRIMES = "FIND_PRIMES"
    MERGE = "MERGE"
    COMPLETED = "COMPLETED"

    # Error enums at various stage
    DOWNLOAD_ERROR = "DOWNLOAD_ERROR"
    UPLOAD_ERROR = "UPLOAD_ERROR"
    FIND_PRIMES_ERROR = "FIND_PRIMES_ERROR"
    MERGE_ERROR = "MERGE_ERROR"


class RpcType(Enum):
    """Enums for rpc incoming or outgoing call.

    Simple implementation that there was a request/response in flight for use during channel recording.
    """
    REQUEST_TO_FILESERVER_UPLOAD = "Requesting to upload to Fileserver"
    RESPONSE_FROM_FILESERVER_UPLOAD = "Receiving upload response from Fileserver"
    REQUEST_TO_FILESERVER_DOWNLOAD = "Requesting to download from Fileserver"
    RESPONSE_FROM_FILESERVER_DOWNLOAD = "Receiving download response from Fileserver"

    REQUEST_TO_COORDINATOR_GET_TASK = "Requesting task to Coordinator"
    RESPONSE_FROM_COORDINATOR_GET_TASK = "Receiving task from Coordinator"
    REQUEST_TO_COORDINATOR_TASK_STATUS = "Requesting task update to Coordinator"
    RESPONSE_FROM_COORDINATOR_TASK_STATUS = "Receiving task update response from Coordinator"


class ProcessState:
    """Process states for snapshot."""
    def __init__(self):
        """Initialise the states.

        Progress percentage refers to the progress for that particular stage of the task. It DOES NOT represent the progress of the whole task. E.g., if task_stage="FIND_PRIMES", progress_percentage=60 means the process of "finding primes" is at 60%.
        COMPLETED will always be 100%; IDLE will always be 0%.
        """
        self.task_id: str = ""
        self.task_type: TaskStage = TaskStage.IDLE  # Overload use of TaskStage. Task type can only be IDLE, FIND_PRIMES, or MERGE.
        self.task_stage: TaskStage = TaskStage.IDLE
        self.progress_percentage: int = 0
        self.current_file: str = ""
        self.start_index: int = 0
        self.end_index: int = 0
        self.output_filename: str = ""
# ========================================================================


class Client:
    def __init__(
        self,
        client_id: str,
        coordinator_addresses: list[str],
        fileserver_addresses: list[str],
        cache_dir: str = "./client_cache",
        heartbeat_interval: int = 0.5,
    ):
        self.client_id = client_id

        # Each client gets its own cache directory to avoid conflicts
        self.cache_dir = Path(cache_dir) / client_id
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # If this file exists on startup, it means I crashed!!!!!!!!!!!!!!!!
        self.alive_lock_file = self.cache_dir / "am_i_alive.lock"

        # Take in coordinator and fileserver addresses
        self.coordinator_addresses = coordinator_addresses
        self.fileserver_addresses = fileserver_addresses
        self.snapshot_coordinator_address = coordinator_addresses[0] if coordinator_addresses else ""

        # Clean up orphaned .tmp files from previous crashes or errors
        tmp_files = list(self.cache_dir.glob("*.tmp"))
        if tmp_files:
            print(f"[{client_id}] Cleaning up {len(tmp_files)} orphaned temp files")
            for tmp_file in tmp_files:
                try:
                    tmp_file.unlink()
                except Exception as e:
                    print(f"[{client_id}] Warning: Could not remove {tmp_file.name}: {e}")

        # Cache coordinator connections to avoid reopening channels
        self._coord_channels: dict[str, grpc.Channel] = {}
        self._coord_stubs: dict[str, coordinator_pb2_grpc.CoordinatorStub] = {}

        # Heartbeat threads for each coordinator
        self.heartbeat_threads: dict[str, threading.Thread] = {}
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_running = True
        self.heartbeat_thread = None

        # =============================== SNAPSHOT ===============================
        # Snapshot state
        self.snapshot_lock = threading.Lock()
        self.snapshot_active = False
        self.channel_messages = []  # Simple list of all messages during snapshot
        self.process_state = ProcessState()
        # ========================================================================

        print(f"[{client_id}] Client initialized")
        print(f"[{client_id}] Cache directory: {self.cache_dir}")
        print(f"[{client_id}] File servers: {', '.join(fileserver_addresses)}")


    # =============================== SNAPSHOT ===============================
    def _start_snapshot(self):
        "Capture client states upon trigger."
        with self.snapshot_lock:
            if self.snapshot_active:
                return
            self.snapshot_active = True
            self.channel_messages = []

            state = {
                "client_id": self.client_id,
                "task_id": self.process_state.task_id,
                "task_stage": self.process_state.task_stage.value,
                "progress": self.process_state.progress_percentage,
                "file": self.process_state.current_file,
            }
            print(f"[{self.client_id}] SNAPSHOT: Started - {state}")

    def _record(self, msg_type, **data):
        """Record messages if snapshot active."""
        with self.snapshot_lock:
            if self.snapshot_active:
                self.channel_messages.append({"type": msg_type, **data})

    def _receive_marker(self):
        """Handle marker from coordinator and send snapshot to coordinator."""
        with self.snapshot_lock:
            if not self.snapshot_active:
                return

            snapshot_data = {
                "client_id": self.client_id,
                "task_id": self.process_state.task_id,
                "task_stage": self.process_state.task_stage.value,
                "progress": self.process_state.progress_percentage,
                "file": self.process_state.current_file,
                "task_type": self.process_state.task_type.value,
                "output_filename": self.process_state.output_filename,
                "task_start_index": self.process_state.start_index,
                "task_end_index": self.process_state.end_index,
                "messages": self.channel_messages
            }
            print(f"[{self.client_id}] SNAPSHOT: {json.dumps(snapshot_data, indent=2)}")

            # Send snapshot to coordinator
            self._submit_snapshot_to_coordinator(snapshot_data)

            self.snapshot_active = False

    def _submit_snapshot_to_coordinator(self, snapshot_data: dict):
        """Client submits snapshot to coordinator.

        Coordinator will compile all client snapshots into snapshot.json and upload to fileserver.
        """

        # Serialize entire snapshot data to JSON string
        snapshot_json = json.dumps(snapshot_data, indent=2)

        request = coordinator_pb2.SnapshotSubmissionRequest(
            client_id = self.client_id,
            task_id = snapshot_data.get("task_id", ""),
            task_stage = snapshot_data.get("task_stage", "IDLE"),
            progress_percentage = snapshot_data.get("progress", 0),
            current_file = snapshot_data.get("file", ""),
            snapshot_json = snapshot_json
        )

        try:
            coordinator_stub = self._get_coordinator_stub(self.snapshot_coordinator_address)

            response = coordinator_stub.SubmitSnapshot(request, timeout=60.0, wait_for_ready=True)

            if response.acknowledged:
                print(f"[{self.client_id}] Snapshot sent to coordinator {self.snapshot_coordinator_address}")
            else:
                error_msg = response.error_message if hasattr(response, 'error_message') else "Unknown"
                print(f"[{self.client_id}] Snapshot not acknowledged: {error_msg}")
        except Exception as e:
            print(f"[{self.client_id}] Failed to send snapshot to {self.snapshot_coordinator_address}: {e}")
    # ========================================================================


    # =============================== CRASH RECOVERY===============================
    # Simple crash detection using lock file mechanism
    # If lock file exists on startup = crashed (didn't clean up)
    def _generate_alive_lock(self):
        """Generates an alive lock file to indicate client is running.

        If alive lock file exists on startup, it means client crashed.
        """
        try:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            with open(self.alive_lock_file, "w") as f:
                f.write(f"Client {self.client_id} started at {timestamp}\n")
            print(f"[{self.client_id}] Created alive lock")
        except Exception as e:
            print(f"[{self.client_id}] Warning: Could not create alive lock: {e}")


    def _remove_alive_lock(self):
        """Remove alive lock file on clean shutdown."""
        if self.alive_lock_file.exists():
            try:
                self.alive_lock_file.unlink()
                print(f"[{self.client_id}] Removed alive lock during clean shutdown")
            except Exception as e:
                print(f"[{self.client_id}] Warning: Unable to remove alive lock: {e}")


    def _check_and_recover_from_crash(self) -> bool:
        """Check if alive lock file exists.
        
        - Upon fresh startup, client creates alive lock file
        - When shutdown gracefully, alive lock file is deleted
        - If alive lock file exists on startup --> client crashed (therefore did not clean up alive lock file)
        - If no alive lock file, it means client has a fresh startup
        """
        print(f"[{self.client_id}] ========== CHECKING IF CLIENT CRASHED ==========")
        
        if not self.alive_lock_file.exists():
            print(f"[{self.client_id}] No crash detected")
            # Create alive lock for this session
            self._generate_alive_lock()
            return False
        
        print(f"[{self.client_id}] CRASH DETECTED!")
        print(f"[{self.client_id}] Trying to recover from snapshot...")
        
        recovered = self._recover_from_global_snapshot()
        
        # Recreate alive lock file
        self._generate_alive_lock()
        
        return recovered


    def _recover_from_global_snapshot(self) -> bool:
        """Download snapshot from fileserver and restore state."""
        snapshot_filename = "snapshot.json"
        try:
            # Download snapshot.json
            print(f"[{self.client_id}] Downloading snapshot.json...")
            snapshot_path = self._fetch_file(snapshot_filename)

            with open(snapshot_path, 'r') as f:
                global_snapshot = json.load(f)
        
            # Get snapshot data
            client_snapshot = global_snapshot.get('clients', {}).get(self.client_id)
            
            if not client_snapshot:
                print(f"[{self.client_id}] No snapshot found")
                print(f"[{self.client_id}] Starting fresh")
                return False
            
            print(f"[{self.client_id}] Found snapshot data")
            self._restore_from_snapshot(client_snapshot)
            return True
            
        except FileNotFoundError:
            print(f"[{self.client_id}] No snapshot file found")
            print(f"[{self.client_id}] Starting fresh")
            return False
        except Exception as e:
            print(f"[{self.client_id}] Error recovering from snapshot: {e}")
            print(f"[{self.client_id}] Starting fresh")
            return False


    def _restore_from_snapshot(self, snapshot_data: dict):
        """Restore state from snapshot."""
        print(f"[{self.client_id}] ============ RESTORING FROM SNAPSHOT ============")
        print(f"[{self.client_id}]   Task ID: {snapshot_data.get('task_id', '')}")
        print(f"[{self.client_id}]   Task Type: {snapshot_data.get('task_type', 'IDLE')}")
        print(f"[{self.client_id}]   Task Stage: {snapshot_data.get('task_stage', 'IDLE')}")
        print(f"[{self.client_id}]   Progress: {snapshot_data.get('progress', 0)}%")
        print(f"[{self.client_id}]   File: {snapshot_data.get('file', '')}")
        print(f"[{self.client_id}] =================================================")

        with self.snapshot_lock:
            self.process_state.task_id = snapshot_data.get('task_id', '')

            # Restore to task stage
            try:
                task_stage_str = snapshot_data.get('task_stage', 'IDLE')
                self.process_state.task_stage = TaskStage(task_stage_str)

                task_type_str = snapshot_data.get('task_type', 'IDLE')
                self.process_state.task_type = TaskStage(task_type_str)

                self.process_state.start_index = snapshot_data.get('task_start_index', 0)
                self.process_state.end_index = snapshot_data.get('task_end_index', 0)

                self.process_state.output_filename = snapshot_data.get('output_filename')

            except Exception as e:
                print(f"[{self.client_id}] Warning: Invalid stage ({e}), resetting")
                self.process_state.task_stage = TaskStage.IDLE
                self.process_state.task_type = TaskStage.IDLE

            self.process_state.progress_percentage = snapshot_data.get('progress', 0)
            self.process_state.current_file = snapshot_data.get('file', '')

        print(f"[{self.client_id}] State restored successfully")
    # =====================================================================


    def _get_coordinator_stub(self, address: str) -> coordinator_pb2_grpc.CoordinatorStub:
        """Return existing connection to coordinator or open a new one if needed."""
        stub = self._coord_stubs.get(address)
        if stub is not None:
            return stub
        channel = grpc.insecure_channel(address)
        self._coord_channels[address] = channel
        stub = coordinator_pb2_grpc.CoordinatorStub(channel)
        self._coord_stubs[address] = stub
        return stub

    def _close_coordinator_channels(self) -> None:
        """Close all open coordinator connections."""
        try:
            for channel in self._coord_channels.values():
                try:
                    channel.close()
                except Exception:
                    pass
        finally:
            self._coord_channels.clear()
            self._coord_stubs.clear()

    def _next_coordinator(self, current: str) -> str:
        addresses = self.coordinator_addresses
        if not addresses:
            raise RuntimeError("No coordinator addresses configured")
        try:
            i = addresses.index(current)
        except ValueError:
            return addresses[0]
        return addresses[(i + 1) % len(addresses)]

    def _compute_checksum(self, filepath: Path, chunk_size: int = 65536) -> str:
        """Compute SHA-256 checksum of file."""
        sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                sha256.update(chunk)
        return sha256.hexdigest()

    def _stream_upload(self, filepath: Path, remote_filename: str, chunk_size: int = 65536):
        """Read local file and generate chunks for upload to file server."""
        file_size = filepath.stat().st_size

        # Compute checksum
        checksum = self._compute_checksum(filepath)

        # Send metadata first
        metadata = fileserver_pb2.FileMetadata(
            filename=remote_filename,
            file_size=file_size,
            checksum=checksum,
        )
        yield fileserver_pb2.FileChunk(metadata=metadata)

        # Then send file data in chunks
        with open(filepath, "rb") as f:
            while True:
                chunk_data = f.read(chunk_size)
                if not chunk_data:
                    break
                yield fileserver_pb2.FileChunk(chunk_data=chunk_data)

    def _stream_fetch(self, chunk_stream, output_path: Path) -> str:
        """Receive chunks from file server and write atomically."""
        server_checksum = ""
        tmp_path = Path(str(output_path) + ".tmp")

        try:
            with open(tmp_path, "wb") as f:
                for chunk in chunk_stream:
                    if chunk.HasField("metadata"):
                        server_checksum = chunk.metadata.checksum
                    elif chunk.HasField("chunk_data"):
                        f.write(chunk.chunk_data)

            tmp_path.replace(output_path)
            return server_checksum

        except Exception as e:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass
            raise e

    def _fetch_file(self, filename: str) -> Path:
        """Fetch file from file server with random selection and failover."""
        # Check if file exists on disk
        local_path = self.cache_dir / Path(filename).name
        if local_path.exists():
            print(f"[{self.client_id}] Using local {filename}")
            return local_path

        # Try file servers in random order for load balancing
        servers = self.fileserver_addresses.copy()
        random.shuffle(servers)

        last_error = None
        for fileserver_address in servers:
            try:
                print(f"[{self.client_id}] Fetching {filename} from {fileserver_address}")
                
                # ============ SNAPSHOT ============
                # Record before sending request to fileserver
                self._record(RpcType.REQUEST_TO_FILESERVER_DOWNLOAD.value, file=filename, server=fileserver_address)
                
                # Update task stage and start with 0 progress
                with self.snapshot_lock:
                    self.process_state.task_stage = TaskStage.DOWNLOAD
                    self.process_state.progress_percentage = 0
                    self.process_state.current_file = filename
                # ==================================
                
                fileserver_channel = grpc.insecure_channel(fileserver_address)
                fileserver_stub = fileserver_pb2_grpc.FileServerStub(fileserver_channel)

                # Request file via RPC
                request = fileserver_pb2.FetchRequest(filename=filename)
                chunk_stream = fileserver_stub.FetchFile(request)
                
                # ============ SNAPSHOT ============
                # Record after getting response from fileserver
                self._record(RpcType.RESPONSE_FROM_FILESERVER_DOWNLOAD.value, file=filename, server=fileserver_address)
                # ==================================

                # Save to local cache
                cached_path = self.cache_dir / Path(filename).name
                server_checksum = self._stream_fetch(chunk_stream, cached_path)

                # Verify checksum if provided by server
                if server_checksum:
                    computed_checksum = self._compute_checksum(cached_path)
                    if computed_checksum != server_checksum:
                        # Delete corrupted file
                        cached_path.unlink()
                        raise RuntimeError(f"Checksum verification failed for {filename}: expected {server_checksum}, got {computed_checksum}")
                    print(f"[{self.client_id}] Successfully verified checksum for {filename}")

                print(f"[{self.client_id}] Successfully fetched {filename} from {fileserver_address}")
                print(f"[{self.client_id}] Cached {filename} locally")
                
                # ============ SNAPSHOT ============
                # Update progress to 100
                with self.snapshot_lock:
                    self.process_state.progress_percentage = 100
                # ==================================
                
                return cached_path

            except Exception as e:
                print(f"[{self.client_id}] Failed to fetch from {fileserver_address}: {e}")
                last_error = e
                
                # ============ SNAPSHOT ============
                with self.snapshot_lock:
                    self.process_state.task_stage = TaskStage.DOWNLOAD_ERROR
                    self.process_state.progress_percentage = 0
                # ==================================
                
                continue
            
        raise RuntimeError(f"Failed to fetch {filename} from all file servers: {last_error}")

    def _build_server_rotation_list(self, preferred_address, all_addresses):
        """Build server list starting with preferred address, others in random order."""
        others = [addr for addr in all_addresses if addr != preferred_address]
        random.shuffle(others)
        return [preferred_address] + others if others else [preferred_address]

    def _rotate_to_next_server(self, servers, current_idx, current_addr):
        """Move to next server in list, reset backoff if actually changed."""
        next_idx = (current_idx + 1) % len(servers)
        next_addr = servers[next_idx]

        if next_addr != current_addr:
            print(f"[{self.client_id}] Switching to next fileserver: {next_addr}")
            return next_idx, next_addr, 1.0

        return next_idx, next_addr, None

    def _add_jitter(self, base_seconds, max_jitter=0.5):
        """Add small random delay so retries don't all happen at once."""
        return base_seconds + random.uniform(0, max_jitter)

    def _apply_backoff(self, current_backoff, max_backoff=30.0):
        """Wait before retry using exponential backoff, doubles each time up to max."""
        sleep_seconds = self._add_jitter(current_backoff)
        print(f"[{self.client_id}] Retry in {sleep_seconds:.1f}s")
        time.sleep(sleep_seconds)
        return min(max_backoff, current_backoff * 2)

    def _handle_upload_redirect(self, redirect_address, current_addr):
        """Follow redirect to new primary server if one was provided."""
        if redirect_address and redirect_address != current_addr:
            print(f"[{self.client_id}] Redirecting to primary: {redirect_address}")
            servers = self._build_server_rotation_list(redirect_address, self.fileserver_addresses)
            return servers, 0, servers[0], 1.0
        return None, None, None, None

    def _upload_file(self, local_filepath: Path, remote_filename: str, fileserver_address: str) -> bool:
        """Upload file to server, retrying until success.

        Tries different servers on failure and follows redirects.
        Wait time doubles after each failure up to 30s, resets when switching servers.
        """
        servers = self._build_server_rotation_list(fileserver_address, self.fileserver_addresses)
        idx = 0
        current_fileserver_address = servers[idx]
        backoff = 1.0
        max_backoff = 30.0

        while True:
            print(f"[{self.client_id}] Uploading {remote_filename} to {current_fileserver_address}")
            fileserver_channel = None

            try:
                fileserver_channel = grpc.insecure_channel(current_fileserver_address)
                fileserver_stub = fileserver_pb2_grpc.FileServerStub(fileserver_channel)
                
                # ============ SNAPSHOT ============
                # Record before sending request to fileserver
                self._record(RpcType.REQUEST_TO_FILESERVER_UPLOAD.value, file=remote_filename, server=current_fileserver_address)
                
                # Update task stage and start with 0 progress
                with self.snapshot_lock:
                    self.process_state.task_stage = TaskStage.UPLOAD
                    self.process_state.progress_percentage = 0
                # ==================================

                # Create fresh iterator for each attempt
                chunk_iterator = self._stream_upload(local_filepath, remote_filename)

                response = fileserver_stub.UploadFile(chunk_iterator, timeout=60.0, wait_for_ready=True)
                
                # ============ SNAPSHOT ============
                # Record after getting response from fileserver
                self._record(RpcType.RESPONSE_FROM_FILESERVER_UPLOAD.value, file=remote_filename, server=current_fileserver_address)
                # ==================================

                if response.success:
                    print(f"[{self.client_id}] Successfully uploaded {remote_filename} to {current_fileserver_address}")
                    
                    # ============ SNAPSHOT ============
                    with self.snapshot_lock:
                        self.process_state.progress_percentage = 100
                    # ==================================
                    return True

                print(f"[{self.client_id}] Upload to {current_fileserver_address} failed: {response.error_message}")
                
                # ============ SNAPSHOT ============
                with self.snapshot_lock:
                    self.process_state.task_stage = TaskStage.UPLOAD_ERROR
                    self.process_state.progress_percentage = 0
                # ==================================

                # Server may redirect us to the actual primary
                redirect_address = response.new_primary_fileserver
                new_servers, new_idx, new_addr, new_backoff = (self._handle_upload_redirect(redirect_address, current_fileserver_address))

                if new_servers is not None:
                    servers = new_servers
                    idx = new_idx
                    current_fileserver_address = new_addr
                    backoff = new_backoff
                else:
                    backoff = self._apply_backoff(backoff, max_backoff)
                    idx, current_fileserver_address, reset_backoff = (self._rotate_to_next_server(servers, idx, current_fileserver_address))
                    if reset_backoff is not None:
                        backoff = reset_backoff

            except grpc.RpcError as e:
                code = e.code().name if hasattr(e, "code") else "UNKNOWN"
                print(f"[{self.client_id}] RPC error to {current_fileserver_address}: {code}, {e}")
                backoff = self._apply_backoff(backoff, max_backoff)
                idx, current_fileserver_address, reset_backoff = (self._rotate_to_next_server(servers, idx, current_fileserver_address))
                if reset_backoff is not None:
                    backoff = reset_backoff
                # ============ SNAPSHOT ============
                with self.snapshot_lock:
                    self.process_state.task_stage = TaskStage.UPLOAD_ERROR
                    self.process_state.progress_percentage = 0
                # ==================================

            except Exception as e:
                print(f"[{self.client_id}] Upload error to {current_fileserver_address}: {e}")
                backoff = self._apply_backoff(backoff, max_backoff)
                idx, current_fileserver_address, reset_backoff = (self._rotate_to_next_server(servers, idx, current_fileserver_address))
                if reset_backoff is not None:
                    backoff = reset_backoff
                # ============ SNAPSHOT ============
                with self.snapshot_lock:
                    self.process_state.task_stage = TaskStage.UPLOAD_ERROR
                    self.process_state.progress_percentage = 0
                # ==================================

            finally:
                try:
                    if fileserver_channel is not None:
                        fileserver_channel.close()
                except Exception as e:
                    print(f"[{self.client_id}] Failed to close connection to {current_fileserver_address}: {e}")

    def process_file(
        self,
        filename: str,
        primary_fileserver_addr: str,
        start_index: int,
        end_index: int,
    ) -> tuple[bool, str]:
        """Process specific line range of file to find primes, deduplicate, and upload result."""
        try:
            # Get input file
            filepath = self._fetch_file(filename)
        except Exception as e:
            print(f"[{self.client_id}] Error: {e}")
            
            # ============ SNAPSHOT ============
            with self.snapshot_lock:
                self.process_state.task_stage = TaskStage.FIND_PRIMES_ERROR
                self.process_state.current_file = filename
                self.process_state.progress_percentage = 0
            # ==================================
            
            return False, ""

        # ============ SNAPSHOT ============
        with self.snapshot_lock:
            self.process_state.task_stage = TaskStage.FIND_PRIMES
            self.process_state.current_file = filename
            self.process_state.progress_percentage = 0
        # ==================================

        print(f"[{self.client_id}] Started processing {filename} from line {start_index} to {end_index - 1}")

        # Find and deduplicate primes
        primes = set()

        with open(filepath, "r") as f:
            for line_num, line in enumerate(f):
                # Skip lines before start_index
                if line_num < start_index:
                    continue

                # Stop when reaching end_index
                if line_num >= end_index:
                    break

                # ============ SNAPSHOT ============
                # Update progress every 100 lines
                if (line_num - start_index) % 100 == 0 and end_index - start_index > 0:
                        progress = int(((line_num - start_index) / (end_index - start_index)) * 100)
                        with self.snapshot_lock:
                            self.process_state.progress_percentage = min(progress, 99)
                # ==================================

                line = line.strip()
                if not line:
                    continue

                try:
                    num = int(line)

                    if isprime(num):
                        primes.add(num)
                except ValueError:
                    print(f"[{self.client_id}] Warning: Invalid number on line {line_num + 1}: {line}")
                    continue

        print(f"[{self.client_id}] Finished processing {filename}: found {len(primes):,} unique primes from line {start_index} to {end_index - 1}")

        # Write sorted primes atomically
        result_filename = (f"{Path(filename).stem}_range_{start_index}_{end_index}_primes.txt")
        result_path = self.cache_dir / result_filename
        tmp_path = Path(str(result_path) + ".tmp")

        try:
            with open(tmp_path, "w") as f:
                for prime in sorted(primes):
                    f.write(f"{prime}\n")

            tmp_path.replace(result_path)
            print(f"[{self.client_id}] Wrote deduplicated primes to {result_filename}")

            # Upload result to primary file server
            success = self._upload_file(result_path, result_filename, primary_fileserver_addr)
            
            # ============ SNAPSHOT ============
            with self.snapshot_lock:
                self.process_state.task_stage = TaskStage.FIND_PRIMES if success else TaskStage.FIND_PRIMES_ERROR
                self.process_state.progress_percentage = 100 if success else 0
                self.process_state.output_filename = result_filename if success else ""
            # ==================================
            
            return success, result_filename

        except Exception as e:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass
            print(f"[{self.client_id}] Error writing result: {e}")
            
            # ============ SNAPSHOT ============
            with self.snapshot_lock:
                self.process_state.task_stage = TaskStage.FIND_PRIMES_ERROR
                self.process_state.current_file = filename
                self.process_state.progress_percentage = 0
            # ==================================
            
            return False, ""

    def merge_files(
        self, file1: str, file2: str, task_id: str, primary_fileserver_addr: str, last_task: bool
    ) -> tuple[bool, str]:
        """Download two deduplicated files, merge them, and upload result to file server."""
        print(f"[{self.client_id}] Started merge task: {task_id}")
        print(f"[{self.client_id}]   File 1: {file1}")
        print(f"[{self.client_id}]   File 2: {file2}")
        
        # ============ SNAPSHOT ============
        with self.snapshot_lock:
            self.process_state.task_stage = TaskStage.MERGE
            self.process_state.current_file = f"{file1},{file2}"
        # ==================================

        merged_filename = "primes.txt" if last_task else f"merged_{task_id}.txt"
        merged_path = self.cache_dir / merged_filename
        tmp_path = Path(f"{str(merged_path)}.tmp")

        try:
            # Download both files from file server
            path1 = self._fetch_file(file1)
            path2 = self._fetch_file(file2)

            # Merge and deduplicate primes
            primes = set()

            for filepath in [path1, path2]:
                with open(filepath, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            primes.add(int(line))

            print(f"[{self.client_id}] Finished merging: {len(primes):,} unique primes total")

            # Write sorted merged result atomically
            with open(tmp_path, "w") as f:
                for prime in sorted(primes):
                    f.write(f"{prime}\n")

            tmp_path.replace(merged_path)
            print(f"[{self.client_id}] Wrote merged result to {merged_filename}")

            # Upload result to primary file server
            success = self._upload_file(merged_path, merged_filename, primary_fileserver_addr)
            
            # ============ SNAPSHOT ============
            with self.snapshot_lock:
                self.process_state.task_stage = TaskStage.MERGE if success else TaskStage.MERGE_ERROR
                self.process_state.progress_percentage = 100 if success else 0
                self.process_state.output_filename = merged_filename if success else ""
            # ==================================
            
            return success, merged_filename

        except Exception as e:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass
            print(f"[{self.client_id}] Merge error: {e}")

            # ============ SNAPSHOT ============
            with self.snapshot_lock:
                self.process_state.task_stage = TaskStage.MERGE_ERROR
                self.process_state.current_file = f"{file1},{file2}"
                self.process_state.progress_percentage = 0
            # ==================================
            
            return False, ""

    def _heartbeat_loop(self, address):
        """Send periodic heartbeat messages to coordinator."""
        while self.heartbeat_running:
            try:
                coordinator_stub = self._get_coordinator_stub(address)
                request = coordinator_pb2.HeartbeatRequest(client_id=self.client_id)
                response = coordinator_stub.SendHeartbeat(request)

                if not response.acknowledged:
                    print(f"[{self.client_id}] Warning: Heartbeat not acknowledged by coordinator {address}")
                    
                # ============ SNAPSHOT ============
                # Handle snapshot trigger
                if response.trigger_snapshot:
                    self._start_snapshot()
                    with self.snapshot_lock:
                        self.snapshot_coordinator_address = address

                # Handle snapshot marker
                if response.snapshot_marker:
                    self._receive_marker()
                # ==================================

            except grpc.RpcError as e:
                print(f"[{self.client_id}] Heartbeat error to {address}: {e}")

            time.sleep(self.heartbeat_interval)

    def _start_heartbeat(self, address):
        """Start background thread to send heartbeats to coordinator."""
        if (address in self.heartbeat_threads and self.heartbeat_threads[address].is_alive()):
            return

        thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(address,),
            daemon=True,
            name=f"{self.client_id}-heartbeat",
        )
        thread.start()
        self.heartbeat_threads[address] = thread
        print(f"[{self.client_id}] Started heartbeat to {address} with {self.heartbeat_interval}s interval")

    def _stop_heartbeat(self):
        """Stop all heartbeat threads and wait for them to finish."""
        self.heartbeat_running = False
        for thread in self.heartbeat_threads.values():
            try:
                if thread.is_alive():
                    thread.join(timeout=self.heartbeat_interval + 1)
            except Exception:
                pass
        self.heartbeat_threads.clear()

    def run(self):
        """Main loop to request and process tasks from coordinator."""
        print(f"[{self.client_id}] Started")
        
        # Send heartbeat to all coordinators
        for address in self.coordinator_addresses:
            self._start_heartbeat(address)

        # Find first responsive coordinator or default to first
        current_coordinator = self.coordinator_addresses[0]
        for address in self.coordinator_addresses:
            try:
                stub = self._get_coordinator_stub(address)
                # Quick connectivity check with short timeout
                stub.SendHeartbeat(coordinator_pb2.HeartbeatRequest(client_id=self.client_id), timeout=self.heartbeat_interval)
                current_coordinator = address
                print(f"[{self.client_id}] Connected to coordinator {address}")
                break
            except Exception:
                # Try next coordinator
                continue

        # Backoff settings for waiting and retries
        base_wait_time = 5
        max_wait_time = 60
        current_wait_time = base_wait_time

        # ==================== CRASH DETECTION AND RECOVERY ====================
        # Check for alive lock (this indicates previous crash)
        # If crashed, download snapshot.json from fileserver and restore
        # Processes restored from major checkpoints
        recovered = self._check_and_recover_from_crash()
        
        if recovered:
            print(f"[{self.client_id}] ================== START CRASH RECOVERY ==================")
            print(f"[{self.client_id}] Recovered states from previous crash using global snapshot")
            print(f"[{self.client_id}] ==========================================================")

            # Read process state with lock
            with self.snapshot_lock:
                task_stage = self.process_state.task_stage
                task_type = self.process_state.task_type
                task_id = self.process_state.task_id
                current_file = self.process_state.current_file
                start_index = self.process_state.start_index
                end_index = self.process_state.end_index
                output_filename_state = self.process_state.output_filename

            if task_stage != TaskStage.IDLE:
                try:
                    start_time = time.time()
                    # Initialise to False to ensure always defined
                    success = False
                    output_filename = ""

                    if task_stage in (TaskStage.DOWNLOAD, TaskStage.DOWNLOAD_ERROR): # In DOWNLOAD or DOWNLOAD ERROR, task type could be FIND_PRIMES or MERGE
                        if task_type in (TaskStage.FIND_PRIMES, TaskStage.FIND_PRIMES_ERROR) and current_file:
                            print(f"[{self.client_id}] Restarting FIND_PRIMES task from beginning")
                            success, output_filename = self.process_file(
                            current_file,
                            self.fileserver_addresses[0],
                            start_index,
                            end_index,
                        )
                        elif task_type in (TaskStage.MERGE, TaskStage.MERGE_ERROR) and current_file:
                            print(f"[{self.client_id}] Restarting MERGE task from beginning")

                            try:
                                files = current_file.split(',')
                                if len(files) != 2:
                                    raise ValueError(f"Expected 2 files, got {len(files)}")
                                file1, file2 = files[0], files[1]

                                success, output_filename = self.merge_files(
                                    file1,
                                    file2,
                                    task_id,
                                    self.fileserver_addresses[0],
                                    False,
                                )
                            except (IndexError, ValueError) as e:
                                print(f"[{self.client_id}] Error: Invalid merge file format: {e}")
                                print(f"[{self.client_id}] Cannot recover merge task, skipping")
                                # Reset to IDLE and continue
                                with self.snapshot_lock:
                                    self.process_state.task_stage = TaskStage.IDLE
                                    self.process_state.task_type = TaskStage.IDLE
                                success = False
                                output_filename = ""
                            
                        elapsed = time.time() - start_time
                        print(f"[{self.client_id}] Task completed in {elapsed:.2f}s")

                        if success:
                            print(f"[{self.client_id}] Updating task status to completed")
                            
                            # Update process state to completed
                            with self.snapshot_lock:
                                self.process_state.task_stage = TaskStage.COMPLETED
                                self.process_state.progress_percentage = 100
                                
                            status_request = coordinator_pb2.TaskStatusRequest(
                                client_id=self.client_id,
                                task_id=task_id,
                                client_task_status=coordinator_pb2.COMPLETED,
                                output_filename=output_filename,
                            )
                            coordinator_stub = self._get_coordinator_stub(current_coordinator)
                            response = coordinator_stub.UpdateTaskStatus(status_request)
                            if response.acknowledged:
                                print(f"[{self.client_id}] Task status updated by coordinator")
                                # Reset to IDLE after successful completion
                                with self.snapshot_lock:
                                    self.process_state.task_stage = TaskStage.IDLE
                                    self.process_state.task_type = TaskStage.IDLE
                                    self.process_state.progress_percentage = 0
                                    self.process_state.current_file = ""
                                    self.process_state.task_id = ""
                                    self.process_state.output_filename = ""
                            else:
                                print(f"[{self.client_id}] Warning: Task status not acknowledged by coordinator - recovery task might have been ignored due to timeout/re-assignment")
                                
                    elif task_stage in (TaskStage.UPLOAD, TaskStage.UPLOAD_ERROR):
                        if output_filename_state:
                            path = self.cache_dir / output_filename_state

                            # Check if file exists before attempting re-upload
                            if not path.exists():
                                print(f"[{self.client_id}] Error: Output file {output_filename_state} not found in cache")
                                print(f"[{self.client_id}] Cannot re-upload, task may need to be reprocessed")
                                success = False
                            else:
                                success = self._upload_file(path, output_filename_state, self.fileserver_addresses[0])
                            
                            if success:
                                print(f"[{self.client_id}] Re-upload successful, updating task status to completed")

                                # Update process state to completed
                                with self.snapshot_lock:
                                    self.process_state.task_stage = TaskStage.COMPLETED
                                    self.process_state.progress_percentage = 100

                                status_request = coordinator_pb2.TaskStatusRequest(
                                    client_id=self.client_id,
                                    task_id=task_id,
                                    client_task_status=coordinator_pb2.COMPLETED,
                                    output_filename=output_filename_state,
                                )
                                coordinator_stub = self._get_coordinator_stub(current_coordinator)
                                response = coordinator_stub.UpdateTaskStatus(status_request)
                                if response.acknowledged:
                                    print(f"[{self.client_id}] Task status updated by coordinator")
                                    # Reset to IDLE after successful completion
                                    with self.snapshot_lock:
                                        self.process_state.task_stage = TaskStage.IDLE
                                        self.process_state.task_type = TaskStage.IDLE
                                        self.process_state.progress_percentage = 0
                                        self.process_state.current_file = ""
                                        self.process_state.task_id = ""
                                        self.process_state.output_filename = ""
                                else:
                                    print(f"[{self.client_id}] Warning: Task status not acknowledged by coordinator - recovery task might have been ignored due to timeout/re-assignment")
                except Exception as e:
                    print(f"[{self.client_id}] Error: Failed to resume process from snapshot state: {e}")
                    
            print(f"[{self.client_id}] ==========================================================")
            print(f"[{self.client_id}] ================= CRASH RECOVERY COMPLETE ================")
            print(f"[{self.client_id}] ==========================================================")
        # ======================================================================

        try:
            while True:
                try:
                    print(f"[{self.client_id}] Requesting task from {current_coordinator}")

                    coordinator_stub = self._get_coordinator_stub(current_coordinator)
                    request = coordinator_pb2.TaskRequest(client_id=self.client_id)

                    # ============ SNAPSHOT ============
                    self._record(RpcType.REQUEST_TO_COORDINATOR_GET_TASK.value, client_id=self.client_id)
                    # ==================================

                    task = coordinator_stub.RequestTask(request, timeout=30.0, wait_for_ready=True)

                    if not task.has_task:
                        print(f"[{self.client_id}] Stopped: no tasks available")
                        break

                    if task.client_task_status == coordinator_pb2.WAIT:
                        print(f"[{self.client_id}] Waiting for task dependencies")
                        current_wait_time = self._apply_backoff(current_wait_time, max_wait_time)
                        continue

                    # Got a task, reset backoff
                    current_wait_time = base_wait_time

                    print(f"[{self.client_id}] Received task from coordinator")
                    print(f"[{self.client_id}] Task type: {task.task_type}")

                    # ============ SNAPSHOT ============
                    with self.snapshot_lock:
                        self.process_state.task_id = task.task_id
                    self._record(RpcType.RESPONSE_FROM_COORDINATOR_GET_TASK.value, task_id=task.task_id, task_type=task.task_type)
                    # ==================================

                    start_time = time.time()
                    success = False
                    task_id = ""
                    output_filename = ""

                    if task.task_type == coordinator_pb2.FIND_PRIMES:
                        print(f"[{self.client_id}] Task details:")
                        print(f"[{self.client_id}]   Task ID: {task.task_id}")
                        print(f"[{self.client_id}]   File: {task.filename}")
                        print(f"[{self.client_id}]   Line range: {task.start_index} to {task.end_index - 1}")
                        print(f"[{self.client_id}]   Primary server: {self.fileserver_addresses[0]}")

                        # Set task_type for snapshot
                        with self.snapshot_lock:
                            self.process_state.task_type = TaskStage.FIND_PRIMES
                            self.process_state.start_index = task.start_index
                            self.process_state.end_index = task.end_index

                        success, output_filename = self.process_file(
                            task.filename,
                            self.fileserver_addresses[0],
                            task.start_index,
                            task.end_index,
                        )
                        task_id = task.task_id

                    elif task.task_type == coordinator_pb2.MERGE:
                        print(f"[{self.client_id}] Task details:")
                        print(f"[{self.client_id}]   Task ID: {task.task_id}")
                        print(f"[{self.client_id}]   File 1: {task.file1}")
                        print(f"[{self.client_id}]   File 2: {task.file2}")
                        print(f"[{self.client_id}]   Primary server: {self.fileserver_addresses[0]}")
                        print(f"[{self.client_id}]   Last Task: {task.last_task}")

                        # Set task_type for snapshot
                        with self.snapshot_lock:
                            self.process_state.task_type = TaskStage.MERGE

                        success, output_filename = self.merge_files(
                            task.file1,
                            task.file2,
                            task.task_id,
                            self.fileserver_addresses[0],
                            task.last_task,
                        )
                        task_id = task.task_id

                    elapsed = time.time() - start_time
                    print(f"[{self.client_id}] Task completed in {elapsed:.2f}s")

                    # ============ SNAPSHOT ============
                    with self.snapshot_lock:
                        self.process_state.task_stage = TaskStage.COMPLETED
                        self.process_state.progress_percentage = 100
                        self.process_state.current_file = ""
                        self.process_state.output_filename = output_filename
                    # ==================================

                    # Update task status to completed
                    if success:
                        print(f"[{self.client_id}] Updating task status to completed")

                        # ============ SNAPSHOT ============
                        self._record(RpcType.REQUEST_TO_COORDINATOR_TASK_STATUS.value, task_id=task_id, output=output_filename)
                        # ==================================

                        status_request = coordinator_pb2.TaskStatusRequest(
                            client_id=self.client_id,
                            task_id=task_id,
                            client_task_status=coordinator_pb2.COMPLETED,
                            output_filename=output_filename,
                        )
                        response = coordinator_stub.UpdateTaskStatus(status_request)

                        # ============ SNAPSHOT ============
                        self._record(RpcType.RESPONSE_FROM_COORDINATOR_TASK_STATUS.value, task_id=task_id, output=output_filename)
                        # ==================================

                        if response.acknowledged:
                            print(f"[{self.client_id}] Task status updated by coordinator")
                            # Reset to IDLE after successful acknowledgment
                            with self.snapshot_lock:
                                self.process_state.task_stage = TaskStage.IDLE
                                self.process_state.task_type = TaskStage.IDLE
                                self.process_state.task_id = ""
                                self.process_state.output_filename = ""
                        else:
                            print(f"[{self.client_id}] Warning: Task status not acknowledged by coordinator")
                    else:
                        print(f"[{self.client_id}] Task failed")
                        print(f"[{self.client_id}] Not acknowledging failed task")

                        # Reset state after failed task
                        with self.snapshot_lock:
                            self.process_state.task_stage = TaskStage.IDLE
                            self.process_state.task_type = TaskStage.IDLE
                            self.process_state.task_id = ""
                            self.process_state.current_file = ""
                            self.process_state.output_filename = ""
                            self.process_state.progress_percentage = 0

                except KeyboardInterrupt:
                    print(f"[{self.client_id}] Stopping")
                    break
                except grpc.RpcError as e:
                    code = e.code().name if hasattr(e, "code") else "UNKNOWN"
                    print(f"[{self.client_id}] RPC error: {code}, {e}")

                    current_coordinator = self._next_coordinator(current_coordinator)
                    print(f"[{self.client_id}] Trying another coordinator")
                    current_wait_time = base_wait_time
                    current_wait_time = self._apply_backoff(
                        current_wait_time, max_wait_time
                    )

                    # ============ SNAPSHOT ============
                    # Update task stage
                    with self.snapshot_lock:
                        self.process_state.task_stage = TaskStage.IDLE
                        self.process_state.progress_percentage = 0
                    # ==================================

                    continue
                except Exception as e:
                    print(f"[{self.client_id}] Error: {e}")
                    time.sleep(5)

        finally:
            # Always cleanup, even if an exception occurs
            # Stop heartbeat thread
            self._stop_heartbeat()
            self._close_coordinator_channels()

            # Remove alive lock file
            self._remove_alive_lock()

            print(f"[{self.client_id}] Stopped")


def main():
    """Entry point for client."""
    parser = argparse.ArgumentParser(description="Distributed System Find Prime - Client")
    parser.add_argument("client_id", help="Unique client identifier")
    parser.add_argument("--coordinators", nargs="+", required=True, help="List of coordinator addresses")
    parser.add_argument("--fileservers", nargs="+", required=True, help="List of file server addresses")

    args = parser.parse_args()

    client = Client(args.client_id, args.coordinators, args.fileservers)
    client.run()


if __name__ == "__main__":
    main()
