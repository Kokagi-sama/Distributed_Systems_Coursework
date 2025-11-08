import argparse
import grpc
import hashlib
import random
import threading
import time
from pathlib import Path
from sympy import isprime

from generated import coordinator_pb2
from generated import coordinator_pb2_grpc
from generated import fileserver_pb2
from generated import fileserver_pb2_grpc


class Client:
    def __init__(
        self,
        client_id: str,
        coordinator_address: str,
        coordinator_addresses: list[str],
        fileserver_addresses: list[str],
        cache_dir: str = "/tmp/client_cache",
        heartbeat_interval: int = 5,
    ):
        self.client_id = client_id
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.coordinator_addresses = coordinator_addresses
        self.fileserver_addresses = fileserver_addresses

        # # Connect to coordinator via gRPC
        # coordinator_channel = grpc.insecure_channel(coordinator_address)
        # self.coordinator = coordinator_pb2_grpc.CoordinatorStub(coordinator_channel)

        # Reusable coordinator channels/stubs cache
        self._coord_channels: dict[str, grpc.Channel] = {}
        self._coord_stubs: dict[str, coordinator_pb2_grpc.CoordinatorStub] = {}

        # File cache
        self.cached_files = {}

        # Heartbeat control
        self.heartbeat_threads: dict[str, threading.Thread] = {}
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_running = True
        self.heartbeat_thread = None

        print(f"[{client_id}] Client initialized")
        print(f"[{client_id}] Cache directory: {self.cache_dir}")
        print(f"[{client_id}] File servers: {', '.join(fileserver_addresses)}")


    def _create_coordinator_connection_stub(self, address: str) -> coordinator_pb2_grpc.CoordinatorStub:
        """Create Coordinator Connection Stub"""
        coordinator_channel = grpc.insecure_channel(address)
        return coordinator_pb2_grpc.CoordinatorStub(coordinator_channel)


    def _get_coordinator_stub(self, address: str) -> coordinator_pb2_grpc.CoordinatorStub:
        """Get or create and cache a Coordinator stub for the given address."""
        stub = self._coord_stubs.get(address)
        if stub is not None:
            return stub
        channel = grpc.insecure_channel(address)
        self._coord_channels[address] = channel
        stub = coordinator_pb2_grpc.CoordinatorStub(channel)
        self._coord_stubs[address] = stub
        return stub


    def _close_coordinator_channels(self) -> None:
        """Close all cached coordinator channels and clear caches."""
        try:
            for ch in list(self._coord_channels.values()):
                try:
                    ch.close()
                except Exception:
                    pass
        finally:
            self._coord_channels.clear()
            self._coord_stubs.clear()


    def _next_coordinator(self, current: str) -> str:
        address = self.coordinator_addresses
        if not address:
            raise RuntimeError("No coordinator addresses configured")
        try:
            i = address.index(current)
        except ValueError:
            return address[0]
        return address[(i + 1) % len(address)]


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
        """Receive chunks from file server and write to local file."""
        server_checksum = ""

        with open(output_path, "wb") as f:
            for chunk in chunk_stream:
                if chunk.HasField("metadata"):
                    # Extract checksum from metadata
                    server_checksum = chunk.metadata.checksum
                elif chunk.HasField("chunk_data"):
                    f.write(chunk.chunk_data)

        return server_checksum


    def _fetch_file(self, filename: str) -> Path:
        """Fetch file from file server with random selection and failover."""
        # Return cached version if available
        if filename in self.cached_files:
            print(f"[{self.client_id}] Using cached {filename}")
            return self.cached_files[filename]

        # Try file servers in random order for load balancing
        servers = self.fileserver_addresses.copy()
        random.shuffle(servers)

        last_error = None
        for fileserver_address in servers:
            try:
                print(f"[{self.client_id}] Fetching {filename} from {fileserver_address}")
                fileserver_channel = grpc.insecure_channel(fileserver_address)
                fileserver_stub = fileserver_pb2_grpc.FileServerStub(fileserver_channel)

                # Request file via RPC
                request = fileserver_pb2.FetchRequest(filename=filename)
                chunk_stream = fileserver_stub.FetchFile(request)

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

                self.cached_files[filename] = cached_path
                print(f"[{self.client_id}] Cached {filename} locally")
                return cached_path

            except Exception as e:
                print(f"[{self.client_id}] Failed to fetch from {fileserver_address}: {e}")
                last_error = e
                continue

        raise RuntimeError(f"Failed to fetch {filename} from all file servers: {last_error}")


    def _upload_file(self, local_filepath: Path, remote_filename: str, fileserver_address: str) -> bool:
        """Upload file to file server.

        On failure, rotate through 'self.fileserver_addresses' in order to try a different file server. If the server responds with a redirect to a new primary, immediately switch to that address and reset backoff and rotation.
        """

        # Build rotation list: start with the provided address, then others in random order
        others = [addr for addr in self.fileserver_addresses if addr != fileserver_address]
        random.shuffle(others)
        servers = [fileserver_address] + others if others else [fileserver_address]
        idx = 0

        current_fileserver_address = servers[idx]
        backoff = 1.0
        max_backoff = 30.0

        while True:
            print(f"[{self.client_id}] Uploading {remote_filename} to {current_fileserver_address}")
            fileserver_channel = None

            try:
                # Connect to file server
                fileserver_channel = grpc.insecure_channel(current_fileserver_address)
                fileserver_stub = fileserver_pb2_grpc.FileServerStub(fileserver_channel)

                # Fresh iterator each attempt; generators cannot be reused
                chunk_iterator = self._stream_upload(local_filepath, remote_filename)

                # Timeout to avoid hanging; wait_for_ready tolerates brief unavailability
                response = fileserver_stub.UploadFile(chunk_iterator, timeout=60.0, wait_for_ready=True)

                if response.success:
                    print(f"[{self.client_id}] Successfully uploaded {remote_filename} to {current_fileserver_address}")
                    return True

                print(f"[{self.client_id}] Upload to {current_fileserver_address} failed: {response.error_message}")

                # Follow redirect if provided and different
                redirect = getattr(response, "new_primary_fileserver", "")
                if redirect and redirect != current_fileserver_address:
                    print(f"[{self.client_id}] Redirecting to primary: {redirect}")
                    # Reset rotation to prioritize the redirected address; randomize the rest
                    others = [addr for addr in self.fileserver_addresses if addr != redirect]
                    random.shuffle(others)
                    servers = [redirect] + others if others else [redirect]
                    idx = 0
                    current_fileserver_address = servers[idx]
                    backoff = 1.0  # reset after redirect
                else:
                    # Exponential backoff with jitter
                    sleep_seconds = backoff + random.uniform(0, 0.5)
                    print(f"[{self.client_id}] Retry in {sleep_seconds:.1f}s...")
                    time.sleep(sleep_seconds)
                    backoff = min(max_backoff, backoff * 2)

                    # Rotate to the next fileserver for the next attempt
                    prev_addr = current_fileserver_address
                    idx = (idx + 1) % len(servers)
                    current_fileserver_address = servers[idx]
                    if current_fileserver_address != prev_addr:
                        print(f"[{self.client_id}] Switching to next fileserver: {current_fileserver_address}")
                        backoff = 1.0  # reset when switching servers
                    continue

            except grpc.RpcError as e:
                code = e.code().name if hasattr(e, "code") else "UNKNOWN"
                print(f"[{self.client_id}] RPC error to {current_fileserver_address}: {code} - {e}")

                # Backoff, then rotate to a different server on RPC errors as well
                sleep_seconds = backoff + random.uniform(0, 0.5)
                print(f"[{self.client_id}] Retry in {sleep_seconds:.1f}s...")
                time.sleep(sleep_seconds)
                backoff = min(max_backoff, backoff * 2)

                prev_addr = current_fileserver_address
                idx = (idx + 1) % len(servers)
                current_fileserver_address = servers[idx]
                if current_fileserver_address != prev_addr:
                    print(f"[{self.client_id}] Switching to next fileserver after RPC error: {current_fileserver_address}")
                    backoff = 1.0
                continue

            except Exception as e:
                print(f"[{self.client_id}] Upload to {current_fileserver_address} error: {e}")
                sleep_seconds = backoff + random.uniform(0, 0.5)
                print(f"[{self.client_id}] Retry in {sleep_seconds:.1f}s...")
                time.sleep(sleep_seconds)
                backoff = min(max_backoff, backoff * 2)

                # Rotate to the next fileserver for the next attempt
                prev_addr = current_fileserver_address
                idx = (idx + 1) % len(servers)
                current_fileserver_address = servers[idx]
                if current_fileserver_address != prev_addr:
                    print(f"[{self.client_id}] Switching to next fileserver: {current_fileserver_address}")
                    backoff = 1.0
                continue

            finally:
                try:
                    if fileserver_channel is not None:
                        fileserver_channel.close()
                except Exception as e:
                    print(f"[{self.client_id}] Unable to close connection to {current_fileserver_address} error: {e}")


    def process_file(
        self,
        filename: str,
        primary_fileserver_addr: str,
        start_index: int,
        end_index: int,
    ) -> bool:
        """Process specific line range of file to find primes, deduplicate, and upload result."""
        try:
            # Get input file
            filepath = self._fetch_file(filename)
        except Exception as e:
            print(f"[{self.client_id}] Error: {e}")
            return False

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

        # Write sorted primes to local file
        result_filename = (f"{Path(filename).stem}_range_{start_index}_{end_index}_primes.txt")
        result_path = self.cache_dir / result_filename

        with open(result_path, "w") as f:
            for prime in sorted(primes):
                f.write(f"{prime}\n")

        print(f"[{self.client_id}] Wrote deduplicated primes to {result_filename}")

        # Upload result to primary file server
        success = self._upload_file(result_path, result_filename, primary_fileserver_addr)

        # Delete local file only if upload succeeded
        if success:
            result_path.unlink()
            print(f"[{self.client_id}] Deleted local {result_filename}")

        return success


    def merge_files(self, file1: str, file2: str, task_id: str, primary_fileserver_addr: str) -> bool:
        """Download two deduplicated files, merge them, and upload result to file server."""
        print(f"[{self.client_id}] Started merge task: {task_id}")
        print(f"[{self.client_id}]   File 1: {file1}")
        print(f"[{self.client_id}]   File 2: {file2}")

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

            # Write sorted merged result to local file
            merged_filename = f"merged_{task_id}.txt"
            merged_path = self.cache_dir / merged_filename

            with open(merged_path, "w") as f:
                for prime in sorted(primes):
                    f.write(f"{prime}\n")

            print(f"[{self.client_id}] Wrote merged result to {merged_filename}")

            # Upload result to primary file server
            success = self._upload_file(merged_path, merged_filename, primary_fileserver_addr)

            # Delete local file only if upload succeeded
            if success:
                merged_path.unlink()
                print(f"[{self.client_id}] Deleted local {merged_filename}")

            return success

        except Exception as e:
            print(f"[{self.client_id}] Merge error: {e}")
            return False


    def _heartbeat_loop(self, address):
        """Background thread that sends periodic heartbeats."""
        while self.heartbeat_running:
            try:
                coordinator_stub = self._create_coordinator_connection_stub(address)
                request = coordinator_pb2.HeartbeatRequest(client_id=self.client_id)
                response = coordinator_stub.SendHeartbeat(request)

                if not response.acknowledged:
                    print(f"[{self.client_id}] Warning: Heartbeat not acknowledged by coordinator {address}")

            except grpc.RpcError as e:
                print(f"[{self.client_id}] Heartbeat error: {address} , {e}")

            time.sleep(self.heartbeat_interval)


    def _start_heartbeat(self, address):
        """Start heartbeat thread."""
        if address in self.heartbeat_threads and self.heartbeat_threads[address].is_alive:
            return
        
        thread = threading.Thread(target=self._heartbeat_loop, args=(address,), daemon=True, name=f"{self.client_id}-heartbeat")
        thread.start()
        self.heartbeat_threads[address] = thread
        print(f"[{self.client_id}] Heartbeat to {address} started with interval: {self.heartbeat_interval}s")


    def _stop_heartbeat(self):
        """Stop all heartbeat threads."""
        self.heartbeat_running = False
        for address, thread in list(self.heartbeat_threads.items()):
            try:
                if thread.is_alive():
                    thread.join(timeout=self.heartbeat_interval + 1)
            except Exception:
                pass
        self.heartbeat_threads.clear()


    def run(self):
        """Main loop: request tasks from coordinator and process them."""
        print(f"[{self.client_id}] Started")

        # Start heartbeat threads to coordinators
        for address in self.coordinator_addresses:
            self._start_heartbeat(address)
        
        # Start with the first coordinator
        current_coordinator = self.coordinator_addresses[0]

        # Exponential backoff parameters
        base_wait_time = 5
        max_wait_time = 60
        current_wait_time = base_wait_time

        while True:
            try:
                # Request task from coordinator
                print(f"[{self.client_id}] Requesting task from {current_coordinator}")
                
                coordinator_stub = self._get_coordinator_stub(current_coordinator)                
                request = coordinator_pb2.TaskRequest(client_id=self.client_id)
                task = coordinator_stub.RequestTask(request, timeout=30.0, wait_for_ready=True)

                # Handle different task statuses
                if task.status == coordinator_pb2.TASK_AVAILABLE:
                    # Reset wait time on successful task assignment
                    current_wait_time = base_wait_time
                elif task.status == coordinator_pb2.WAIT_FOR_DEPENDENCIES:
                    # Exponential backoff
                    sleep_seconds = current_wait_time + random.uniform(0, 0.5)
                    print(f"[{self.client_id}] Waiting for task dependencies for {sleep_seconds:.1f}s")
                    time.sleep(sleep_seconds)
                    current_wait_time = min(current_wait_time * 2, max_wait_time)
                    continue
                elif task.status == coordinator_pb2.NO_TASK_AVAILABLE:
                    print(f"[{self.client_id}] Stopped: no tasks available")
                    break

                print(f"[{self.client_id}] Received task from coordinator")
                print(f"[{self.client_id}] Task type: {task.task_type}")

                # Process task based on type
                start_time = time.time()
                success = False
                task_id = ""

                if task.task_type == coordinator_pb2.FIND_PRIMES:
                    print(f"[{self.client_id}] Task details:")
                    print(f"[{self.client_id}]   Task ID: {task.task_id}")
                    print(f"[{self.client_id}]   File: {task.filename}")
                    print(
                        f"[{self.client_id}]   Line range: {task.start_index} to {task.end_index - 1}"
                    )
                    print(
                        f"[{self.client_id}]   Primary server: {task.primary_fileserver_addr}"
                    )

                    success = self.process_file(
                        task.filename,
                        task.primary_fileserver_addr,
                        task.start_index,
                        task.end_index,
                    )
                    task_id = task.task_id

                elif task.task_type == coordinator_pb2.MERGE:
                    print(f"[{self.client_id}] Task details:")
                    print(f"[{self.client_id}]   Task ID: {task.task_id}")
                    print(f"[{self.client_id}]   File 1: {task.file1}")
                    print(f"[{self.client_id}]   File 2: {task.file2}")
                    print(f"[{self.client_id}]   Primary server: {task.primary_fileserver_addr}")

                    success = self.merge_files(
                        task.file1,
                        task.file2,
                        task.task_id,
                        task.primary_fileserver_addr,
                    )
                    task_id = task.task_id

                elapsed = time.time() - start_time
                print(f"[{self.client_id}] Task completed in {elapsed:.2f}s")

                # Acknowledge task completion
                if success:
                    print(f"[{self.client_id}] Acknowledging task completion")
                    completion_request = coordinator_pb2.TaskCompletionRequest(
                        client_id=self.client_id, task_id=task_id
                    )
                    response = self.coordinator.CompleteTask(completion_request)

                    if response.acknowledged:
                        print(f"[{self.client_id}] Task acknowledged by coordinator")
                    else:
                        print(f"[{self.client_id}] Warning: Task not acknowledged by coordinator")
                else:
                    print(f"[{self.client_id}] Task failed")
                    print(f"[{self.client_id}] Not acknowledging failed task")

            except KeyboardInterrupt:
                print(f"[{self.client_id}] Stopping")
                break
            except grpc.RpcError as e:
                code = e.code().name if hasattr(e, "code") else "UNKNOWN"
                print(f"[{self.client_id}] RPC error: {code} - {e}")

                # Exponential backoff
                sleep_seconds = current_wait_time + random.uniform(0, 0.5)
                time.sleep(sleep_seconds)
                current_wait_time = min(current_wait_time * 2, max_wait_time)

                print(f"[{self.client_id}] Trying another coordinator after {sleep_seconds:.1f}s")
                current_coordinator = self._next_coordinator(current_coordinator)
                continue
            except Exception as e:
                print(f"[{self.client_id}] Error: {e}")
                time.sleep(5)

        # Stop heartbeat thread
        self._stop_heartbeat()
        self._close_coordinator_channels()
        print(f"[{self.client_id}] Stopped")


def main():
    """Entry point for client."""
    parser = argparse.ArgumentParser(description="Distributed prime number client")
    parser.add_argument("client_id", help="Unique client identifier")
    parser.add_argument("coordinator_address", help="Coordinator address")
    parser.add_argument("--coordinators", nargs="+", required=True, help="List of coordinator addresses")
    parser.add_argument("--fileservers", nargs="+", required=True, help="List of file server addresses")

    args = parser.parse_args()

    client = Client(args.client_id, args.coordinator_address, args.coordinators, args.fileservers)
    client.run()


if __name__ == "__main__":
    main()
