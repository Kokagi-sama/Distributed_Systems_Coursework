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
        fileserver_addresses: list[str],
        cache_dir: str = "/tmp/client_cache",
        heartbeat_interval: int = 5,
    ):
        self.client_id = client_id
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.fileserver_addresses = fileserver_addresses

        # Clean up orphaned .tmp files from previous crashes or errors
        tmp_files = list(self.cache_dir.glob("*.tmp"))
        if tmp_files:
            print(f"[{client_id}] Cleaning up {len(tmp_files)} orphaned temp files")
            for tmp_file in tmp_files:
                try:
                    tmp_file.unlink()
                except Exception as e:
                    print(
                        f"[{client_id}] Warning: Could not remove {tmp_file.name}: {e}"
                    )

        # Connect to coordinator via gRPC
        coordinator_channel = grpc.insecure_channel(coordinator_address)
        self.coordinator = coordinator_pb2_grpc.CoordinatorStub(coordinator_channel)

        # Heartbeat control
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_running = True
        self.heartbeat_thread = None

        print(f"[{client_id}] Client initialized")
        print(f"[{client_id}] Cache directory: {self.cache_dir}")
        print(f"[{client_id}] File servers: {', '.join(fileserver_addresses)}")

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

    def _stream_upload(
        self, filepath: Path, remote_filename: str, chunk_size: int = 65536
    ):
        """Read local file and generate chunks for upload to file server."""
        file_size = filepath.stat().st_size

        # Compute checksum
        checksum = self._compute_checksum(filepath)

        # Send metadata first
        metadata = fileserver_pb2.FileMetadata(
            filename=remote_filename,
            file_size=file_size,
            atomic_upload=True,
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

            tmp_path.rename(output_path)
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
                print(
                    f"[{self.client_id}] Fetching {filename} from {fileserver_address}"
                )
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
                        raise RuntimeError(
                            f"Checksum verification failed for {filename}: expected {server_checksum}, got {computed_checksum}"
                        )
                    print(
                        f"[{self.client_id}] Successfully verified checksum for {filename}"
                    )

                print(
                    f"[{self.client_id}] Successfully fetched {filename} from {fileserver_address}"
                )
                print(f"[{self.client_id}] Cached {filename} locally")
                return cached_path

            except Exception as e:
                print(
                    f"[{self.client_id}] Failed to fetch from {fileserver_address}: {e}"
                )
                last_error = e
                continue

        raise RuntimeError(
            f"Failed to fetch {filename} from all file servers: {last_error}"
        )

    def _upload_file(
        self, local_filepath: Path, remote_filename: str, fileserver_address: str
    ) -> bool:
        """Upload file to file server."""
        print(f"[{self.client_id}] Uploading {remote_filename} to {fileserver_address}")

        try:
            # Connect to file server
            fileserver_channel = grpc.insecure_channel(fileserver_address)
            fileserver_stub = fileserver_pb2_grpc.FileServerStub(fileserver_channel)

            # Upload file via RPC
            chunk_iterator = self._stream_upload(local_filepath, remote_filename)
            response = fileserver_stub.UploadFile(chunk_iterator)

            if not response.success:
                print(f"[{self.client_id}] Upload failed: {response.error_message}")
                return False

            print(
                f"[{self.client_id}] Successfully uploaded {remote_filename} to {fileserver_address}"
            )
            return True

        except Exception as e:
            print(f"[{self.client_id}] Upload error: {e}")
            return False

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

        print(
            f"[{self.client_id}] Started processing {filename} from line {start_index} to {end_index - 1}"
        )

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
                    print(
                        f"[{self.client_id}] Warning: Invalid number on line {line_num + 1}: {line}"
                    )
                    continue

        print(
            f"[{self.client_id}] Finished processing {filename}: found {len(primes):,} unique primes from line {start_index} to {end_index - 1}"
        )

        # Write sorted primes atomically
        result_filename = (
            f"{Path(filename).stem}_range_{start_index}_{end_index}_primes.txt"
        )
        result_path = self.cache_dir / result_filename
        tmp_path = Path(str(result_path) + ".tmp")

        try:
            with open(tmp_path, "w") as f:
                for prime in sorted(primes):
                    f.write(f"{prime}\n")

            tmp_path.rename(result_path)
            print(f"[{self.client_id}] Wrote deduplicated primes to {result_filename}")

            # Upload result to primary file server
            success = self._upload_file(
                result_path, result_filename, primary_fileserver_addr
            )
            return success

        except Exception as e:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass
            print(f"[{self.client_id}] Error writing result: {e}")
            return False

    def merge_files(
        self, file1: str, file2: str, task_id: str, primary_fileserver_addr: str
    ) -> bool:
        """Download two deduplicated files, merge them, and upload result to file server."""
        print(f"[{self.client_id}] Started merge task: {task_id}")
        print(f"[{self.client_id}]   File 1: {file1}")
        print(f"[{self.client_id}]   File 2: {file2}")

        merged_filename = f"merged_{task_id}.txt"
        merged_path = self.cache_dir / merged_filename
        tmp_path = Path(str(merged_path) + ".tmp")

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

            print(
                f"[{self.client_id}] Finished merging: {len(primes):,} unique primes total"
            )

            # Write sorted merged result atomically
            with open(tmp_path, "w") as f:
                for prime in sorted(primes):
                    f.write(f"{prime}\n")

            tmp_path.rename(merged_path)
            print(f"[{self.client_id}] Wrote merged result to {merged_filename}")

            # Upload result to primary file server
            success = self._upload_file(
                merged_path, merged_filename, primary_fileserver_addr
            )
            return success

        except Exception as e:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass
            print(f"[{self.client_id}] Merge error: {e}")
            return False

    def _heartbeat_loop(self):
        """Background thread that sends periodic heartbeats."""
        while self.heartbeat_running:
            try:
                request = coordinator_pb2.HeartbeatRequest(client_id=self.client_id)
                response = self.coordinator.SendHeartbeat(request)

                if not response.acknowledged:
                    print(
                        f"[{self.client_id}] Warning: Heartbeat not acknowledged by coordinator"
                    )

            except grpc.RpcError as e:
                print(f"[{self.client_id}] Heartbeat error: {e}")

            time.sleep(self.heartbeat_interval)

    def _start_heartbeat(self):
        """Start heartbeat thread."""
        self.heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True, name=f"{self.client_id}-heartbeat"
        )
        self.heartbeat_thread.start()
        print(
            f"[{self.client_id}] Heartbeat started with interval: {self.heartbeat_interval}s"
        )

    def _stop_heartbeat(self):
        """Stop heartbeat thread."""
        self.heartbeat_running = False
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=self.heartbeat_interval + 1)

    def run(self):
        """Main loop: request tasks from coordinator and process them."""
        print(f"[{self.client_id}] Started")

        # Start heartbeat thread
        self._start_heartbeat()

        # Exponential backoff parameters
        base_wait_time = 5
        max_wait_time = 60
        current_wait_time = base_wait_time

        while True:
            try:
                # Request task from coordinator
                print(f"[{self.client_id}] Requesting task from coordinator")
                request = coordinator_pb2.TaskRequest(client_id=self.client_id)
                task = self.coordinator.RequestTask(request)

                # Handle different task statuses
                if task.status == coordinator_pb2.TASK_AVAILABLE:
                    # Reset wait time on successful task assignment
                    current_wait_time = base_wait_time
                elif task.status == coordinator_pb2.WAIT_FOR_DEPENDENCIES:
                    print(f"[{self.client_id}] Waiting for task dependencies")
                    time.sleep(current_wait_time)
                    # Exponential backoff
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
                    print(
                        f"[{self.client_id}]   Primary server: {task.primary_fileserver_addr}"
                    )

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
                        print(
                            f"[{self.client_id}] Warning: Task not acknowledged by coordinator"
                        )
                else:
                    print(f"[{self.client_id}] Task failed")
                    print(f"[{self.client_id}] Not acknowledging failed task")

            except KeyboardInterrupt:
                print(f"[{self.client_id}] Stopping")
                break
            except grpc.RpcError as e:
                # Retry on network errors
                print(f"[{self.client_id}] RPC error: {e}")
                print(f"[{self.client_id}] Retrying in 5s")
                time.sleep(5)
            except Exception as e:
                print(f"[{self.client_id}] Error: {e}")
                time.sleep(5)

        # Stop heartbeat thread
        self._stop_heartbeat()
        print(f"[{self.client_id}] Stopped")


def main():
    """Entry point for client."""
    parser = argparse.ArgumentParser(description="Distributed prime number client")
    parser.add_argument("client_id", help="Unique client identifier")
    parser.add_argument("coordinator_address", help="Coordinator address")
    parser.add_argument(
        "--fileservers", nargs="+", required=True, help="List of file server addresses"
    )

    args = parser.parse_args()

    client = Client(args.client_id, args.coordinator_address, args.fileservers)
    client.run()


if __name__ == "__main__":
    main()
