import grpc
import hashlib
import sys
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
        cache_dir: str = "/tmp/client_cache",
        heartbeat_interval: int = 5,
    ):
        self.client_id = client_id
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Connect to coordinator via gRPC
        coordinator_channel = grpc.insecure_channel(coordinator_address)
        self.coordinator = coordinator_pb2_grpc.CoordinatorStub(coordinator_channel)

        # File cache
        self.cached_files = {}

        # Heartbeat control
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_running = True
        self.heartbeat_thread = None

        print(f"[{client_id}] Client initialized")
        print(f"[{client_id}] Cache directory: {self.cache_dir}")

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

    def _stream_fetch(self, chunk_stream, output_path: Path):
        """Receive chunks from file server and write to local file."""
        with open(output_path, "wb") as f:
            for chunk in chunk_stream:
                if chunk.HasField("metadata"):
                    # Skip metadata chunk
                    continue
                elif chunk.HasField("chunk_data"):
                    f.write(chunk.chunk_data)

    def _fetch_file(self, filename: str, fileserver_address: str) -> Path:
        """Fetch file from file server and cache it locally."""
        # Return cached version if available
        if filename in self.cached_files:
            print(f"[{self.client_id}] Using cached {filename}")
            return self.cached_files[filename]

        # Connect to file server
        print(f"[{self.client_id}] Fetching {filename} from {fileserver_address}")
        fileserver_channel = grpc.insecure_channel(fileserver_address)
        fileserver_stub = fileserver_pb2_grpc.FileServerStub(fileserver_channel)

        # Request file via RPC
        request = fileserver_pb2.FetchRequest(filename=filename)
        chunk_stream = fileserver_stub.FetchFile(request)

        # Save to local cache
        cached_path = self.cache_dir / Path(filename).name

        try:
            self._stream_fetch(chunk_stream, cached_path)
        except grpc.RpcError as e:
            raise FileNotFoundError(f"File {filename} not found: {e}")

        print(
            f"[{self.client_id}] Successfully fetched {filename} from {fileserver_address}"
        )

        self.cached_files[filename] = cached_path
        print(f"[{self.client_id}] Cached {filename} locally")
        return cached_path

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
        fileserver_address: str,
        start_index: int,
        end_index: int,
    ) -> bool:
        """Process specific line range of file to find primes, deduplicate, and upload result."""
        try:
            # Get input file
            filepath = self._fetch_file(filename, fileserver_address)
        except FileNotFoundError as e:
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

        # Write sorted primes to local file
        result_filename = (
            f"{Path(filename).stem}_range_{start_index}_{end_index}_primes.txt"
        )
        result_path = self.cache_dir / result_filename

        with open(result_path, "w") as f:
            for prime in sorted(primes):
                f.write(f"{prime}\n")

        print(f"[{self.client_id}] Wrote deduplicated primes to {result_filename}")

        # Upload result to file server
        success = self._upload_file(result_path, result_filename, fileserver_address)

        # Delete local file only if upload succeeded
        if success:
            result_path.unlink()
            print(f"[{self.client_id}] Deleted local {result_filename}")

        return success

    def merge_files(
        self, file1: str, file2: str, task_id: str, fileserver_address: str
    ) -> bool:
        """Download two deduplicated files, merge them, and upload result to file server."""
        print(f"[{self.client_id}] Started merge task: {task_id}")
        print(f"[{self.client_id}]   File 1: {file1}")
        print(f"[{self.client_id}]   File 2: {file2}")

        try:
            # Download both files from file server
            path1 = self._fetch_file(file1, fileserver_address)
            path2 = self._fetch_file(file2, fileserver_address)

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

            # Write sorted merged result to local file
            merged_filename = f"merged_{task_id}.txt"
            merged_path = self.cache_dir / merged_filename

            with open(merged_path, "w") as f:
                for prime in sorted(primes):
                    f.write(f"{prime}\n")

            print(f"[{self.client_id}] Wrote merged result to {merged_filename}")

            # Upload result to file server
            success = self._upload_file(
                merged_path, merged_filename, fileserver_address
            )

            # Delete local file only if upload succeeded
            if success:
                merged_path.unlink()
                print(f"[{self.client_id}] Deleted local {merged_filename}")

            return success

        except Exception as e:
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

        no_task_attempts = 0
        max_no_task_attempts = 3

        while True:
            try:
                # Request task from coordinator
                print(f"[{self.client_id}] Requesting task from coordinator")
                request = coordinator_pb2.TaskRequest(client_id=self.client_id)
                task = self.coordinator.RequestTask(request)

                if not task.has_task:
                    no_task_attempts += 1
                    if no_task_attempts >= max_no_task_attempts:
                        print(
                            f"[{self.client_id}] Info: No tasks available after {max_no_task_attempts} attempts"
                        )
                        break
                    print(f"[{self.client_id}] Info: No tasks available")
                    time.sleep(5)
                    continue

                no_task_attempts = 0

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
    if len(sys.argv) != 3:
        print("Usage: python -m client.client <client_id> <coordinator_address>")
        sys.exit(1)

    client_id = sys.argv[1]
    coordinator_addr = sys.argv[2]

    client = Client(client_id, coordinator_addr)
    client.run()


if __name__ == "__main__":
    main()
