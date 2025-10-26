import grpc
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

    def _fetch_file(self, filename: str, fileserver_address: str) -> Path:
        """Fetch file from server and cache locally."""
        # Check if already cached
        if filename in self.cached_files:
            print(f"[{self.client_id}] Using cached {filename}")
            return self.cached_files[filename]

        # Connect to assigned file server
        print(f"[{self.client_id}] Fetching {filename} from {fileserver_address}")
        fileserver_channel = grpc.insecure_channel(fileserver_address)
        fileserver_stub = fileserver_pb2_grpc.FileServerStub(fileserver_channel)

        # Fetch from file server
        request = fileserver_pb2.FetchRequest(filename=filename)
        response = fileserver_stub.FetchFile(request)

        if not response.success:
            raise FileNotFoundError(
                f"File {filename} not found: {response.error_message}"
            )

        print(
            f"[{self.client_id}] Fetched {filename} successfully from {fileserver_address}"
        )

        # Write to local cache
        cached_path = self.cache_dir / Path(filename).name
        cached_path.write_bytes(response.file_data)

        self.cached_files[filename] = cached_path
        print(f"[{self.client_id}] Cached {filename} locally")
        return cached_path

    def process_chunk(
        self, filename: str, start_line: int, end_line: int, fileserver_address: str
    ) -> list[int]:
        """Read file chunk and return prime numbers found."""
        primes = []

        try:
            filepath = self._fetch_file(filename, fileserver_address)
        except FileNotFoundError as e:
            print(f"[{self.client_id}] Error: {e}")
            return primes

        print(f"[{self.client_id}] Processing chunk started")

        with open(filepath, "r") as f:
            # Skip to the chunk start
            for _ in range(start_line):
                f.readline()

            # Check each line in range for primes
            for line_num in range(start_line, end_line):
                line = f.readline()
                if not line:
                    break

                line = line.strip()
                if not line:
                    continue

                try:
                    num = int(line)
                    if isprime(num):
                        primes.append(num)
                except ValueError:
                    print(
                        f"[{self.client_id}] Warning: Invalid number on line {line_num}: {line}"
                    )
                    continue

        print(f"[{self.client_id}] Processing chunk finished")
        return primes

    def _heartbeat_loop(self):
        """Background thread that sends periodic heartbeats."""
        while self.heartbeat_running:
            try:
                request = coordinator_pb2.HeartbeatRequest(client_id=self.client_id)
                response = self.coordinator.SendHeartbeat(request)

                if not response.acknowledged:
                    print(f"[{self.client_id}] Warning: Heartbeat not acknowledged")

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
            f"[{self.client_id}] Heartbeat started (interval: {self.heartbeat_interval}s)"
        )

    def _stop_heartbeat(self):
        """Stop heartbeat thread."""
        self.heartbeat_running = False
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=self.heartbeat_interval + 1)

    def run(self):
        """Main loop: request work, process chunk, submit result."""
        print(f"[{self.client_id}] Started")

        # Start heartbeat thread
        self._start_heartbeat()

        no_work_attempts = 0
        max_no_work_attempts = 3

        while True:
            try:
                # Request work from coordinator
                print(f"[{self.client_id}] Requesting work from coordinator")
                request = coordinator_pb2.WorkRequest(client_id=self.client_id)
                work = self.coordinator.RequestWork(request)

                if not work.has_work:
                    no_work_attempts += 1
                    if no_work_attempts >= max_no_work_attempts:
                        print(
                            f"[{self.client_id}] Info: No work available after {max_no_work_attempts} attempts"
                        )
                        break
                    print(f"[{self.client_id}] Info: No work available")
                    time.sleep(5)
                    continue

                no_work_attempts = 0

                print(f"[{self.client_id}] Received work from coordinator")
                print(f"[{self.client_id}] Work details:")
                print(f"[{self.client_id}]   Chunk ID: {work.chunk_id}")
                print(f"[{self.client_id}]   File: {work.filename}")
                print(f"[{self.client_id}]   Lines: {work.start_line}-{work.end_line}")
                print(f"[{self.client_id}]   File server: {work.fileserver_address}")

                # Process assigned chunk
                start_time = time.time()
                primes = self.process_chunk(
                    work.filename,
                    work.start_line,
                    work.end_line,
                    work.fileserver_address,
                )
                elapsed = time.time() - start_time

                print(
                    f"[{self.client_id}] Processing result: Found {len(primes)} primes in {elapsed:.2f}s"
                )

                # Submit result back
                print(f"[{self.client_id}] Submitting result to coordinator")
                submit_request = coordinator_pb2.SubmitRequest(
                    client_id=self.client_id, chunk_id=work.chunk_id, primes=primes
                )
                response = self.coordinator.SubmitPrimes(submit_request)

                if response.success:
                    print(f"[{self.client_id}] Submit result: Success")
                else:
                    print(f"[{self.client_id}] Submit result: Failed")

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
