import grpc
import sys
import threading
import time
from concurrent import futures
from pathlib import Path
import argparse
import csv

# Add project root to Python path
project_root = str(Path(__file__).resolve().parents[1])
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# fmt: off
from generated import coordinator_pb2 # isort: skip
from generated import coordinator_pb2_grpc # isort: skip
# fmt: on


class CoordinatorServer(coordinator_pb2_grpc.CoordinatorServicer):

    def __init__(
        self,
        address: str,
        port: int,
        file_list_path: str,
        client_response_timeout: int = 15,
    ):
        self.address = address  # Server address
        self.port = port  # Server port
        # self.heartbeat_interval = heartbeat_interval  # Heartbeat interval in seconds
        # Path to the file containing list of files to process
        self.file_list_path = file_list_path
        self.file_list = []  # List of files to process
        self.file_size = []  # List of file sizes
        self.lock = threading.RLock()  # Add a lock for thread safety
        # Time in seconds to wait before considering a client dead
        self.client_response_timeout = client_response_timeout

        self.processing_complete = False  # Flag to indicate if processing is complete
        # State dictionaries to track file processing status
        self.state = {
            "pending_prime": [],
            "in_progress_prime": [],
            "completed_prime": [],
            "pending_de_duplication": [],
            "in_progress_de_duplication": [],
            "completed_de_duplication": [],
        }
        self.result_filename = ""  # To store the final result filename

        # To track connected clients/workers {client_id: [alive_status, last_heartbeat_time, assigned_task_id]}
        self.clients = {}

        self.tasks = {}  # To track tasks {task_id: Task object}

    def initialize(self):
        """
        Initizalizes the server by loading the file list, setting up initial state.
        """
        print(f"Initializing Coordinator Server at {self.address}:{self.port}")
        self.load_file_list()
        self.initialize_files_for_processing()
        print(
            f"Number of files to process: {len(self.state['pending_prime'])}")

        # self.start_heartbeat()

    def register_client(self, client_id: str):
        """
        Registers a new client with the server (adds client id to the clients dictionary), and start heartbeat.
        """
        with self.lock:
            if client_id not in self.clients:
                # [alive_status, last_heartbeat_time, assigned_task_id]
                self.clients[client_id] = [True, time.time(), 0]
            else:
                print(f"Client {client_id} is already registered.")

            # Start heartbeat monitoring for the client

    def load_file_list(self):
        """
        Loads the file list from a CSV and adds first column to self.file_list and state['pending_prime'],
        and second column to self.file_size. The CSV is expected to contain filenames in the first column
        and optional file sizes in the second column.
        """
        with self.lock:
            print(f"Loading file list from {self.file_list_path}")
            try:
                with open(self.file_list_path, 'r', newline='') as f:
                    reader = csv.reader(f)

                    first_row = reader.__next__()
                    # Check if first row is header
                    if first_row[0].lower() == 'file_name':
                        pass  # Skip header row
                    else:
                        # Process first row
                        if first_row:
                            self.file_list.append(first_row[0].strip())
                            self.file_size.append(int(first_row[1].strip()))
                    # Process remaining rows
                    for row in reader:
                        if not row:
                            continue  # Skip empty rows
                        # Get filename
                        self.file_list.append(row[0].strip())
                        # Get file size
                        self.file_size.append(int(row[1].strip()))

                print(f"Read {len(self.file_list)} files.")
            except Exception as e:
                print(f"Error loading file list: {e}")
                sys.exit(1)

    def initialize_files_for_processing(self):
        """Adds the first list of files to queue for processing divides them based on number of files

        Args:
            files (list): list of all files
        """

        # get average numbers per file
        with self.lock:
            num_files = max(len(self.file_list), 3)
            tot_numbers = sum(self.file_size)
            avg_numbers_per_file = int(
                tot_numbers / num_files) if num_files > 0 else 0
            print(
                f"Average numbers per worker: {avg_numbers_per_file}, Total files: {num_files}, Total numbers: {tot_numbers}")

            # Distribute files into pending_prime based on average size
            for filename, size in zip(self.file_list, self.file_size):

                if size <= avg_numbers_per_file:
                    self.state['pending_prime'].append((filename, 0, size))
                else:
                    size_temp = size
                    i = 1
                    while size_temp > avg_numbers_per_file:
                        self.state['pending_prime'].append(
                            (filename, size - size_temp, int(avg_numbers_per_file) * i))
                        size_temp -= avg_numbers_per_file
                        i += 1
                    if size_temp > 0:
                        self.state['pending_prime'].append(
                            (filename, size - size_temp, int(size)))
            print(self.state['pending_prime'])

    def check_processing_complete(self):
        """Checks if all processing is completed
        Returns:
            Boolean: true if completed, false otherwise
        """
        with self.lock:
            # check if there are no pending or in-progress tasks and at most one pending de-duplication file
            if (not self.state['pending_prime'] and
                    not self.state['in_progress_prime'] and
                    not self.state['in_progress_de_duplication'] and
                    len(self.state['pending_de_duplication']) == 1):
                self.processing_complete = True
                return True
            return False

    # TODO: change according to implementation, depends on how the state is being organized
    # Pick a task based on current state and return a Task object
    def create_task(self, client_id: str):
        """Creates a task Obejct based on current state and returns it

        Args:
            client_id (str): unique client id

        Returns:
            Task: Task object or None if no tasks available
        """
        with self.lock:
            if self.state['pending_prime']:
                task_type = 0  # Prime task
                task_files = [self.state['pending_prime'].pop(0)]
            elif len(self.state['pending_de_duplication']) > 1:
                task_type = 1  # De-duplication task
                task_files = [self.state['pending_de_duplication'].pop(
                    0), self.state['pending_de_duplication'].pop(0)]
            elif len(self.state['pending_de_duplication']) == 1:
                if (self.check_processing_complete()):
                    self.result_filename = self.state['pending_de_duplication'][0]
                    return None
            else:
                return None  # No tasks available
            # Creating Task object
            task = Task(client_id, task_type, task_files)
            # Adding task to tasks dictionary
            self.tasks[task.task_id] = task
            return task

    def assign_task(self, client_id: str):
        """Create a task object and assigns it to a client, and updates the state

        Args:
            client_id (str): unique client id

        Returns:
            Task: Returns assigned Task object or None if no tasks available
        """
        with self.lock:
            # Create new task
            task = self.create_task(client_id)
            if task:
                print(f"Assigning task {task.task_id} to client {client_id}")
                # Update assigned task ID in the clients dictionary
                self.clients[client_id][2] = task.task_id
                # Update state to reflect in-progress task
                if task.task_type == 0:
                    self.state['in_progress_prime'].append(task.task_files[0])
                else:
                    self.state['in_progress_de_duplication'].extend(
                        task.task_files)
                return task
            print(f"No tasks available to assign to client {client_id}")
            return None

    # TODO: Maybe required depending on implementation
    def handle_state(self):
        pass

    def complete_task(self, task_id, client_id, new_file: str):
        """Responds to client task complete, updates the state based on completed task

        Args:
            task_id (_type_): unique task id
            client_id (_type_): unique client id
            new_file (str): name of the new file returned by the client
        """
        with self.lock:
            task = self.tasks.get(int(task_id))
            if not task:
                print(f"Task {task_id} not found.")
                return
            print(f"Completing task {task_id} from client {client_id}")
            # Update state based on task type
            if task.task_type == 0:
                # For prime task
                self.state['in_progress_prime'].remove(
                    task.task_files[0])  # Remove from in-progress
                self.state['completed_prime'].append(
                    task.task_files[0])  # Add to completed
                # Add new returned file to pending de-duplication
                self.state['pending_de_duplication'].append(new_file)
            else:
                # For de-duplication task
                # Remove both files from in-progress
                self.state['in_progress_de_duplication'].remove(
                    task.task_files[0])
                self.state['in_progress_de_duplication'].remove(
                    task.task_files[1])
                # Add both files to completed
                self.state['completed_de_duplication'].extend(
                    [task.task_files[0], task.task_files[1]])

                # Add new returned file to pending de-duplication
                self.state['pending_de_duplication'].append(new_file)
            self.clients[client_id][2] = 0  # Clear assigned task ID

    def start_heartbeat(self):
        pass

    def check_dead_clients_reassign_tasks(self):
        for client_id, (alive_status, last_heartbeat, assigned_task_id) in self.clients.items():
            if ((time.time() - last_heartbeat > self.client_response_timeout) and alive_status):
                # Mark client as dead
                self.clients[client_id][0] = False
                # Log dead client
                now = time.time()
                last_time = time.strftime(
                    '%Y-%m-%d %H:%M:%S', time.localtime(last_heartbeat))
                now_time = time.strftime(
                    '%Y-%m-%d %H:%M:%S', time.localtime(now))
                print(
                    f"Client {client_id} is considered dead. Last heartbeat at {last_time} (ts={last_heartbeat}), current time {now_time} (ts={now})")
                # Reassign the task if there was an assigned task
                if assigned_task_id:
                    self.reassign_task(assigned_task_id)

    def reassign_task(self, task_id):
        with self.lock:
            task = self.tasks.get(task_id)
            if not task:
                print(f"Task {task_id} not found for reassignment.")
                return
            print(f"Reassigning task {task_id} back to pending state.")
            # Update state based on task type
            if task.task_type == 0:
                # For prime task
                if task.task_files[0] in self.state['in_progress_prime']:

                    self.state['in_progress_prime'].remove(
                        task.task_files[0])  # Remove from in-progress
                    self.state['pending_prime'].append(
                        task.task_files[0])  # Add back to pending
            else:
                # For de-duplication task
                for file in task.task_files:
                    if file in self.state['in_progress_de_duplication']:
                        self.state['in_progress_de_duplication'].remove(
                            file)  # Remove from in-progress
                        self.state['pending_de_duplication'].append(
                            file)  # Add back to pending

    def update_heartbeat(self, client_id: str):
        with self.lock:
            if (client_id in self.clients):
                # Update last heartbeat time
                self.clients[client_id][1] = time.time()
                self.clients[client_id][0] = True  # Mark as alive
                print(f"Heartbeat received from client {client_id}")

    # RPC Methods

    def RequestTask(self, request, context):
        client_id = request.client_id
        print(f"Client {client_id} is requesting a task.")
        self.register_client(client_id)
        print(f"Registered client {client_id} for task request.")
        task = self.assign_task(client_id)
        # No task available
        if not task:
            # Processing complete
            if self.processing_complete:
                print(
                    f"All processing completed. Result file: {self.result_filename}")
                return coordinator_pb2.TaskResponse(has_task=False, client_task_status=coordinator_pb2.ClientTaskStatus.COMPLETED)
            # Processing not complete, but no tasks available
            return coordinator_pb2.TaskResponse(has_task=False, client_task_status=coordinator_pb2.ClientTaskStatus.WAIT)

        if task.task_type == 0:  # FIND_PRIMES
            return coordinator_pb2.TaskResponse(
                has_task=True,
                task_type=coordinator_pb2.TaskType.FIND_PRIMES,
                filename=task.task_files[0][0],
                start_index=task.task_files[0][1],
                end_index=task.task_files[0][2],

                task_id=str(task.task_id)
            )
        elif task.task_type == 1:  # MERGE
            return coordinator_pb2.TaskResponse(
                has_task=True,
                task_type=coordinator_pb2.TaskType.MERGE,
                task_id=str(task.task_id),
                file1=task.task_files[0],
                file2=task.task_files[1]
            )

        # Just in case lol
        return coordinator_pb2.TaskResponse(has_task=False)

    def UpdateTaskStatus(self, request, context):
        client_id = request.client_id
        task_id = request.task_id
        output_filename = request.output_filename

        self.complete_task(task_id, client_id, output_filename)

        return coordinator_pb2.TaskStatusResponse(acknowledged=True)

    def SendHeartbeat(self, request, context):
        client_id = request.client_id
        self.update_heartbeat(client_id)
        return coordinator_pb2.HeartbeatResponse(acknowledged=True)

    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        coordinator_pb2_grpc.add_CoordinatorServicer_to_server(
            self, server)
        server.add_insecure_port(f"{self.address}:{self.port}")
        server.start()
        print(f"Coordinator Server started at {self.address}:{self.port}")
        try:
            while True:
                time.sleep(10)
                self.check_dead_clients_reassign_tasks()
        except KeyboardInterrupt:
            print("Shutting down Coordinator Server...")
            server.stop(0)


class Task:

    task_id_static = 0  # Starts from 0, increments with each new task

    def __init__(self, client_id: str, task_type: str, task_files: list, task_status: int = 0):
        self.task_id = Task.task_id_static + 1
        self.client_id = client_id
        self.task_type = task_type  # 0: prime, 1: de-duplication
        self.task_files = task_files
        self.task_status = task_status  # 0: in-progress, 1: completed
        Task.task_id_static += 1

    def set_global_taskID(self, task_id: int):
        Task.task_id_static = self.task_id


def main():
    parser = argparse.ArgumentParser(description="Coordinator server CLI")
    parser.add_argument("--address", "-a",
                        default="localhost", help="Server address")
    parser.add_argument("--port", "-p", type=int,
                        default=50051, help="Server port")
    parser.add_argument("--file-list", "-f",
                        default="file_list.csv", help="Path to the file list")
    parser.add_argument("--timeout", "-t", type=int, default=30,
                        help="Client response timeout (seconds)")
    args = parser.parse_args()

    file_path = Path(args.file_list)
    if not file_path.exists():
        print(f"File list not found: {file_path}")
        sys.exit(1)

    coordinator_server = CoordinatorServer(
        address=args.address,
        port=args.port,
        file_list_path=str(file_path),
        client_response_timeout=args.timeout,
    )
    coordinator_server.initialize()
    coordinator_server.serve()


if __name__ == "__main__":
    main()
