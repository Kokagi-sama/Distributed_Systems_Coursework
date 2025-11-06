import grpc
import sys
import threading
import time

from generated import coordinator_pb2
from generated import coordinator_pb2_grpc
from generated import fileserver_pb2
from generated import fileserver_pb2_grpc


class CoordinatorServer(coordinator_pb2_grpc.CoordinatorServicer):

    def __init__(
        self,
        address: str,
        port: int,
        file_list: str,
        heartbeat_interval: int = 5,
    ):
        self.address = address  # Server address
        self.port = port  # Server port
        self.heartbeat_interval = heartbeat_interval  # Heartbeat interval in seconds
        self.file_list = file_list  # Path to the file containing list of files to process
        self.lock = threading.Lock()  # Add a lock for thread safety
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

        # To track connected clients/workers {client_id: [client_alive_status, last_heartbeat_time, assigned_task_id]}
        self.clients = {}

        self.tasks = {}  # To track tasks {task_id: Task object}

    def initialize(self):
        """
        Initizalizes the server by loading the file list, setting up initial state.
        """
        print(f"Initializing Coordinator Server at {self.address}:{self.port}")
        self.load_file_list()
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
                self.clients[client_id] = [True, time.time(), None]
                print(f"Registered new client: {client_id}")
            else:
                print(f"Client {client_id} is already registered.")

            # Start heartbeat monitoring for the client

    def load_file_list(self):
        """
        Loads the file list and adds them to the pending_prime state.
        """
        print(f"Loading file list from {self.file_list}")
        try:
            # open file
            with open(self.file_list, 'r') as f:
                files = f.readlines()
                # adding all files pending_prime state
                self.state['pending_prime'] = [file.strip() for file in files]
            print(
                f"Loaded {len(self.state['pending_prime'])} files for processing.")
        except Exception as e:
            print(f"Error loading file list: {e}")
            sys.exit(1)

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
                    # Print final result
                    print(
                        f"All processing completed. Result file: {self.result_filename}")
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
            task = self.tasks.get(task_id)
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

    def start_heartbeat(self):
        pass

    # RPC Methods

    def RequestTask(self, request, context):
        client_id = request.client_id
        self.register_client(client_id)

        task = self.assign_task(client_id)

        if not task:
            # No task available
            return coordinator_pb2.TaskResponse(has_task=False)

        if task.task_type == 0:  # FIND_PRIMES
            return coordinator_pb2.TaskResponse(
                has_task=True,
                task_type=coordinator_pb2.FIND_PRIMES,
                primary_fileserver_addr=self.primary_fileserver_addr,
                filename=task.task_files[0],
                task_id=str(task.task_id)
            )
        elif task.task_type == 1:  # MERGE
            return coordinator_pb2.TaskResponse(
                has_task=True,
                task_type=coordinator_pb2.MERGE,
                primary_fileserver_addr=self.primary_fileserver_addr,
                task_id=str(task.task_id),
                file1=task.task_files[0],
                file2=task.task_files[1]
            )

        # Should not be reached
        return coordinator_pb2.TaskResponse(has_task=False)

    def CompleteTask(self, request, context):
        client_id = request.client_id
        task_id = request.task_id
        new_file = request.new_file

        self.complete_task(task_id, client_id, new_file)

        return coordinator_pb2.TaskCompletionResponse(acknowledged=True)

    # Not doing anything for heartbeat for now just responding acknowledged

    def SendHeartbeat(self, request, context):
        # self.register_client(client_id) # Not doing it here for now
        return coordinator_pb2.HeartbeatResponse(acknowledged=True)


class Task:

    task_id_static = 1  # Starts from 1, increments with each new task

    def __init__(self, client_id: str, task_type: str, task_files: list, task_status: int = 0):
        self.task_id = Task.task_id_static + 1
        self.client_id = client_id
        self.task_type = task_type  # 0: prime, 1: de-duplication
        self.task_files = task_files
        self.task_status = task_status  # 0: in-progress, 1: completed
        Task.task_id_static += 1


def main():

    print("Coordinator Server is running...")
    # Example of creating a Task object
    task = Task(client_id="worker-1", task_type="prime",
                task_files=["file1.txt", "file2.txt"])
    print(
        f"Created task {task.task_id} for client {task.client_id} of type {task.task_type}")

    print(Task.task_id_s)


if __name__ == "__main__":
    main()
