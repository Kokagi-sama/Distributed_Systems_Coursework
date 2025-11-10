import hashlib
import time
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import grpc
import pytest

from client.client import Client
from generated import coordinator_pb2, fileserver_pb2


@pytest.fixture
def temp_test_file(tmp_path):
    """Create temporary test file with numbers."""
    test_file = tmp_path / "test_numbers.txt"
    numbers = [17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]
    test_file.write_text("\n".join(map(str, numbers)))
    return test_file


@pytest.fixture
def file_servers():
    """List of file server addresses for testing."""
    return ["localhost:50052", "localhost:50053", "localhost:50054"]


@pytest.fixture
def coordinators():
    """List of coordinator addresses for testing."""
    return ["localhost:50051", "localhost:50052"]


@pytest.fixture
def test_cache_dir(tmp_path):
    """Unique cache directory for each test."""
    return str(tmp_path / "client_cache")


def create_upload_capturer():
    """Create a function that captures uploaded chunks."""
    uploaded_data = []

    def capture_upload(chunk_iterator, **kwargs):
        for chunk in chunk_iterator:
            uploaded_data.append(chunk)
        return fileserver_pb2.UploadResponse(success=True, error_message="")

    return capture_upload, uploaded_data


def create_fetch_chunks(filename, file_content, checksum=None):
    """Create fetch chunks with optional checksum."""
    metadata = fileserver_pb2.FileMetadata(
        filename=filename, file_size=len(file_content)
    )
    if checksum:
        metadata.checksum = checksum

    return iter(
        [
            fileserver_pb2.FileChunk(metadata=metadata),
            fileserver_pb2.FileChunk(chunk_data=file_content),
        ]
    )


@pytest.fixture
def upload_test_file(test_cache_dir):
    """Test file for upload tests."""
    local_file = Path(test_cache_dir) / "upload_test.txt"
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_text("test content\n")
    return local_file


@pytest.fixture
def client(file_servers, coordinators, test_cache_dir):
    """Standard client instance for tests."""
    return Client("test-client", coordinators, file_servers, test_cache_dir)


def mock_upload_test(
    stub_responses, jitter=0.0, track_sleep=False, track_connections=False
):
    """Setup mocks for upload tests."""
    stub = MagicMock()
    stub.UploadFile.side_effect = stub_responses

    num_calls = len(stub_responses)
    channels = [MagicMock() for _ in range(num_calls)]

    result = {
        "stub": stub,
        "channels": channels,
    }

    if track_sleep:
        result["slept"] = []

    if track_connections:
        result["connected_servers"] = []

    patches = ExitStack()
    patches.enter_context(patch("random.shuffle", lambda lst: None))

    if track_connections:

        def track_connection(addr):
            result["connected_servers"].append(addr)
            return channels[len(result["connected_servers"]) - 1]

        patches.enter_context(
            patch("grpc.insecure_channel", side_effect=track_connection)
        )
    else:
        patches.enter_context(patch("grpc.insecure_channel", side_effect=channels))

    patches.enter_context(
        patch("generated.fileserver_pb2_grpc.FileServerStub", return_value=stub)
    )
    patches.enter_context(patch("random.uniform", return_value=jitter))

    if track_sleep:
        patches.enter_context(
            patch("time.sleep", side_effect=lambda s: result["slept"].append(s))
        )
    else:
        patches.enter_context(patch("time.sleep"))

    result["patches"] = patches
    return result


def mock_fileserver(fetch_file=None, upload_success=True):
    """Setup mock fileserver for fetch/process/merge tests."""
    stub = MagicMock()
    result = {"stub": stub}

    if fetch_file:
        filename = fetch_file.get("filename", "test.txt")
        content = fetch_file.get("content", b"")
        checksum = fetch_file.get("checksum")
        stub.FetchFile.return_value = create_fetch_chunks(filename, content, checksum)

    if upload_success:
        capture_upload, uploaded_data = create_upload_capturer()
        stub.UploadFile.side_effect = capture_upload
        result["uploaded_data"] = uploaded_data

    channel = MagicMock()
    patches = ExitStack()
    patches.enter_context(patch("grpc.insecure_channel", return_value=channel))
    patches.enter_context(
        patch("generated.fileserver_pb2_grpc.FileServerStub", return_value=stub)
    )

    result["channel"] = channel
    result["patches"] = patches
    return result


def mock_coordinator_stub(client, method_name, response):
    """Mock a coordinator stub method."""
    mock_stub = Mock()
    setattr(mock_stub, method_name, Mock(return_value=response))
    client._get_coordinator_stub = Mock(return_value=mock_stub)
    return mock_stub


def test_client_initialization(client):
    """Test client initializes correctly."""
    assert client.client_id == "test-client"
    assert client.cache_dir.exists()


def test_process_file(client, temp_test_file):
    """Test processing full file."""

    file_content = temp_test_file.read_bytes()
    capture_upload, uploaded_data = create_upload_capturer()

    checksum = hashlib.sha256(file_content).hexdigest()

    fetch_chunks = [
        fileserver_pb2.FileChunk(
            metadata=fileserver_pb2.FileMetadata(
                filename="test_numbers.txt",
                file_size=len(file_content),
                checksum=checksum,
            )
        ),
        fileserver_pb2.FileChunk(chunk_data=file_content),
    ]

    mock_stub = MagicMock()
    mock_stub.FetchFile.return_value = iter(fetch_chunks)
    mock_stub.UploadFile.side_effect = capture_upload

    mock_channel = MagicMock()
    with patch("grpc.insecure_channel", return_value=mock_channel):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            success, output_filename = client.process_file(
                str(temp_test_file.name), "localhost:50052", 0, 16
            )

    result_data = b"".join(
        chunk.chunk_data for chunk in uploaded_data if chunk.HasField("chunk_data")
    )
    result_numbers = [int(line) for line in result_data.decode().strip().split("\n")]

    assert success is True
    assert output_filename == "test_numbers_range_0_16_primes.txt"
    assert result_numbers == [17, 19, 23, 29, 31]
    assert mock_stub.FetchFile.call_count == 1
    assert mock_stub.UploadFile.call_count == 1


def test_request_task_with_mock(client):
    """Test RequestTask RPC with mock coordinator."""

    response = coordinator_pb2.TaskResponse(
        has_task=True,
        task_type=coordinator_pb2.FIND_PRIMES,
        task_id="task_1",
        filename="test.txt",
        start_index=0,
        end_index=100,
        client_task_status=coordinator_pb2.COMPLETED,
    )

    stub = mock_coordinator_stub(client, "RequestTask", response)
    task = stub.RequestTask(coordinator_pb2.TaskRequest(client_id=client.client_id))

    assert task.has_task is True
    assert task.task_type == coordinator_pb2.FIND_PRIMES
    assert task.filename == "test.txt"


def test_update_task_status_with_mock(client):
    """Test UpdateTaskStatus RPC with mock coordinator."""

    response = coordinator_pb2.TaskStatusResponse(acknowledged=True)
    stub = mock_coordinator_stub(client, "UpdateTaskStatus", response)

    request = coordinator_pb2.TaskStatusRequest(
        client_id=client.client_id,
        task_id="task_1",
        client_task_status=coordinator_pb2.COMPLETED,
        output_filename="output.txt",
    )
    result = stub.UpdateTaskStatus(request)

    assert result.acknowledged is True


def test_no_task_available(client):
    """Test handling when coordinator has no tasks."""
    response = coordinator_pb2.TaskResponse(has_task=False)
    mock_stub = mock_coordinator_stub(client, "RequestTask", response)

    request = coordinator_pb2.TaskRequest(client_id=client.client_id)
    task = mock_stub.RequestTask(request)

    assert task.has_task is False


def test_wait_for_dependencies(client):
    """Test handling when coordinator returns WAIT status."""
    response = coordinator_pb2.TaskResponse(
        has_task=True, client_task_status=coordinator_pb2.WAIT
    )
    mock_stub = mock_coordinator_stub(client, "RequestTask", response)

    request = coordinator_pb2.TaskRequest(client_id=client.client_id)
    task = mock_stub.RequestTask(request)

    assert task.has_task is True
    assert task.client_task_status == coordinator_pb2.WAIT


def test_exponential_backoff_on_wait(client):
    """Test exponential backoff when waiting for dependencies."""

    wait_response = coordinator_pb2.TaskResponse(
        has_task=True, client_task_status=coordinator_pb2.WAIT
    )
    task_response = coordinator_pb2.TaskResponse(
        has_task=True,
        task_type=coordinator_pb2.FIND_PRIMES,
        task_id="test_task",
        filename="test.txt",
        start_index=0,
        end_index=10,
        client_task_status=coordinator_pb2.COMPLETED,
    )

    # Mock _get_coordinator_stub to return a mock stub
    mock_stub = Mock()
    mock_stub.RequestTask = Mock(
        side_effect=[wait_response, wait_response, task_response]
    )
    client._get_coordinator_stub = Mock(return_value=mock_stub)

    client.process_file = Mock(return_value=(True, "output.txt"))
    mock_stub.UpdateTaskStatus = Mock(
        return_value=coordinator_pb2.TaskStatusResponse(acknowledged=True)
    )

    sleep_times = []

    def track_sleep(duration):
        sleep_times.append(duration)

    with patch("time.sleep", side_effect=track_sleep):
        base_wait_time = 5
        max_wait_time = 60
        current_wait_time = base_wait_time

        for _ in range(3):
            request = coordinator_pb2.TaskRequest(client_id=client.client_id)
            task = mock_stub.RequestTask(request)

            if task.has_task and task.client_task_status != coordinator_pb2.WAIT:
                client.process_file(
                    task.filename, "localhost:50052", task.start_index, task.end_index
                )
                break
            elif task.client_task_status == coordinator_pb2.WAIT:
                time.sleep(current_wait_time)
                current_wait_time = min(current_wait_time * 2, max_wait_time)

    assert len(sleep_times) == 2
    assert sleep_times[0] == 5
    assert sleep_times[1] == 10
    assert client.process_file.call_count == 1


def test_heartbeat_with_mock(client):
    """Test SendHeartbeat RPC with mock coordinator."""
    response = coordinator_pb2.HeartbeatResponse(acknowledged=True)
    stub = mock_coordinator_stub(client, "SendHeartbeat", response)

    request = coordinator_pb2.HeartbeatRequest(client_id=client.client_id)
    result = stub.SendHeartbeat(request)

    assert result.acknowledged is True


def test_fetch_file_with_mock(client):
    """Test file fetching with mock file server."""
    file_content = b"17\n19\n23\n29\n31\n"
    mocks = mock_fileserver(
        fetch_file={"filename": "test.txt", "content": file_content},
        upload_success=False,
    )

    with mocks["patches"]:
        filepath = client._fetch_file("test.txt")

    assert filepath.exists()
    assert filepath.read_bytes() == file_content


def test_fetch_file_from_cache(client):
    """Test file is served from cache on second request."""
    file_content = b"2\n3\n5\n7\n"
    mocks = mock_fileserver(
        fetch_file={"filename": "test.txt", "content": file_content},
        upload_success=False,
    )

    with mocks["patches"]:
        filepath1 = client._fetch_file("test.txt")
        filepath2 = client._fetch_file("test.txt")

    assert filepath1 == filepath2
    assert mocks["stub"].FetchFile.call_count == 1


def test_process_file_partial_range(client, temp_test_file):
    """Test processing partial line range."""
    file_content = temp_test_file.read_bytes()
    capture_upload, uploaded_data = create_upload_capturer()

    fetch_chunks = [
        fileserver_pb2.FileChunk(
            metadata=fileserver_pb2.FileMetadata(
                filename="test_numbers.txt", file_size=len(file_content)
            )
        ),
        fileserver_pb2.FileChunk(chunk_data=file_content),
    ]

    mock_stub = MagicMock()
    mock_stub.FetchFile.return_value = iter(fetch_chunks)
    mock_stub.UploadFile.side_effect = capture_upload

    mock_channel = MagicMock()
    with patch("grpc.insecure_channel", return_value=mock_channel):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            success, output_filename = client.process_file(
                str(temp_test_file.name), "localhost:50052", 5, 10
            )

    result_data = b"".join(
        chunk.chunk_data for chunk in uploaded_data if chunk.HasField("chunk_data")
    )
    result_numbers = [int(line) for line in result_data.decode().strip().split("\n")]

    assert success is True
    assert output_filename == "test_numbers_range_5_10_primes.txt"
    assert result_numbers == [23]
    assert mock_stub.FetchFile.call_count == 1
    assert mock_stub.UploadFile.call_count == 1


def test_merge_files(client):
    """Test merging two prime files."""
    capture_upload, uploaded_data = create_upload_capturer()

    file1_content = b"2\n3\n5\n7\n"
    fetch_chunks1 = [
        fileserver_pb2.FileChunk(
            metadata=fileserver_pb2.FileMetadata(
                filename="file1_primes.txt", file_size=len(file1_content)
            )
        ),
        fileserver_pb2.FileChunk(chunk_data=file1_content),
    ]

    file2_content = b"5\n7\n11\n13\n"
    fetch_chunks2 = [
        fileserver_pb2.FileChunk(
            metadata=fileserver_pb2.FileMetadata(
                filename="file2_primes.txt", file_size=len(file2_content)
            )
        ),
        fileserver_pb2.FileChunk(chunk_data=file2_content),
    ]

    mock_stub = MagicMock()
    mock_stub.FetchFile.side_effect = [iter(fetch_chunks1), iter(fetch_chunks2)]
    mock_stub.UploadFile.side_effect = capture_upload

    mock_channel = MagicMock()
    with patch("grpc.insecure_channel", return_value=mock_channel):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            success, output_filename = client.merge_files(
                "file1_primes.txt", "file2_primes.txt", "task123", "localhost:50052"
            )

    result_data = b"".join(
        chunk.chunk_data for chunk in uploaded_data if chunk.HasField("chunk_data")
    )
    result_numbers = [int(line) for line in result_data.decode().strip().split("\n")]

    assert success is True
    assert output_filename == "merged_task123.txt"
    assert result_numbers == [2, 3, 5, 7, 11, 13]
    assert mock_stub.FetchFile.call_count == 2
    assert mock_stub.UploadFile.call_count == 1


def test_upload_includes_checksum(client, temp_test_file):
    """Test that upload includes checksum."""
    file_content = temp_test_file.read_bytes()
    capture_upload, uploaded_data = create_upload_capturer()

    fetch_chunks = [
        fileserver_pb2.FileChunk(
            metadata=fileserver_pb2.FileMetadata(
                filename="test_numbers.txt", file_size=len(file_content)
            )
        ),
        fileserver_pb2.FileChunk(chunk_data=file_content),
    ]

    mock_stub = MagicMock()
    mock_stub.FetchFile.return_value = iter(fetch_chunks)
    mock_stub.UploadFile.side_effect = capture_upload

    mock_channel = MagicMock()
    with patch("grpc.insecure_channel", return_value=mock_channel):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            success, output_filename = client.process_file(
                str(temp_test_file.name), "localhost:50052", 0, 16
            )

    # Extract metadata from uploaded chunks
    metadata = None
    for chunk in uploaded_data:
        if chunk.HasField("metadata"):
            metadata = chunk.metadata
            break

    assert success is True
    assert output_filename == "test_numbers_range_0_16_primes.txt"
    assert metadata is not None
    assert metadata.checksum != ""
    assert len(metadata.checksum) == 64  # SHA-256 produces 64 hex chars


def test_fetch_verifies_checksum(client, temp_test_file):
    """Test that fetch verifies checksum from file server."""
    file_content = temp_test_file.read_bytes()

    # Compute correct checksum
    sha256 = hashlib.sha256()
    sha256.update(file_content)
    correct_checksum = sha256.hexdigest()

    mock_stub = MagicMock()
    mock_stub.FetchFile.return_value = create_fetch_chunks(
        "test_numbers.txt", file_content, correct_checksum
    )

    with patch("grpc.insecure_channel"):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            # Should succeed with correct checksum
            cached_path = client._fetch_file("test_numbers.txt")

    assert cached_path.exists()
    assert cached_path.read_bytes() == file_content


def test_fetch_failover_on_server_error(client, temp_test_file):
    """Test that fetch fails over to next server when first server fails."""
    file_content = temp_test_file.read_bytes()

    # Compute correct checksum
    sha256 = hashlib.sha256()
    sha256.update(file_content)
    correct_checksum = sha256.hexdigest()

    # First call fails, second call succeeds
    mock_stub = MagicMock()
    mock_stub.FetchFile.side_effect = [
        Exception("Connection refused"),
        create_fetch_chunks("test_numbers.txt", file_content, correct_checksum),
    ]

    with patch("grpc.insecure_channel"):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            cached_path = client._fetch_file("test_numbers.txt")

    assert cached_path.exists()
    assert cached_path.read_bytes() == file_content
    assert mock_stub.FetchFile.call_count == 2


def test_fetch_detects_checksum_mismatch(client, temp_test_file):
    """Test that fetch detects checksum mismatch and raises error."""
    file_content = temp_test_file.read_bytes()

    # Use wrong checksum
    wrong_checksum = "a" * 64

    mock_stub = MagicMock()
    mock_stub.FetchFile.side_effect = lambda _: create_fetch_chunks(
        "test_numbers.txt", file_content, wrong_checksum
    )

    with patch("grpc.insecure_channel"):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            # Should raise RuntimeError after all servers fail checksum
            try:
                client._fetch_file("test_numbers.txt")
                assert False, "Expected RuntimeError for checksum mismatch"
            except RuntimeError as e:
                assert "Failed to fetch" in str(e)
                assert "from all file servers" in str(e)


def test_tmp_cleanup_on_startup(test_cache_dir):
    """Test that orphaned .tmp files are cleaned up on client initialization."""
    # Create client-specific cache directory with orphaned .tmp files
    client_cache_path = Path(test_cache_dir) / "test-client"
    client_cache_path.mkdir(parents=True, exist_ok=True)

    tmp1 = client_cache_path / "orphaned1.txt.tmp"
    tmp2 = client_cache_path / "orphaned2.txt.tmp"
    tmp1.write_text("partial data")
    tmp2.write_text("more partial data")

    assert tmp1.exists()
    assert tmp2.exists()

    # Initialize client to trigger cleanup
    Client(
        "test-client",
        ["localhost:50051", "localhost:50052"],
        ["localhost:50052"],
        test_cache_dir,
    )

    assert not tmp1.exists()
    assert not tmp2.exists()


def test_no_tmp_files_after_successful_fetch(client, test_cache_dir):
    """Test that .tmp files are removed after successful fetch."""
    file_content = b"2\n3\n5\n7\n"
    checksum = hashlib.sha256(file_content).hexdigest()

    fetch_chunks = [
        fileserver_pb2.FileChunk(
            metadata=fileserver_pb2.FileMetadata(
                filename="test.txt", file_size=len(file_content), checksum=checksum
            )
        ),
        fileserver_pb2.FileChunk(chunk_data=file_content),
    ]

    mock_stub = MagicMock()
    mock_stub.FetchFile.return_value = iter(fetch_chunks)

    mock_channel = MagicMock()
    with patch("grpc.insecure_channel", return_value=mock_channel):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            filepath = client._fetch_file("test.txt")

    assert filepath.exists()

    # Check no .tmp file exists
    cache_path = Path(test_cache_dir)
    tmp_files = list(cache_path.glob("*.tmp"))
    assert len(tmp_files) == 0


def test_files_persist_after_upload(client, temp_test_file, test_cache_dir):
    """Test that result files stay on disk after successful upload."""
    file_content = temp_test_file.read_bytes()
    capture_upload, _ = create_upload_capturer()

    checksum = hashlib.sha256(file_content).hexdigest()

    fetch_chunks = [
        fileserver_pb2.FileChunk(
            metadata=fileserver_pb2.FileMetadata(
                filename="test_numbers.txt",
                file_size=len(file_content),
                checksum=checksum,
            )
        ),
        fileserver_pb2.FileChunk(chunk_data=file_content),
    ]

    mock_stub = MagicMock()
    mock_stub.FetchFile.return_value = iter(fetch_chunks)
    mock_stub.UploadFile.side_effect = capture_upload

    mock_channel = MagicMock()
    with patch("grpc.insecure_channel", return_value=mock_channel):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            success, output_filename = client.process_file(
                str(temp_test_file.name), "localhost:50052", 0, 16
            )

    assert success is True
    assert output_filename == "test_numbers_range_0_16_primes.txt"

    # Check that result file still exists on disk in client-specific directory
    client_cache_path = Path(test_cache_dir) / "test-client"
    result_file = client_cache_path / "test_numbers_range_0_16_primes.txt"
    assert result_file.exists()

    # Verify no .tmp files left behind
    tmp_files = list(client_cache_path.glob("*.tmp"))
    assert len(tmp_files) == 0


def test_process_file_cleans_tmp_on_error(client, temp_test_file, test_cache_dir):
    """Test that .tmp files are cleaned up when process_file upload fails."""
    file_content = temp_test_file.read_bytes()
    checksum = hashlib.sha256(file_content).hexdigest()

    fetch_chunks = [
        fileserver_pb2.FileChunk(
            metadata=fileserver_pb2.FileMetadata(
                filename="test_numbers.txt",
                file_size=len(file_content),
                checksum=checksum,
            )
        ),
        fileserver_pb2.FileChunk(chunk_data=file_content),
    ]

    mock_stub = MagicMock()
    mock_stub.FetchFile.return_value = iter(fetch_chunks)

    mock_channel = MagicMock()
    with patch("grpc.insecure_channel", return_value=mock_channel):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            # Mock upload to fail
            with patch.object(client, "_upload_file", return_value=False):
                success, output_filename = client.process_file(
                    str(temp_test_file.name), "localhost:50052", 0, 16
                )

    assert success is False
    assert output_filename == "test_numbers_range_0_16_primes.txt"

    # No .tmp files should remain
    cache_path = Path(test_cache_dir)
    tmp_files = list(cache_path.glob("*.tmp"))
    assert len(tmp_files) == 0


def test_merge_files_cleans_tmp_on_error(client, test_cache_dir):
    """Test that .tmp files are cleaned up when merge_files upload fails."""
    file1_content = b"2\n3\n5\n"
    file2_content = b"7\n11\n13\n"

    checksum1 = hashlib.sha256(file1_content).hexdigest()
    checksum2 = hashlib.sha256(file2_content).hexdigest()

    fetch_chunks1 = [
        fileserver_pb2.FileChunk(
            metadata=fileserver_pb2.FileMetadata(
                filename="file1_primes.txt",
                file_size=len(file1_content),
                checksum=checksum1,
            )
        ),
        fileserver_pb2.FileChunk(chunk_data=file1_content),
    ]

    fetch_chunks2 = [
        fileserver_pb2.FileChunk(
            metadata=fileserver_pb2.FileMetadata(
                filename="file2_primes.txt",
                file_size=len(file2_content),
                checksum=checksum2,
            )
        ),
        fileserver_pb2.FileChunk(chunk_data=file2_content),
    ]

    mock_stub = MagicMock()
    mock_stub.FetchFile.side_effect = [iter(fetch_chunks1), iter(fetch_chunks2)]

    mock_channel = MagicMock()
    with patch("grpc.insecure_channel", return_value=mock_channel):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            # Mock upload to fail
            with patch.object(client, "_upload_file", return_value=False):
                success, output_filename = client.merge_files(
                    "file1_primes.txt", "file2_primes.txt", "task123", "localhost:50052"
                )

    assert success is False
    assert output_filename == "merged_task123.txt"

    # No .tmp files should remain
    cache_path = Path(test_cache_dir)
    tmp_files = list(cache_path.glob("*.tmp"))
    assert len(tmp_files) == 0


def test_upload_success_on_first_attempt(client, upload_test_file):
    """Test upload succeeds immediately on first try."""
    mocks = mock_upload_test([fileserver_pb2.UploadResponse(success=True)])

    with mocks["patches"]:
        result = client._upload_file(upload_test_file, "remote.txt", "localhost:50052")

    assert result is True
    assert mocks["stub"].UploadFile.call_count == 1
    mocks["channels"][0].close.assert_called_once()


def test_upload_retries_on_failure(upload_test_file, coordinators, test_cache_dir):
    """Test upload retries with backoff when server returns failure."""
    multi_servers = ["localhost:50052", "localhost:50053"]
    client = Client("test-client", coordinators, multi_servers, test_cache_dir)

    mocks = mock_upload_test(
        [
            fileserver_pb2.UploadResponse(success=False, error_message="busy"),
            fileserver_pb2.UploadResponse(success=True),
        ],
        jitter=0.3,
        track_sleep=True,
    )

    with mocks["patches"]:
        result = client._upload_file(upload_test_file, "remote.txt", "localhost:50052")

    assert result is True
    assert mocks["stub"].UploadFile.call_count == 2
    assert len(mocks["slept"]) == 1
    assert mocks["slept"][0] == pytest.approx(1.3)


def test_upload_rotates_servers_on_failure(
    upload_test_file, coordinators, test_cache_dir
):
    """Test upload rotates through servers after failures."""
    multi_servers = ["localhost:50052", "localhost:50053", "localhost:50054"]
    client = Client("test-client", coordinators, multi_servers, test_cache_dir)

    mocks = mock_upload_test(
        [
            fileserver_pb2.UploadResponse(success=False, error_message="error1"),
            fileserver_pb2.UploadResponse(success=False, error_message="error2"),
            fileserver_pb2.UploadResponse(success=True),
        ],
        track_connections=True,
    )

    with mocks["patches"]:
        result = client._upload_file(upload_test_file, "remote.txt", "localhost:50052")

    assert result is True
    assert mocks["stub"].UploadFile.call_count == 3
    assert mocks["connected_servers"][0] == "localhost:50052"
    assert mocks["connected_servers"][1] == "localhost:50053"
    assert mocks["connected_servers"][2] == "localhost:50054"


def test_upload_handles_rpc_error(upload_test_file, coordinators, test_cache_dir):
    """Test upload handles RPC errors and retries."""

    class MockRpcError(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.UNAVAILABLE

    multi_servers = ["localhost:50052", "localhost:50053"]
    client = Client("test-client", coordinators, multi_servers, test_cache_dir)

    mocks = mock_upload_test(
        [MockRpcError(), fileserver_pb2.UploadResponse(success=True)]
    )

    with mocks["patches"]:
        result = client._upload_file(upload_test_file, "remote.txt", "localhost:50052")

    assert result is True
    assert mocks["stub"].UploadFile.call_count == 2
    assert mocks["channels"][0].close.call_count == 1
    assert mocks["channels"][1].close.call_count == 1


def test_upload_handles_generic_exception(
    upload_test_file, coordinators, test_cache_dir
):
    """Test upload handles generic exceptions and retries."""
    multi_servers = ["localhost:50052", "localhost:50053"]
    client = Client("test-client", coordinators, multi_servers, test_cache_dir)

    mocks = mock_upload_test(
        [Exception("network error"), fileserver_pb2.UploadResponse(success=True)]
    )

    with mocks["patches"]:
        result = client._upload_file(upload_test_file, "remote.txt", "localhost:50052")

    assert result is True
    assert mocks["stub"].UploadFile.call_count == 2
    assert mocks["channels"][0].close.call_count == 1
    assert mocks["channels"][1].close.call_count == 1


def test_upload_backoff_caps_at_30_seconds(
    upload_test_file, coordinators, test_cache_dir
):
    """Test upload backoff caps at 30 seconds max."""
    client = Client(
        "test-client",
        coordinators,
        ["localhost:50052"],
        test_cache_dir,
    )

    mocks = mock_upload_test(
        [
            fileserver_pb2.UploadResponse(success=False, error_message="error"),
            fileserver_pb2.UploadResponse(success=False, error_message="error"),
            fileserver_pb2.UploadResponse(success=False, error_message="error"),
            fileserver_pb2.UploadResponse(success=False, error_message="error"),
            fileserver_pb2.UploadResponse(success=False, error_message="error"),
            fileserver_pb2.UploadResponse(success=False, error_message="error"),
            fileserver_pb2.UploadResponse(success=True),
        ],
        track_sleep=True,
    )

    with mocks["patches"]:
        result = client._upload_file(upload_test_file, "remote.txt", "localhost:50052")

    assert result is True
    assert len(mocks["slept"]) == 6
    assert mocks["slept"][0] == pytest.approx(1.0)
    assert mocks["slept"][1] == pytest.approx(2.0)
    assert mocks["slept"][2] == pytest.approx(4.0)
    assert mocks["slept"][3] == pytest.approx(8.0)
    assert mocks["slept"][4] == pytest.approx(16.0)
    assert mocks["slept"][5] == pytest.approx(30.0)


def test_upload_resets_backoff_on_server_switch(
    upload_test_file, coordinators, test_cache_dir
):
    """Test upload resets backoff when switching to a different server."""
    multi_servers = ["localhost:50052", "localhost:50053"]
    client = Client("test-client", coordinators, multi_servers, test_cache_dir)

    mocks = mock_upload_test(
        [
            fileserver_pb2.UploadResponse(success=False, error_message="error"),
            fileserver_pb2.UploadResponse(success=False, error_message="error"),
            fileserver_pb2.UploadResponse(success=True),
        ],
        track_sleep=True,
    )

    with mocks["patches"]:
        result = client._upload_file(upload_test_file, "remote.txt", "localhost:50052")

    assert result is True
    assert len(mocks["slept"]) == 2
    assert mocks["slept"][0] == pytest.approx(1.0)


def test_next_coordinator_rotation(client):
    """Test coordinator rotation cycles through all coordinators."""
    coords = ["coord1:5000", "coord2:5000", "coord3:5000"]
    client.coordinator_addresses = coords

    assert client._next_coordinator("coord1:5000") == "coord2:5000"
    assert client._next_coordinator("coord2:5000") == "coord3:5000"
    assert client._next_coordinator("coord3:5000") == "coord1:5000"


def test_next_coordinator_returns_first_when_current_not_found(client):
    """Test coordinator rotation returns first when current not in list."""
    coords = ["coord1:5000", "coord2:5000"]
    client.coordinator_addresses = coords

    assert client._next_coordinator("unknown:5000") == "coord1:5000"


def test_next_coordinator_raises_when_no_coordinators(client):
    """Test coordinator rotation raises error when no coordinators configured."""
    client.coordinator_addresses = []

    with pytest.raises(RuntimeError, match="No coordinator addresses configured"):
        client._next_coordinator("coord1:5000")


def test_coordinator_stub_caching(client):
    """Test coordinator stub is cached and reused."""
    addr = "localhost:50051"

    stub1 = client._get_coordinator_stub(addr)
    stub2 = client._get_coordinator_stub(addr)

    assert stub1 is stub2
    assert addr in client._coord_stubs
    assert addr in client._coord_channels


def test_close_coordinator_channels(client):
    """Test closing coordinator channels clears cache."""
    client._get_coordinator_stub("localhost:50051")
    client._get_coordinator_stub("localhost:50052")

    assert len(client._coord_stubs) == 2
    assert len(client._coord_channels) == 2

    client._close_coordinator_channels()

    assert len(client._coord_stubs) == 0
    assert len(client._coord_channels) == 0


def test_heartbeat_to_multiple_coordinators():
    """Test heartbeat threads are started for all coordinators."""
    coords = ["localhost:50051", "localhost:50052", "localhost:50053"]
    client = Client(
        "test-client",
        coords,
        ["localhost:60001"],
        "/tmp/test_cache",
    )

    for addr in coords:
        client._start_heartbeat(addr)

    assert len(client.heartbeat_threads) == 3
    for addr in coords:
        assert addr in client.heartbeat_threads
        assert client.heartbeat_threads[addr].is_alive()

    client._stop_heartbeat()
    assert len(client.heartbeat_threads) == 0


def test_coordinator_failover_on_rpc_error():
    """Test client switches coordinator on RPC error."""
    coords = ["localhost:50051", "localhost:50052"]
    client = Client(
        "test-client",
        coords,
        ["localhost:60001"],
        "/tmp/test_cache",
    )

    # Mock coordinator stubs
    stub1 = Mock()
    stub2 = Mock()

    # First coordinator raises RPC error, second succeeds
    stub1.RequestTask.side_effect = grpc.RpcError("Connection failed")
    stub2.RequestTask.return_value = coordinator_pb2.TaskResponse(
        has_task=False, client_task_status=coordinator_pb2.WAIT
    )

    call_count = [0]

    def get_stub(addr):
        call_count[0] += 1
        if addr == "localhost:50051":
            return stub1
        return stub2

    client._get_coordinator_stub = get_stub

    # Mock heartbeat and sleep to avoid delays
    client._start_heartbeat = Mock()
    client._stop_heartbeat = Mock()

    with patch("time.sleep"):
        with patch("random.uniform", return_value=0.0):
            try:
                # Run will try first coordinator, get RPC error, switch to second
                client.run()
            except grpc.RpcError:
                pass

    # Should have tried both coordinators
    assert call_count[0] >= 2
    stub1.RequestTask.assert_called()
    stub2.RequestTask.assert_called()


def test_stream_fetch_overwrites_existing_file(client, test_cache_dir):
    """Test stream fetch can overwrite existing file atomically."""
    client_cache_path = Path(test_cache_dir) / "test-client"
    existing_file = client_cache_path / "test_data.txt"
    existing_file.write_text("old data from previous fetch")

    new_content = b"new data"
    chunks = create_fetch_chunks("test_data.txt", new_content, checksum="new_checksum")

    checksum = client._stream_fetch(chunks, existing_file)

    assert existing_file.exists()
    assert existing_file.read_text() == "new data"
    assert checksum == "new_checksum"

    tmp_file = Path(str(existing_file) + ".tmp")
    assert not tmp_file.exists()


def test_process_file_overwrites_existing_result(client, test_cache_dir):
    """Test process file can overwrite existing result file atomically."""
    client_cache_path = Path(test_cache_dir) / "test-client"
    result_filename = "test_numbers_range_0_16_primes.txt"
    existing_result = client_cache_path / result_filename
    existing_result.write_text("old primes from previous run")

    test_content = b"2\n3\n5\n7\n11\n13\n17\n19\n23\n29\n31\n37\n41\n43\n47\n53\n"
    mock = mock_fileserver(
        fetch_file={"filename": "test_numbers.txt", "content": test_content, "checksum": "abc123"},
        upload_success=True
    )

    with mock["patches"]:
        success, result = client.process_file(
            "test_numbers.txt", "localhost:50052", 0, 16
        )

    assert success
    assert result == result_filename
    assert existing_result.exists()
    assert "old primes from previous run" not in existing_result.read_text()

    tmp_file = Path(str(existing_result) + ".tmp")
    assert not tmp_file.exists()


def test_merge_files_overwrites_existing_result(client, test_cache_dir):
    """Test merge files can overwrite existing merged file atomically."""
    client_cache_path = Path(test_cache_dir) / "test-client"

    file1 = client_cache_path / "primes1.txt"
    file2 = client_cache_path / "primes2.txt"
    file1.write_text("2\n3\n5\n")
    file2.write_text("7\n11\n")

    merged_filename = "merged_task123.txt"
    existing_merged = client_cache_path / merged_filename
    existing_merged.write_text("old merged data")

    mock = mock_fileserver(upload_success=True)

    with mock["patches"]:
        success = client.merge_files(
            "primes1.txt", "primes2.txt", "task123", "localhost:50052"
        )

    assert success
    assert existing_merged.exists()
    assert "old merged data" not in existing_merged.read_text()

    tmp_file = Path(str(existing_merged) + ".tmp")
    assert not tmp_file.exists()
