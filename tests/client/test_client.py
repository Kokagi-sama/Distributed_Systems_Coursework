import hashlib
import time
import pytest
from unittest.mock import Mock, patch, MagicMock
from client.client import Client
from generated import coordinator_pb2
from generated import fileserver_pb2


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
def test_cache_dir(tmp_path):
    """Unique cache directory for each test."""
    return str(tmp_path / "client_cache")


def create_upload_capturer():
    """Create a function that captures uploaded chunks."""
    uploaded_data = []

    def capture_upload(chunk_iterator):
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


def test_client_initialization(file_servers, test_cache_dir):
    """Test client initializes correctly."""
    client = Client("test-client", "localhost:50051", file_servers, test_cache_dir)
    assert client.client_id == "test-client"
    assert client.cache_dir.exists()


def test_process_file(temp_test_file, file_servers, test_cache_dir):
    """Test processing full file."""
    client = Client("test-client", "localhost:50051", file_servers, test_cache_dir)

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

    with patch("grpc.insecure_channel"):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            success = client.process_file(
                str(temp_test_file.name), "localhost:50052", 0, 16
            )

    result_data = b"".join(
        chunk.chunk_data for chunk in uploaded_data if chunk.HasField("chunk_data")
    )
    result_numbers = [int(line) for line in result_data.decode().strip().split("\n")]

    assert success is True
    assert result_numbers == [17, 19, 23, 29, 31]
    assert mock_stub.FetchFile.call_count == 1
    assert mock_stub.UploadFile.call_count == 1


def test_request_task_with_mock(file_servers, test_cache_dir):
    """Test RequestTask RPC with mock coordinator."""
    client = Client("test-client", "localhost:50051", file_servers, test_cache_dir)

    mock_response = coordinator_pb2.TaskResponse(
        status=coordinator_pb2.TASK_AVAILABLE,
        task_type=coordinator_pb2.FIND_PRIMES,
        filename="test.txt",
        primary_fileserver_addr="localhost:50052",
    )
    client.coordinator.RequestTask = Mock(return_value=mock_response)

    request = coordinator_pb2.TaskRequest(client_id=client.client_id)
    task = client.coordinator.RequestTask(request)

    assert task.status == coordinator_pb2.TASK_AVAILABLE
    assert task.task_type == coordinator_pb2.FIND_PRIMES
    assert task.filename == "test.txt"
    assert task.primary_fileserver_addr == "localhost:50052"


def test_complete_task_with_mock(file_servers, test_cache_dir):
    """Test CompleteTask RPC with mock coordinator."""
    client = Client("test-client", "localhost:50051", file_servers, test_cache_dir)

    mock_response = coordinator_pb2.TaskCompletionResponse(acknowledged=True)
    client.coordinator.CompleteTask = Mock(return_value=mock_response)

    completion_request = coordinator_pb2.TaskCompletionRequest(
        client_id=client.client_id, task_id="test.txt"
    )
    response = client.coordinator.CompleteTask(completion_request)

    assert response.acknowledged is True


def test_no_task_available(file_servers, test_cache_dir):
    """Test handling when coordinator has no tasks."""
    client = Client("test-client", "localhost:50051", file_servers, test_cache_dir)

    mock_response = coordinator_pb2.TaskResponse(
        status=coordinator_pb2.NO_TASK_AVAILABLE
    )
    client.coordinator.RequestTask = Mock(return_value=mock_response)

    request = coordinator_pb2.TaskRequest(client_id=client.client_id)
    task = client.coordinator.RequestTask(request)

    assert task.status == coordinator_pb2.NO_TASK_AVAILABLE


def test_wait_for_dependencies(file_servers, test_cache_dir):
    """Test handling when coordinator returns WAIT_FOR_DEPENDENCIES."""
    client = Client("test-client", "localhost:50051", file_servers, test_cache_dir)

    mock_response = coordinator_pb2.TaskResponse(
        status=coordinator_pb2.WAIT_FOR_DEPENDENCIES
    )
    client.coordinator.RequestTask = Mock(return_value=mock_response)

    request = coordinator_pb2.TaskRequest(client_id=client.client_id)
    task = client.coordinator.RequestTask(request)

    assert task.status == coordinator_pb2.WAIT_FOR_DEPENDENCIES


def test_exponential_backoff_on_wait(file_servers, test_cache_dir):
    """Test exponential backoff when waiting for dependencies."""
    client = Client("test-client", "localhost:50051", file_servers, test_cache_dir)

    wait_response = coordinator_pb2.TaskResponse(
        status=coordinator_pb2.WAIT_FOR_DEPENDENCIES
    )
    task_response = coordinator_pb2.TaskResponse(
        status=coordinator_pb2.TASK_AVAILABLE,
        task_type=coordinator_pb2.FIND_PRIMES,
        task_id="test_task",
        filename="test.txt",
        start_index=0,
        end_index=10,
        primary_fileserver_addr="localhost:50052",
    )

    client.coordinator.RequestTask = Mock(
        side_effect=[wait_response, wait_response, task_response]
    )
    client.process_file = Mock(return_value=True)
    client.coordinator.CompleteTask = Mock(
        return_value=coordinator_pb2.TaskCompletionResponse(acknowledged=True)
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
            task = client.coordinator.RequestTask(request)

            if task.status == coordinator_pb2.TASK_AVAILABLE:
                client.process_file(
                    task.filename,
                    task.primary_fileserver_addr,
                    task.start_index,
                    task.end_index,
                )
                break
            elif task.status == coordinator_pb2.WAIT_FOR_DEPENDENCIES:
                time.sleep(current_wait_time)
                current_wait_time = min(current_wait_time * 2, max_wait_time)

    assert len(sleep_times) == 2
    assert sleep_times[0] == 5
    assert sleep_times[1] == 10
    assert client.process_file.call_count == 1


def test_heartbeat_with_mock(file_servers, test_cache_dir):
    """Test SendHeartbeat RPC with mock coordinator."""
    client = Client("test-client", "localhost:50051", file_servers, test_cache_dir)

    mock_response = coordinator_pb2.HeartbeatResponse(acknowledged=True)
    client.coordinator.SendHeartbeat = Mock(return_value=mock_response)

    request = coordinator_pb2.HeartbeatRequest(client_id=client.client_id)
    response = client.coordinator.SendHeartbeat(request)

    assert response.acknowledged is True


def test_fetch_file_with_mock(file_servers, test_cache_dir):
    """Test file fetching with mock file server."""
    client = Client("test-client", "localhost:50051", file_servers, test_cache_dir)

    file_content = b"17\n19\n23\n29\n31\n"
    fetch_chunks = [
        fileserver_pb2.FileChunk(
            metadata=fileserver_pb2.FileMetadata(
                filename="test.txt", file_size=len(file_content)
            )
        ),
        fileserver_pb2.FileChunk(chunk_data=file_content),
    ]
    mock_stub = MagicMock()
    mock_stub.FetchFile.return_value = iter(fetch_chunks)

    with patch("grpc.insecure_channel"):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            filepath = client._fetch_file("test.txt")

    assert filepath.exists()
    assert filepath.read_bytes() == file_content


def test_fetch_file_from_cache(file_servers, test_cache_dir):
    """Test file is served from cache on second request."""
    client = Client("test-client", "localhost:50051", file_servers, test_cache_dir)

    file_content = b"2\n3\n5\n7\n"
    fetch_chunks = [
        fileserver_pb2.FileChunk(
            metadata=fileserver_pb2.FileMetadata(
                filename="test.txt", file_size=len(file_content)
            )
        ),
        fileserver_pb2.FileChunk(chunk_data=file_content),
    ]
    mock_stub = MagicMock()
    mock_stub.FetchFile.return_value = iter(fetch_chunks)

    with patch("grpc.insecure_channel"):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            filepath1 = client._fetch_file("test.txt")
            filepath2 = client._fetch_file("test.txt")

    assert filepath1 == filepath2
    assert mock_stub.FetchFile.call_count == 1


def test_process_file_partial_range(temp_test_file, file_servers, test_cache_dir):
    """Test processing partial line range."""
    client = Client("test-client", "localhost:50051", file_servers, test_cache_dir)

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

    with patch("grpc.insecure_channel"):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            success = client.process_file(
                str(temp_test_file.name), "localhost:50052", 5, 10
            )

    result_data = b"".join(
        chunk.chunk_data for chunk in uploaded_data if chunk.HasField("chunk_data")
    )
    result_numbers = [int(line) for line in result_data.decode().strip().split("\n")]

    assert success is True
    assert result_numbers == [23]
    assert mock_stub.FetchFile.call_count == 1
    assert mock_stub.UploadFile.call_count == 1


def test_merge_files(file_servers, test_cache_dir):
    """Test merging two prime files."""
    client = Client("test-client", "localhost:50051", file_servers, test_cache_dir)

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

    with patch("grpc.insecure_channel"):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            success = client.merge_files(
                "file1_primes.txt", "file2_primes.txt", "task123", "localhost:50052"
            )

    result_data = b"".join(
        chunk.chunk_data for chunk in uploaded_data if chunk.HasField("chunk_data")
    )
    result_numbers = [int(line) for line in result_data.decode().strip().split("\n")]

    assert success is True
    assert result_numbers == [2, 3, 5, 7, 11, 13]
    assert mock_stub.FetchFile.call_count == 2
    assert mock_stub.UploadFile.call_count == 1


def test_upload_includes_checksum(temp_test_file, file_servers, test_cache_dir):
    """Test that upload includes checksum and atomic_upload flag."""
    client = Client("test-client", "localhost:50051", file_servers, test_cache_dir)

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

    with patch("grpc.insecure_channel"):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            success = client.process_file(
                str(temp_test_file.name), "localhost:50052", 0, 16
            )

    # Extract metadata from uploaded chunks
    metadata = None
    for chunk in uploaded_data:
        if chunk.HasField("metadata"):
            metadata = chunk.metadata
            break

    assert success is True
    assert metadata is not None
    assert metadata.atomic_upload is True
    assert metadata.checksum != ""
    assert len(metadata.checksum) == 64  # SHA-256 produces 64 hex chars


def test_fetch_verifies_checksum(temp_test_file, file_servers, test_cache_dir):
    """Test that fetch verifies checksum from file server."""
    client = Client("test-client", "localhost:50051", file_servers, test_cache_dir)

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


def test_fetch_failover_on_server_error(temp_test_file, file_servers, test_cache_dir):
    """Test that fetch fails over to next server when first server fails."""
    client = Client("test-client", "localhost:50051", file_servers, test_cache_dir)

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


def test_fetch_detects_checksum_mismatch(temp_test_file, file_servers, test_cache_dir):
    """Test that fetch detects checksum mismatch and raises error."""
    client = Client("test-client", "localhost:50051", file_servers, test_cache_dir)

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
    from pathlib import Path

    # Create cache directory with orphaned .tmp files
    cache_path = Path(test_cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)

    tmp1 = cache_path / "orphaned1.txt.tmp"
    tmp2 = cache_path / "orphaned2.txt.tmp"
    tmp1.write_text("partial data")
    tmp2.write_text("more partial data")

    assert tmp1.exists()
    assert tmp2.exists()

    # Initialize client to trigger cleanup
    Client("test-client", "localhost:50051", ["localhost:50052"], test_cache_dir)

    assert not tmp1.exists()
    assert not tmp2.exists()


def test_no_tmp_files_after_successful_fetch(file_servers, test_cache_dir):
    """Test that .tmp files are removed after successful fetch."""
    from pathlib import Path

    client = Client("test-client", "localhost:50051", file_servers, test_cache_dir)

    file_content = b"2\n3\n5\n7\n"
    fetch_chunks = [
        fileserver_pb2.FileChunk(
            metadata=fileserver_pb2.FileMetadata(
                filename="test.txt", file_size=len(file_content)
            )
        ),
        fileserver_pb2.FileChunk(chunk_data=file_content),
    ]

    mock_stub = MagicMock()
    mock_stub.FetchFile.return_value = iter(fetch_chunks)

    with patch("grpc.insecure_channel"):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            filepath = client._fetch_file("test.txt")

    assert filepath.exists()

    # Check no .tmp file exists
    cache_path = Path(test_cache_dir)
    tmp_files = list(cache_path.glob("*.tmp"))
    assert len(tmp_files) == 0


def test_files_persist_after_upload(temp_test_file, file_servers, test_cache_dir):
    """Test that result files stay on disk after successful upload."""
    from pathlib import Path

    client = Client("test-client", "localhost:50051", file_servers, test_cache_dir)

    file_content = temp_test_file.read_bytes()
    capture_upload, _ = create_upload_capturer()

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

    with patch("grpc.insecure_channel"):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            success = client.process_file(
                str(temp_test_file.name), "localhost:50052", 0, 16
            )

    assert success is True

    # Check that result file still exists on disk
    cache_path = Path(test_cache_dir)
    result_file = cache_path / "test_numbers_range_0_16_primes.txt"
    assert result_file.exists()

    # Verify no .tmp files left behind
    tmp_files = list(cache_path.glob("*.tmp"))
    assert len(tmp_files) == 0
