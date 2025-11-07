import hashlib
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


def create_upload_capturer():
    """Create a function that captures uploaded chunks."""
    uploaded_data = []

    def capture_upload(chunk_iterator):
        for chunk in chunk_iterator:
            uploaded_data.append(chunk)
        return fileserver_pb2.UploadResponse(success=True, error_message="")

    return capture_upload, uploaded_data


def test_client_initialization():
    """Test client initializes correctly."""
    client = Client("test-client", "localhost:50051")
    assert client.client_id == "test-client"
    assert client.cache_dir.exists()


def test_process_file(temp_test_file):
    """Test processing full file."""
    client = Client("test-client", "localhost:50051")

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


def test_request_task_with_mock():
    """Test RequestTask RPC with mock coordinator."""
    client = Client("test-client", "localhost:50051")

    mock_response = coordinator_pb2.TaskResponse(
        has_task=True,
        task_type=coordinator_pb2.FIND_PRIMES,
        filename="test.txt",
        primary_fileserver_addr="localhost:50052",
    )
    client.coordinator.RequestTask = Mock(return_value=mock_response)

    request = coordinator_pb2.TaskRequest(client_id=client.client_id)
    task = client.coordinator.RequestTask(request)

    assert task.has_task is True
    assert task.task_type == coordinator_pb2.FIND_PRIMES
    assert task.filename == "test.txt"
    assert task.primary_fileserver_addr == "localhost:50052"


def test_complete_task_with_mock():
    """Test CompleteTask RPC with mock coordinator."""
    client = Client("test-client", "localhost:50051")

    mock_response = coordinator_pb2.TaskCompletionResponse(acknowledged=True)
    client.coordinator.CompleteTask = Mock(return_value=mock_response)

    completion_request = coordinator_pb2.TaskCompletionRequest(
        client_id=client.client_id, task_id="test.txt"
    )
    response = client.coordinator.CompleteTask(completion_request)

    assert response.acknowledged is True


def test_no_task_available():
    """Test handling when coordinator has no tasks."""
    client = Client("test-client", "localhost:50051")

    mock_response = coordinator_pb2.TaskResponse(has_task=False)
    client.coordinator.RequestTask = Mock(return_value=mock_response)

    request = coordinator_pb2.TaskRequest(client_id=client.client_id)
    task = client.coordinator.RequestTask(request)

    assert task.has_task is False


def test_heartbeat_with_mock():
    """Test SendHeartbeat RPC with mock coordinator."""
    client = Client("test-client", "localhost:50051")

    mock_response = coordinator_pb2.HeartbeatResponse(acknowledged=True)
    client.coordinator.SendHeartbeat = Mock(return_value=mock_response)

    request = coordinator_pb2.HeartbeatRequest(client_id=client.client_id)
    response = client.coordinator.SendHeartbeat(request)

    assert response.acknowledged is True


def test_fetch_file_with_mock():
    """Test file fetching with mock file server."""
    client = Client("test-client", "localhost:50051")

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
            filepath = client._fetch_file("test.txt", "localhost:50052")

    assert filepath.exists()
    assert filepath.read_bytes() == file_content
    assert "test.txt" in client.cached_files


def test_fetch_file_from_cache():
    """Test file is served from cache on second request."""
    client = Client("test-client", "localhost:50051")

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
            filepath1 = client._fetch_file("test.txt", "localhost:50052")
            filepath2 = client._fetch_file("test.txt", "localhost:50052")

    assert filepath1 == filepath2
    assert mock_stub.FetchFile.call_count == 1


def test_process_file_partial_range(temp_test_file):
    """Test processing partial line range."""
    client = Client("test-client", "localhost:50051")

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


def test_merge_files():
    """Test merging two prime files."""
    client = Client("test-client", "localhost:50051")

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


def test_upload_includes_checksum(temp_test_file):
    """Test that upload includes checksum and atomic_upload flag."""
    client = Client("test-client", "localhost:50051")

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


def test_download_verifies_checksum(temp_test_file):
    """Test that download verifies checksum from file server."""
    client = Client("test-client", "localhost:50051")

    file_content = temp_test_file.read_bytes()

    # Compute correct checksum
    sha256 = hashlib.sha256()
    sha256.update(file_content)
    correct_checksum = sha256.hexdigest()

    # Create fetch chunks with checksum in metadata
    fetch_chunks = [
        fileserver_pb2.FileChunk(
            metadata=fileserver_pb2.FileMetadata(
                filename="test_numbers.txt",
                file_size=len(file_content),
                checksum=correct_checksum,
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
            # Should succeed with correct checksum
            cached_path = client._fetch_file("test_numbers.txt", "localhost:50052")

    assert cached_path.exists()
    assert cached_path.read_bytes() == file_content


def test_download_detects_checksum_mismatch(temp_test_file):
    """Test that download detects checksum mismatch and raises error."""
    client = Client("test-client", "localhost:50051")

    file_content = temp_test_file.read_bytes()

    # Use wrong checksum
    wrong_checksum = "a" * 64

    # Create fetch chunks with wrong checksum
    fetch_chunks = [
        fileserver_pb2.FileChunk(
            metadata=fileserver_pb2.FileMetadata(
                filename="test_numbers.txt",
                file_size=len(file_content),
                checksum=wrong_checksum,
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
            # Should raise RuntimeError on checksum mismatch
            try:
                client._fetch_file("test_numbers.txt", "localhost:50052")
                assert False, "Expected RuntimeError for checksum mismatch"
            except RuntimeError as e:
                assert "Checksum verification failed" in str(e)
                assert wrong_checksum in str(e)
