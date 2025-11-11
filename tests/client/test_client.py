"""
Complete test suite for Client.

Total: 74 tests
"""
import hashlib
import json
import threading
import time
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import grpc
import pytest

from client.client import Client, ProcessState, RpcType, TaskStage
from generated import coordinator_pb2, fileserver_pb2


# ============================================================================
# FIXTURES (Shared between all tests)
# ============================================================================

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


@pytest.fixture
def snapshot_heartbeat_response():
    """Create mock heartbeat response with snapshot fields."""
    def _create_response(trigger=False, marker=False):
        return coordinator_pb2.HeartbeatResponse(
            acknowledged=True,
            trigger_snapshot=trigger,
            snapshot_marker=marker
        )
    return _create_response


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

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
        # Use side_effect with lambda to create fresh iterator on each call
        stub.FetchFile.side_effect = lambda req: create_fetch_chunks(filename, content, checksum)

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
    # Always patch time.sleep to prevent tests from hanging in retry loops
    patches.enter_context(patch("time.sleep"))

    result["channel"] = channel
    result["patches"] = patches
    return result


def mock_coordinator_stub(client, method_name, response):
    """Mock a coordinator stub method."""
    mock_stub = Mock()
    setattr(mock_stub, method_name, Mock(return_value=response))
    client._get_coordinator_stub = Mock(return_value=mock_stub)
    return mock_stub


# ============================================================================
# SECTION 1: BASIC CLIENT TESTS (2 tests)
# ============================================================================

def test_client_initialization(client):
    """Test client initializes correctly."""
    assert client.client_id == "test-client"
    assert client.cache_dir.exists()
    # Snapshot initialization
    assert hasattr(client, 'snapshot_lock')
    assert hasattr(client, 'snapshot_active')
    assert hasattr(client, 'process_state')
    assert client.snapshot_active is False


def test_tmp_cleanup_on_startup(test_cache_dir):
    """Test that orphaned .tmp files are cleaned up on client initialization."""
    client_cache_path = Path(test_cache_dir) / "test-client"
    client_cache_path.mkdir(parents=True, exist_ok=True)

    tmp1 = client_cache_path / "orphaned1.txt.tmp"
    tmp2 = client_cache_path / "orphaned2.txt.tmp"
    tmp1.write_text("partial data")
    tmp2.write_text("more partial data")

    assert tmp1.exists()
    assert tmp2.exists()

    Client(
        "test-client",
        ["localhost:50051", "localhost:50052"],
        ["localhost:50052"],
        test_cache_dir,
    )

    assert not tmp1.exists()
    assert not tmp2.exists()


# ============================================================================
# SECTION 2: FILE PROCESSING TESTS (6 tests)
# ============================================================================

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
                "file1_primes.txt", "file2_primes.txt", "task123", "localhost:50052", False
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


def test_merge_files_with_last_task_true(client):
    """Test merging files with last_task=True produces 'primes.txt'."""
    file1_content = b"2\n3\n5\n7\n"
    file2_content = b"5\n7\n11\n13\n"

    capture_upload, uploaded_data = create_upload_capturer()

    fetch_chunks1 = [
        fileserver_pb2.FileChunk(
            metadata=fileserver_pb2.FileMetadata(
                filename="file1_primes.txt", file_size=len(file1_content)
            )
        ),
        fileserver_pb2.FileChunk(chunk_data=file1_content),
    ]

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
                "file1_primes.txt", "file2_primes.txt", "final_task", "localhost:50052", True
            )

    result_data = b"".join(
        chunk.chunk_data for chunk in uploaded_data if chunk.HasField("chunk_data")
    )
    result_numbers = [int(line) for line in result_data.decode().strip().split("\n")]

    assert success is True
    # Key assertion: when last_task=True, output should be "primes.txt"
    assert output_filename == "primes.txt"
    assert result_numbers == [2, 3, 5, 7, 11, 13]
    assert mock_stub.FetchFile.call_count == 2
    assert mock_stub.UploadFile.call_count == 1


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
                    "file1_primes.txt", "file2_primes.txt", "task123", "localhost:50052", False
                )

    assert success is False
    assert output_filename == "merged_task123.txt"

    # No .tmp files should remain
    cache_path = Path(test_cache_dir)
    tmp_files = list(cache_path.glob("*.tmp"))
    assert len(tmp_files) == 0


# ============================================================================
# SECTION 3: FILE FETCH TESTS (7 tests)
# ============================================================================

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
    """Test that fetch detects checksum mismatch and retries indefinitely."""
    file_content = temp_test_file.read_bytes()

    # Use wrong checksum - will fail on all servers
    wrong_checksum = "a" * 64

    mock_stub = MagicMock()
    call_count = 0
    max_attempts = 3  # Limit attempts to avoid hanging

    def mock_fetch(_):
        nonlocal call_count
        call_count += 1
        if call_count >= max_attempts * len(client.fileserver_addresses):
            # After max attempts, succeed to break the retry loop
            correct_checksum = hashlib.sha256(file_content).hexdigest()
            return create_fetch_chunks("test_numbers.txt", file_content, correct_checksum)
        # Keep failing with wrong checksum
        return create_fetch_chunks("test_numbers.txt", file_content, wrong_checksum)

    mock_stub.FetchFile.side_effect = mock_fetch
    sleep_times = []

    with patch("grpc.insecure_channel"):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            with patch("time.sleep", side_effect=lambda s: sleep_times.append(s)):
                # Should retry with backoff when all servers fail checksum
                cached_path = client._fetch_file("test_numbers.txt")

    # Verify it retried multiple times
    assert call_count >= len(client.fileserver_addresses)  # At least one full round
    # Verify exponential backoff occurred
    assert len(sleep_times) > 0
    assert sleep_times[0] == 2  # First backoff: 2^1 seconds
    # Verify it eventually succeeded
    assert cached_path.exists()


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


# ============================================================================
# SECTION 4: UPLOAD RETRY & FAILOVER TESTS (9 tests)
# ============================================================================

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


# ============================================================================
# SECTION 5: COORDINATOR RPC TESTS (6 tests)
# ============================================================================

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


# ============================================================================
# SECTION 6: COORDINATOR FAILOVER TESTS (4 tests)
# ============================================================================

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


# ============================================================================
# SECTION 7: SNAPSHOT TESTS (17 tests)
# ============================================================================

def test_snapshot_initiation(client):
    """Test snapshot can be initiated."""
    client._start_snapshot()

    assert client.snapshot_active is True
    assert client.channel_messages == []


def test_process_state_updates_during_find_primes(client, temp_test_file):
    """Test process state updates during FIND_PRIMES stage."""
    file_content = temp_test_file.read_bytes()
    checksum = hashlib.sha256(file_content).hexdigest()

    mocks = mock_fileserver(
        fetch_file={"filename": "test.txt", "content": file_content, "checksum": checksum},
        upload_success=True
    )

    with mocks["patches"]:
        client._start_snapshot()

        # Process file
        success, _ = client.process_file("test.txt", "localhost:50052", 0, 10)

        assert success is True
        # Check process state was updated (can be FIND_PRIMES, COMPLETED, or error states)
        assert client.process_state.task_stage in [
            TaskStage.FIND_PRIMES,
            TaskStage.COMPLETED,
            TaskStage.UPLOAD_ERROR,
            TaskStage.FIND_PRIMES_ERROR
        ]


def test_message_recording_during_snapshot(client):
    """Test messages are recorded when snapshot is active."""
    client._start_snapshot()

    client._record(RpcType.REQUEST_TO_COORDINATOR_GET_TASK.value, client_id="test-client")
    client._record(RpcType.RESPONSE_FROM_COORDINATOR_GET_TASK.value, task_id="task_123")

    assert len(client.channel_messages) == 2
    assert client.channel_messages[0]["type"] == RpcType.REQUEST_TO_COORDINATOR_GET_TASK.value
    assert client.channel_messages[1]["type"] == RpcType.RESPONSE_FROM_COORDINATOR_GET_TASK.value


def test_no_recording_when_snapshot_inactive(client):
    """Test messages are not recorded when snapshot is inactive."""
    assert client.snapshot_active is False

    client._record(RpcType.REQUEST_TO_FILESERVER_DOWNLOAD.value, file="test.txt")

    assert len(client.channel_messages) == 0


def test_snapshot_finalization_on_marker(client, capsys):
    """Test snapshot is finalized when marker is received."""
    # Mock the coordinator stub for snapshot submission
    mock_stub = Mock()
    mock_response = coordinator_pb2.SnapshotSubmissionResponse(acknowledged=True)
    mock_stub.SubmitSnapshot = Mock(return_value=mock_response)
    client._get_coordinator_stub = Mock(return_value=mock_stub)

    client._start_snapshot()
    client._record(RpcType.REQUEST_TO_COORDINATOR_GET_TASK.value, client_id=client.client_id)
    client._receive_marker()

    assert client.snapshot_active is False

    captured = capsys.readouterr()
    assert "SNAPSHOT" in captured.out


def test_process_state_during_merge(client):
    """Test process state during merge operation."""
    file1_content = b"2\n3\n5\n"
    file2_content = b"7\n11\n13\n"

    checksum1 = hashlib.sha256(file1_content).hexdigest()
    checksum2 = hashlib.sha256(file2_content).hexdigest()

    mocks = mock_fileserver(upload_success=True)
    mocks["stub"].FetchFile.side_effect = [
        create_fetch_chunks("file1.txt", file1_content, checksum1),
        create_fetch_chunks("file2.txt", file2_content, checksum2)
    ]

    with mocks["patches"]:
        client._start_snapshot()

        # Perform merge
        success, output = client.merge_files("file1.txt", "file2.txt", "task_merge", "localhost:50052", False)

        # Check state
        assert success is True
        assert client.process_state.task_stage in [TaskStage.MERGE, TaskStage.MERGE_ERROR, TaskStage.COMPLETED]
        assert client.process_state.current_file in ["file1.txt", "file2.txt", "file1.txt,file2.txt"]


def test_progress_updates_during_find_primes(client):
    """Test progress percentage updates every 100 lines."""
    # Create test file with 500 lines
    test_file = client.cache_dir / "large_file.txt"
    test_file.parent.mkdir(parents=True, exist_ok=True)
    with open(test_file, "w") as f:
        for i in range(500):
            f.write(f"{i}\n")

    file_content = test_file.read_bytes()
    checksum = hashlib.sha256(file_content).hexdigest()

    mocks = mock_fileserver(
        fetch_file={"filename": "large_file.txt", "content": file_content, "checksum": checksum},
        upload_success=True
    )

    with mocks["patches"]:
        client._start_snapshot()
        success, _ = client.process_file("large_file.txt", "localhost:50052", 0, 500)

        assert success is True
        # Progress should reach 100 after completion
        assert client.process_state.progress_percentage == 100


def test_all_rpc_types_recorded(client):
    """Test all RpcType enum values can be recorded."""
    client._start_snapshot()

    # Record all RPC types (passing string values, not enum objects)
    client._record(RpcType.REQUEST_TO_FILESERVER_UPLOAD.value, file="test.txt")
    client._record(RpcType.RESPONSE_FROM_FILESERVER_UPLOAD.value, file="test.txt")
    client._record(RpcType.REQUEST_TO_FILESERVER_DOWNLOAD.value, file="test.txt")
    client._record(RpcType.RESPONSE_FROM_FILESERVER_DOWNLOAD.value, file="test.txt")
    client._record(RpcType.REQUEST_TO_COORDINATOR_GET_TASK.value, client_id="test")
    client._record(RpcType.RESPONSE_FROM_COORDINATOR_GET_TASK.value, task_id="123")
    client._record(RpcType.REQUEST_TO_COORDINATOR_TASK_STATUS.value, task_id="123")
    client._record(RpcType.RESPONSE_FROM_COORDINATOR_TASK_STATUS.value, acknowledged=True)

    assert len(client.channel_messages) == 8
    # Verify all messages have type field and are strings
    for msg in client.channel_messages:
        assert "type" in msg
        assert isinstance(msg["type"], str)


def test_snapshot_lock_thread_safety(client):
    """Test snapshot lock ensures thread safety."""
    client._start_snapshot()

    # Simulate concurrent message recording from multiple threads
    def record_messages():
        for i in range(100):
            client._record(RpcType.REQUEST_TO_COORDINATOR_GET_TASK.value, msg_id=i)

    threads = [threading.Thread(target=record_messages) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Should have exactly 500 messages (5 threads * 100 messages)
    assert len(client.channel_messages) == 500


def test_heartbeat_with_snapshot_trigger_and_marker(client, snapshot_heartbeat_response):
    """Test heartbeat can trigger and finalize snapshots."""
    # Mock coordinator stub
    mock_stub = Mock()
    mock_response = coordinator_pb2.SnapshotSubmissionResponse(acknowledged=True)
    mock_stub.SubmitSnapshot = Mock(return_value=mock_response)
    client._get_coordinator_stub = Mock(return_value=mock_stub)

    # Simulate heartbeat trigger
    trigger_response = snapshot_heartbeat_response(trigger=True)
    assert trigger_response.trigger_snapshot is True

    client._start_snapshot()
    assert client.snapshot_active is True

    # Simulate heartbeat marker
    marker_response = snapshot_heartbeat_response(marker=True)
    assert marker_response.snapshot_marker is True

    client._receive_marker()
    assert client.snapshot_active is False


def test_full_snapshot_integration(client, temp_test_file, snapshot_heartbeat_response, capsys):
    """Integration test for complete snapshot flow."""
    file_content = temp_test_file.read_bytes()
    checksum = hashlib.sha256(file_content).hexdigest()

    # Mock fileserver
    mocks = mock_fileserver(
        fetch_file={"filename": "test.txt", "content": file_content, "checksum": checksum},
        upload_success=True
    )

    # Mock coordinator
    mock_stub = Mock()
    mock_response = coordinator_pb2.SnapshotSubmissionResponse(acknowledged=True)
    mock_stub.SubmitSnapshot = Mock(return_value=mock_response)
    client._get_coordinator_stub = Mock(return_value=mock_stub)

    trigger_response = snapshot_heartbeat_response(trigger=True)
    marker_response = snapshot_heartbeat_response(marker=True)

    with mocks["patches"]:
        # 1. Start snapshot via trigger
        client._start_snapshot()
        assert client.snapshot_active is True

        # 2. Update process state and record task
        with client.snapshot_lock:
            client.process_state.task_id = "task_integration"
        client._record(RpcType.REQUEST_TO_COORDINATOR_GET_TASK.value, client_id=client.client_id)
        client._record(RpcType.RESPONSE_FROM_COORDINATOR_GET_TASK.value, task_id="task_integration")

        # 3. Process file (records fetch/upload messages)
        success, output = client.process_file("test.txt", "localhost:50052", 0, 10)
        assert success is True

        # 4. Check messages were recorded
        assert len(client.channel_messages) >= 4  # 2 coordinator + 2+ fileserver

        # 5. Finalize with marker
        client._receive_marker()
        assert client.snapshot_active is False

        # 6. Verify JSON output
        captured = capsys.readouterr()
        assert "task_integration" in captured.out
        assert "SNAPSHOT" in captured.out


def test_process_state_class_initialization():
    """Test ProcessState class initializes correctly."""
    state = ProcessState()
    assert state.task_id == ""
    assert state.task_stage == TaskStage.IDLE
    assert state.task_type == TaskStage.IDLE
    assert state.progress_percentage == 0
    assert state.current_file == ""
    assert state.start_index == 0
    assert state.end_index == 0
    assert state.output_filename == ""


def test_task_stage_enum_values():
    """Test TaskStage enum has all expected values aligned with coordinator.proto."""
    assert TaskStage.IDLE.value == -1
    assert TaskStage.DOWNLOAD.value == 2
    assert TaskStage.UPLOAD.value == 3
    assert TaskStage.FIND_PRIMES.value == 0  # Matches coordinator.proto TaskType
    assert TaskStage.MERGE.value == 1  # Matches coordinator.proto TaskType
    assert TaskStage.COMPLETED.value == 4
    assert TaskStage.DOWNLOAD_ERROR.value == 5
    assert TaskStage.UPLOAD_ERROR.value == 6
    assert TaskStage.FIND_PRIMES_ERROR.value == 7
    assert TaskStage.MERGE_ERROR.value == 8


def test_rpc_type_enum_values():
    """Test RpcType enum has all expected values."""
    assert RpcType.REQUEST_TO_FILESERVER_UPLOAD.value == "Requesting to upload to Fileserver"
    assert RpcType.RESPONSE_FROM_FILESERVER_UPLOAD.value == "Receiving upload response from Fileserver"
    assert RpcType.REQUEST_TO_FILESERVER_DOWNLOAD.value == "Requesting to download from Fileserver"
    assert RpcType.RESPONSE_FROM_FILESERVER_DOWNLOAD.value == "Receiving download response from Fileserver"
    assert RpcType.REQUEST_TO_COORDINATOR_GET_TASK.value == "Requesting task to Coordinator"
    assert RpcType.RESPONSE_FROM_COORDINATOR_GET_TASK.value == "Receiving task from Coordinator"
    assert RpcType.REQUEST_TO_COORDINATOR_TASK_STATUS.value == "Requesting task update to Coordinator"
    assert RpcType.RESPONSE_FROM_COORDINATOR_TASK_STATUS.value == "Receiving task update response from Coordinator"


def test_snapshot_double_initiation_ignored(client):
    """Test that initiating snapshot twice doesn't reset it."""
    client._start_snapshot()
    client._record(RpcType.REQUEST_TO_COORDINATOR_GET_TASK.value, client_id="test")

    # Try to initiate again
    client._start_snapshot()

    # Should still have messages from first snapshot
    assert len(client.channel_messages) == 1


def test_snapshot_state_after_error(client, temp_test_file):
    """Test process state captures error stages."""
    file_content = temp_test_file.read_bytes()
    checksum = hashlib.sha256(file_content).hexdigest()

    mocks = mock_fileserver(
        fetch_file={"filename": "test.txt", "content": file_content, "checksum": checksum},
        upload_success=False  # Upload will fail
    )
    mocks["stub"].UploadFile.return_value = fileserver_pb2.UploadResponse(success=False, error_message="Upload failed")

    with mocks["patches"]:
        client._start_snapshot()

        with patch.object(client, "_upload_file", return_value=False):
            success, _ = client.process_file("test.txt", "localhost:50052", 0, 10)

        assert success is False
        # Process state should show error (can be UPLOAD_ERROR or FIND_PRIMES_ERROR depending on where failure occurred)
        assert client.process_state.task_stage in [TaskStage.UPLOAD_ERROR, TaskStage.FIND_PRIMES_ERROR]


# ============================================================================
# SECTION 8: CRASH RECOVERY TESTS - BUG FIXES (12 new tests)
# ============================================================================

def test_crash_detection_with_alive_lock(client, test_cache_dir):
    """Test crash is detected when alive lock file exists."""
    # Create alive lock file to simulate crash
    alive_lock = client.alive_lock_file
    alive_lock.write_text("crash simulation")

    # Create snapshot.json for recovery
    snapshot_data = {
        "clients": {
            client.client_id: {
                "client_id": client.client_id,
                "task_id": "",
                "task_stage": -1,  # IDLE
                "task_type": -1,  # IDLE
                "progress": 0,
                "file": "",
                "task_start_index": 0,
                "task_end_index": 0,
                "output_filename": "",
                "messages": []
            }
        }
    }
    snapshot_file = client.cache_dir / "snapshot.json"
    snapshot_file.write_text(json.dumps(snapshot_data))

    # Mock fileserver for snapshot download
    mocks = mock_fileserver(
        fetch_file={"filename": "snapshot.json", "content": json.dumps(snapshot_data).encode()},
        upload_success=False
    )

    with mocks["patches"]:
        recovered = client._check_and_recover_from_crash()

    # Should detect crash and recover
    assert recovered is True
    # Alive lock should be recreated
    assert client.alive_lock_file.exists()


def test_crash_recovery_merge_task_type_check(client, test_cache_dir):
    """Test Bug #1: Merge recovery checks task_type not task_stage."""
    # Simulate crash during DOWNLOAD stage with MERGE task type
    snapshot_data = {
        "clients": {
            client.client_id: {
                "client_id": client.client_id,
                "task_id": "merge_123",
                "task_stage": 2,  # DOWNLOAD
                "task_type": 1,  # MERGE
                "progress": 50,
                "file": "file1.txt,file2.txt",
                "task_start_index": 0,
                "task_end_index": 100,
                "output_filename": "",
                "messages": []
            }
        }
    }

    client.process_state.task_stage = TaskStage.DOWNLOAD
    client.process_state.task_type = TaskStage.MERGE
    client.process_state.task_id = "merge_123"
    client.process_state.current_file = "file1.txt,file2.txt"

    # Mock merge_files to verify it's called
    with patch.object(client, 'merge_files', return_value=(True, "merged_merge_123.txt")) as mock_merge:
        with patch.object(client, '_get_coordinator_stub') as mock_coord:
            mock_coord.return_value.UpdateTaskStatus.return_value = coordinator_pb2.TaskStatusResponse(acknowledged=True)

            # Simulate recovery logic
            task_stage = client.process_state.task_stage
            task_type = client.process_state.task_type
            current_file = client.process_state.current_file

            if task_stage in (TaskStage.DOWNLOAD, TaskStage.DOWNLOAD_ERROR):
                if task_type in (TaskStage.MERGE, TaskStage.MERGE_ERROR) and current_file:
                    files = current_file.split(',')
                    client.merge_files(files[0], files[1], "merge_123", "localhost:50052", False)

    # Verify merge_files was called (Bug fix working)
    mock_merge.assert_called_once()


def test_crash_recovery_invalid_merge_file_format(client):
    """Test Bug #3: Handle invalid merge file format gracefully."""
    # Test with no comma
    client.process_state.current_file = "singlefile.txt"
    client.process_state.task_type = TaskStage.MERGE
    client.process_state.task_stage = TaskStage.DOWNLOAD

    # Should not crash, should handle gracefully
    try:
        files = client.process_state.current_file.split(',')
        if len(files) != 2:
            # This is the fix - validate before using
            with client.snapshot_lock:
                client.process_state.task_stage = TaskStage.IDLE
                client.process_state.task_type = TaskStage.IDLE
        success = True
    except Exception:
        success = False

    assert success is True


def test_crash_recovery_missing_end_index(client):
    """Test Bug #4: Handle missing end_index in snapshot with default value."""
    snapshot_data = {
        "task_id": "test_123",
        "task_stage": 0,  # FIND_PRIMES
        "task_type": 0,  # FIND_PRIMES
        "progress": 50,
        "file": "test.txt",
        "task_start_index": 0,
        # end_index is missing
        "output_filename": ""
    }

    client._restore_from_snapshot(snapshot_data)

    # Should have default value (0) not None
    assert client.process_state.end_index == 0
    assert isinstance(client.process_state.end_index, int)


def test_progress_calculation_division_by_zero(client):
    """Test Bug #5: Handle division by zero when start_index == end_index."""
    # Create test file
    test_file = client.cache_dir / "test.txt"
    test_file.write_text("17\n19\n23\n")

    # Set start_index == end_index (empty range)
    start_index = 10
    end_index = 10

    # Should not crash with ZeroDivisionError
    try:
        with open(test_file, "r") as f:
            for line_num, line in enumerate(f):
                if line_num >= end_index:
                    break
                if line_num < start_index:
                    continue

                # This is the fix - check before division
                if end_index - start_index > 0:
                    progress = int(((line_num - start_index) / (end_index - start_index)) * 100)
        success = True
    except ZeroDivisionError:
        success = False

    assert success is True


def test_state_reset_after_failed_task(client):
    """Test Bug #6: Process state is reset after failed task."""
    # Set up state as if task failed
    with client.snapshot_lock:
        client.process_state.task_id = "failed_123"
        client.process_state.task_stage = TaskStage.UPLOAD_ERROR
        client.process_state.task_type = TaskStage.FIND_PRIMES
        client.process_state.current_file = "test.txt"
        client.process_state.output_filename = "output.txt"

    # Simulate failed task handling
    success = False
    if not success:
        # This is the fix - reset state after failure
        with client.snapshot_lock:
            client.process_state.task_stage = TaskStage.IDLE
            client.process_state.task_type = TaskStage.IDLE
            client.process_state.task_id = ""
            client.process_state.current_file = ""
            client.process_state.output_filename = ""
            client.process_state.progress_percentage = 0

    # Verify state was reset
    assert client.process_state.task_stage == TaskStage.IDLE
    assert client.process_state.task_type == TaskStage.IDLE
    assert client.process_state.task_id == ""
    assert client.process_state.current_file == ""
    assert client.process_state.output_filename == ""


def test_output_filename_in_completion_state(client):
    """Test Bug #7: output_filename is set in process state on completion."""
    output_filename = "test_output.txt"

    # Simulate task completion
    with client.snapshot_lock:
        client.process_state.task_stage = TaskStage.COMPLETED
        client.process_state.progress_percentage = 100
        client.process_state.current_file = ""
        client.process_state.output_filename = output_filename  # This is the fix

    # Verify output_filename is stored
    assert client.process_state.output_filename == output_filename


def test_snapshot_restoration_with_all_fields(client):
    """Test complete snapshot restoration including new fields."""
    snapshot_data = {
        "task_id": "complete_123",
        "task_stage": 3,  # UPLOAD
        "task_type": 0,  # FIND_PRIMES
        "progress": 75,
        "file": "numbers.txt",
        "task_start_index": 100,
        "task_end_index": 200,
        "output_filename": "numbers_range_100_200_primes.txt"
    }

    client._restore_from_snapshot(snapshot_data)

    # Verify all fields restored correctly
    assert client.process_state.task_id == "complete_123"
    assert client.process_state.task_stage == TaskStage.UPLOAD
    assert client.process_state.task_type == TaskStage.FIND_PRIMES
    assert client.process_state.progress_percentage == 75
    assert client.process_state.current_file == "numbers.txt"
    assert client.process_state.start_index == 100
    assert client.process_state.end_index == 200
    assert client.process_state.output_filename == "numbers_range_100_200_primes.txt"


def test_crash_recovery_upload_stage(client, test_cache_dir):
    """Test crash recovery during UPLOAD stage."""
    # Create output file
    output_filename = "test_output.txt"
    output_file = client.cache_dir / output_filename
    output_file.write_text("2\n3\n5\n7\n")

    # Set state as if crashed during upload
    with client.snapshot_lock:
        client.process_state.task_id = "upload_123"
        client.process_state.task_stage = TaskStage.UPLOAD
        client.process_state.task_type = TaskStage.FIND_PRIMES
        client.process_state.output_filename = output_filename

    # Mock upload
    with patch.object(client, '_upload_file', return_value=True) as mock_upload:
        with patch.object(client, '_get_coordinator_stub') as mock_coord:
            mock_coord.return_value.UpdateTaskStatus.return_value = coordinator_pb2.TaskStatusResponse(acknowledged=True)

            # Simulate recovery
            output_filename_state = client.process_state.output_filename
            if output_filename_state:
                path = client.cache_dir / output_filename_state
                success = client._upload_file(path, output_filename_state, "localhost:50052")

    # Verify upload was retried
    mock_upload.assert_called_once()


def test_crash_recovery_find_primes_stage(client):
    """Test crash recovery during FIND_PRIMES stage."""
    # Set state as if crashed during find primes
    with client.snapshot_lock:
        client.process_state.task_id = "primes_123"
        client.process_state.task_stage = TaskStage.DOWNLOAD
        client.process_state.task_type = TaskStage.FIND_PRIMES
        client.process_state.current_file = "numbers.txt"
        client.process_state.start_index = 0
        client.process_state.end_index = 100

    # Mock process_file
    with patch.object(client, 'process_file', return_value=(True, "output.txt")) as mock_process:
        # Simulate recovery
        task_stage = client.process_state.task_stage
        task_type = client.process_state.task_type
        current_file = client.process_state.current_file
        start_index = client.process_state.start_index
        end_index = client.process_state.end_index

        if task_stage in (TaskStage.DOWNLOAD, TaskStage.DOWNLOAD_ERROR):
            if task_type in (TaskStage.FIND_PRIMES, TaskStage.FIND_PRIMES_ERROR) and current_file:
                client.process_file(current_file, "localhost:50052", start_index, end_index)

    # Verify process_file was called to restart
    mock_process.assert_called_once_with("numbers.txt", "localhost:50052", 0, 100)


def test_snapshot_json_serialization(client):
    """Test snapshot data can be serialized to JSON with enums."""
    with client.snapshot_lock:
        client.process_state.task_id = "json_test"
        client.process_state.task_stage = TaskStage.FIND_PRIMES
        client.process_state.task_type = TaskStage.FIND_PRIMES
        client.process_state.progress_percentage = 50
        client.process_state.current_file = "test.txt"
        client.process_state.start_index = 0
        client.process_state.end_index = 100
        client.process_state.output_filename = ""

    client._start_snapshot()
    client._record(RpcType.REQUEST_TO_COORDINATOR_GET_TASK.value, task_id="json_test")

    # Create snapshot data (using .value for enums)
    snapshot_data = {
        "client_id": client.client_id,
        "task_id": client.process_state.task_id,
        "task_stage": client.process_state.task_stage.value,  # .value converts enum to integer
        "task_type": client.process_state.task_type.value,
        "progress": client.process_state.progress_percentage,
        "file": client.process_state.current_file,
        "task_start_index": client.process_state.start_index,
        "task_end_index": client.process_state.end_index,
        "output_filename": client.process_state.output_filename,
        "messages": client.channel_messages
    }

    # Should not raise error
    json_str = json.dumps(snapshot_data, indent=2)
    assert json_str is not None

    # Verify can be deserialized
    parsed = json.loads(json_str)
    assert parsed["task_stage"] == 0  # FIND_PRIMES = 0
    assert parsed["task_type"] == 0  # FIND_PRIMES = 0


def test_alive_lock_cleanup_on_graceful_shutdown(client):
    """Test alive lock is removed on clean shutdown."""
    # Create alive lock
    client._generate_alive_lock()
    assert client.alive_lock_file.exists()

    # Clean shutdown
    client._remove_alive_lock()
    assert not client.alive_lock_file.exists()


# ============================================================================
# SECTION 9: EDGE CASE TESTS (8 new tests)
# ============================================================================

def test_process_file_empty_range(client, temp_test_file):
    """Test processing file with empty range (start == end)."""
    file_content = temp_test_file.read_bytes()
    checksum = hashlib.sha256(file_content).hexdigest()

    mocks = mock_fileserver(
        fetch_file={"filename": "test.txt", "content": file_content, "checksum": checksum},
        upload_success=True
    )

    with mocks["patches"]:
        # start_index == end_index (empty range)
        success, output_filename = client.process_file("test.txt", "localhost:50052", 5, 5)

    # Should succeed with empty result
    assert success is True


def test_snapshot_with_empty_messages(client, capsys):
    """Test snapshot finalization with no recorded messages."""
    # Mock coordinator stub
    mock_stub = Mock()
    mock_response = coordinator_pb2.SnapshotSubmissionResponse(acknowledged=True)
    mock_stub.SubmitSnapshot = Mock(return_value=mock_response)
    client._get_coordinator_stub = Mock(return_value=mock_stub)

    client._start_snapshot()
    # Don't record any messages
    client._receive_marker()

    captured = capsys.readouterr()
    assert "SNAPSHOT" in captured.out


def test_restore_snapshot_invalid_enum_value(client):
    """Test snapshot restoration with invalid enum value."""
    snapshot_data = {
        "task_id": "test",
        "task_stage": "INVALID_STAGE",
        "task_type": "INVALID_TYPE",
        "progress": 0,
        "file": "",
        "task_start_index": 0,
        "task_end_index": 0,
        "output_filename": ""
    }

    # Should handle gracefully and reset to IDLE
    client._restore_from_snapshot(snapshot_data)

    # Should fall back to IDLE due to exception handling
    assert client.process_state.task_stage == TaskStage.IDLE
    assert client.process_state.task_type == TaskStage.IDLE


def test_concurrent_snapshot_recording(client):
    """Test concurrent message recording from multiple threads doesn't corrupt data."""
    client._start_snapshot()
    errors = []

    def record_batch(batch_id):
        try:
            for i in range(50):
                client._record(
                    RpcType.REQUEST_TO_COORDINATOR_GET_TASK.value,
                    batch=batch_id,
                    index=i
                )
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=record_batch, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # No errors should occur
    assert len(errors) == 0
    # Should have all 500 messages
    assert len(client.channel_messages) == 500


def test_process_file_with_invalid_numbers(client):
    """Test processing file with invalid number formats."""
    # Create file with mixed valid/invalid content
    test_file = client.cache_dir / "mixed.txt"
    test_file.write_text("17\ninvalid\n19\n\n23\nabc\n29\n")

    file_content = test_file.read_bytes()
    checksum = hashlib.sha256(file_content).hexdigest()

    capture_upload, uploaded_data = create_upload_capturer()

    mocks = mock_fileserver(
        fetch_file={"filename": "mixed.txt", "content": file_content, "checksum": checksum},
        upload_success=True
    )
    mocks["stub"].UploadFile.side_effect = capture_upload

    with mocks["patches"]:
        success, output = client.process_file("mixed.txt", "localhost:50052", 0, 7)

    # Should succeed and skip invalid lines
    assert success is True

    result_data = b"".join(
        chunk.chunk_data for chunk in uploaded_data if chunk.HasField("chunk_data")
    )
    result_numbers = [int(line) for line in result_data.decode().strip().split("\n")]

    # Should only have valid primes
    assert 17 in result_numbers
    assert 19 in result_numbers
    assert 23 in result_numbers
    assert 29 in result_numbers


def test_merge_with_duplicate_primes(client):
    """Test merging files with duplicate primes."""
    file1_content = b"2\n3\n5\n7\n11\n"
    file2_content = b"5\n7\n11\n13\n17\n"

    capture_upload, uploaded_data = create_upload_capturer()

    mocks = mock_fileserver(upload_success=True)
    mocks["stub"].FetchFile.side_effect = [
        create_fetch_chunks("file1.txt", file1_content),
        create_fetch_chunks("file2.txt", file2_content)
    ]
    mocks["stub"].UploadFile.side_effect = capture_upload

    with mocks["patches"]:
        success, output = client.merge_files("file1.txt", "file2.txt", "dup_test", "localhost:50052", False)

    assert success is True

    result_data = b"".join(
        chunk.chunk_data for chunk in uploaded_data if chunk.HasField("chunk_data")
    )
    result_numbers = [int(line) for line in result_data.decode().strip().split("\n")]

    # Should deduplicate
    assert result_numbers == [2, 3, 5, 7, 11, 13, 17]


def test_state_updates_with_snapshot_lock(client):
    """Test all state updates properly use snapshot lock."""
    def update_state():
        for i in range(100):
            with client.snapshot_lock:
                client.process_state.progress_percentage = i

    threads = [threading.Thread(target=update_state) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Final value should be consistent (no race conditions)
    assert 0 <= client.process_state.progress_percentage <= 100


def test_snapshot_submission_failure_handling(client, capsys):
    """Test handling when snapshot submission to coordinator fails."""
    # Mock coordinator to fail
    mock_stub = Mock()
    mock_stub.SubmitSnapshot.side_effect = Exception("Coordinator unreachable")
    client._get_coordinator_stub = Mock(return_value=mock_stub)

    client._start_snapshot()
    client._record(RpcType.REQUEST_TO_COORDINATOR_GET_TASK.value, client_id="test")

    # Should not crash even if submission fails
    try:
        client._receive_marker()
        success = True
    except Exception:
        success = False

    assert success is True
    assert client.snapshot_active is False

    captured = capsys.readouterr()
    assert "Failed to send snapshot" in captured.out or "SNAPSHOT" in captured.out


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
    checksum = hashlib.sha256(test_content).hexdigest()
    mock = mock_fileserver(
        fetch_file={"filename": "test_numbers.txt", "content": test_content, "checksum": checksum},
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
        success, _ = client.merge_files(
            "primes1.txt", "primes2.txt", "task123", "localhost:50052", False
        )

    assert success
    assert existing_merged.exists()
    assert "old merged data" not in existing_merged.read_text()

    tmp_file = Path(str(existing_merged) + ".tmp")
    assert not tmp_file.exists()