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


def test_client_initialization():
    """Test client initializes correctly."""
    client = Client("test-client", "localhost:50051")
    assert client.client_id == "test-client"
    assert client.cache_dir.exists()


def test_process_file(temp_test_file):
    """Test processing entire file with mock file server."""
    client = Client("test-client", "localhost:50051")

    file_content = temp_test_file.read_bytes()
    mock_response = fileserver_pb2.FetchResponse(
        success=True, file_data=file_content, error_message=""
    )
    mock_stub = MagicMock()
    mock_stub.FetchFile.return_value = mock_response

    with patch("grpc.insecure_channel"):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            primes = client.process_file(str(temp_test_file.name), "localhost:50052")

    assert len(primes) == 5
    assert 17 in primes
    assert 19 in primes
    assert 23 in primes
    assert 29 in primes
    assert 31 in primes


def test_request_work_with_mock():
    """Test RequestWork RPC with mock coordinator."""
    client = Client("test-client", "localhost:50051")

    mock_response = coordinator_pb2.WorkResponse(
        has_work=True,
        filename="test.txt",
        fileserver_address="localhost:50052",
    )
    client.coordinator.RequestWork = Mock(return_value=mock_response)

    request = coordinator_pb2.WorkRequest(client_id=client.client_id)
    work = client.coordinator.RequestWork(request)

    assert work.has_work is True
    assert work.filename == "test.txt"
    assert work.fileserver_address == "localhost:50052"


def test_submit_primes_with_mock():
    """Test SubmitPrimes RPC with mock coordinator."""
    client = Client("test-client", "localhost:50051")

    mock_response = coordinator_pb2.SubmitResponse(success=True)
    client.coordinator.SubmitPrimes = Mock(return_value=mock_response)

    submit_request = coordinator_pb2.SubmitRequest(
        client_id=client.client_id, filename="test.txt", primes=[2, 3, 5, 7]
    )
    response = client.coordinator.SubmitPrimes(submit_request)

    assert response.success is True


def test_no_work_available():
    """Test handling when coordinator has no work."""
    client = Client("test-client", "localhost:50051")

    mock_response = coordinator_pb2.WorkResponse(has_work=False)
    client.coordinator.RequestWork = Mock(return_value=mock_response)

    request = coordinator_pb2.WorkRequest(client_id=client.client_id)
    work = client.coordinator.RequestWork(request)

    assert work.has_work is False


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
    mock_response = fileserver_pb2.FetchResponse(
        success=True, file_data=file_content, error_message=""
    )
    mock_stub = MagicMock()
    mock_stub.FetchFile.return_value = mock_response

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
    mock_response = fileserver_pb2.FetchResponse(
        success=True, file_data=file_content, error_message=""
    )
    mock_stub = MagicMock()
    mock_stub.FetchFile.return_value = mock_response

    with patch("grpc.insecure_channel"):
        with patch(
            "generated.fileserver_pb2_grpc.FileServerStub", return_value=mock_stub
        ):
            filepath1 = client._fetch_file("test.txt", "localhost:50052")
            filepath2 = client._fetch_file("test.txt", "localhost:50052")

    assert filepath1 == filepath2
    assert mock_stub.FetchFile.call_count == 1
