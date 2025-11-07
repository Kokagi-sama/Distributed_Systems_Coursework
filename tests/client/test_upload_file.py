import grpc
import pytest
from unittest.mock import MagicMock, patch

from client.client import Client
from generated import fileserver_pb2


class FakeRpcError(grpc.RpcError):
    def __init__(self, code):
        super().__init__()
        self._code = code

    def code(self):
        return self._code

    def __str__(self):
        return "FakeRpcError"


@pytest.fixture
def client_instance(tmp_path):
    # Three servers to exercise rotation
    return Client(
        client_id="test-client",
        coordinator_address="localhost:50051",
        fileserver_addresses=["serverA:1111", "serverB:2222", "serverC:3333"],
        cache_dir=str(tmp_path / "cache"),
    )


@pytest.fixture
def local_file(tmp_path):
    p = tmp_path / "sample.txt"
    p.write_text("hello world\nthis is a test\n")
    return p


def test_upload_success_first_try(client_instance, local_file):
    stub = MagicMock()
    stub.UploadFile.return_value = fileserver_pb2.UploadResponse(success=True)

    channel = MagicMock()

    with patch("grpc.insecure_channel", return_value=channel) as ch_p, \
         patch("generated.fileserver_pb2_grpc.FileServerStub", return_value=stub):
        ok = client_instance._upload_file(local_file, "remote.txt", "serverA:1111")

    assert ok is True
    assert stub.UploadFile.call_count == 1
    # Ensure channel was created and closed
    ch_p.assert_called_with("serverA:1111")
    channel.close.assert_called_once()

    # Verify UploadFile called with timeout and wait_for_ready
    _, kwargs = stub.UploadFile.call_args
    assert kwargs.get("timeout") == 60.0
    assert kwargs.get("wait_for_ready") is True


def test_upload_redirect_then_success(client_instance, local_file):
    stub = MagicMock()
    # First attempt fails with redirect, second succeeds
    stub.UploadFile.side_effect = [
        fileserver_pb2.UploadResponse(
            success=False,
            error_message="not primary",
            new_primary_fileserver="serverC:3333",
        ),
        fileserver_pb2.UploadResponse(success=True),
    ]

    channels = [MagicMock(), MagicMock()]

    with patch("grpc.insecure_channel", side_effect=channels) as ch_p, \
         patch("generated.fileserver_pb2_grpc.FileServerStub", return_value=stub):
        ok = client_instance._upload_file(local_file, "remote.txt", "serverA:1111")

    assert ok is True
    # Connected first to A, then redirected to C
    assert ch_p.call_args_list[0].args[0] == "serverA:1111"
    assert ch_p.call_args_list[1].args[0] == "serverC:3333"
    # Both connections closed
    assert channels[0].close.call_count == 1
    assert channels[1].close.call_count == 1
    assert stub.UploadFile.call_count == 2


def test_upload_rpc_error_then_success_rotates(client_instance, local_file):
    stub = MagicMock()
    stub.UploadFile.side_effect = [
        FakeRpcError(grpc.StatusCode.UNAVAILABLE),
        fileserver_pb2.UploadResponse(success=True),
    ]

    channels = [MagicMock(), MagicMock()]
    slept = []

    with patch("grpc.insecure_channel", side_effect=channels) as ch_p, \
         patch("generated.fileserver_pb2_grpc.FileServerStub", return_value=stub), \
         patch("random.uniform", return_value=0.0), \
         patch("time.sleep", side_effect=lambda s: slept.append(s)):
        ok = client_instance._upload_file(local_file, "remote.txt", "serverA:1111")

    assert ok is True
    # Rotates to next server after RPC error
    assert ch_p.call_args_list[0].args[0] == "serverA:1111"
    assert ch_p.call_args_list[1].args[0] in {"serverB:2222", "serverC:3333"}
    assert stub.UploadFile.call_count == 2
    assert channels[0].close.call_count == 1
    assert channels[1].close.call_count == 1
    # First backoff applied
    assert slept and pytest.approx(slept[0]) == 1.0


def test_upload_failure_no_redirect_backoff_then_success(client_instance, local_file):
    stub = MagicMock()
    # First attempt fails without redirect, then succeed
    stub.UploadFile.side_effect = [
        fileserver_pb2.UploadResponse(success=False, error_message="temp error"),
        fileserver_pb2.UploadResponse(success=True),
    ]

    channels = [MagicMock(), MagicMock()]
    slept = []

    # Make rotation deterministic: A -> B -> C (shuffle no-op)
    with patch("random.shuffle", lambda lst: None), \
         patch("grpc.insecure_channel", side_effect=channels) as ch_p, \
         patch("generated.fileserver_pb2_grpc.FileServerStub", return_value=stub), \
         patch("random.uniform", return_value=0.4), \
         patch("time.sleep", side_effect=lambda s: slept.append(s)):
        ok = client_instance._upload_file(local_file, "remote.txt", "serverA:1111")

    assert ok is True
    # One backoff sleep occurred: base 1.0 + jitter 0.4
    assert slept and pytest.approx(slept[0], rel=1e-6) == 1.4
    # Deterministic rotation A -> B
    assert ch_p.call_args_list[0].args[0] == "serverA:1111"
    assert ch_p.call_args_list[1].args[0] == "serverB:2222"
    assert channels[0].close.call_count == 1
    assert channels[1].close.call_count == 1
    assert stub.UploadFile.call_count == 2


def test_redirect_resets_backoff(client_instance, local_file):
    stub = MagicMock()
    stub.UploadFile.side_effect = [
        fileserver_pb2.UploadResponse(
            success=False, error_message="not primary", new_primary_fileserver="serverC:3333"
        ),
        fileserver_pb2.UploadResponse(success=False, error_message="temporary"),
        fileserver_pb2.UploadResponse(success=True),
    ]

    channels = [MagicMock(), MagicMock(), MagicMock()]
    slept = []

    with patch("random.shuffle", lambda lst: None), \
         patch("grpc.insecure_channel", side_effect=channels) as ch_p, \
         patch("generated.fileserver_pb2_grpc.FileServerStub", return_value=stub), \
         patch("random.uniform", return_value=0.0), \
         patch("time.sleep", side_effect=lambda s: slept.append(s)):
        ok = client_instance._upload_file(local_file, "remote.txt", "serverA:1111")

    assert ok is True
    # Calls: A (redirect) -> C (fail, sleep 1.0) -> A or B (rotation after C)
    assert ch_p.call_args_list[0].args[0] == "serverA:1111"
    assert ch_p.call_args_list[1].args[0] == "serverC:3333"
    # Backoff reset after redirect => first sleep is exactly 1.0 (jitter=0)
    assert slept and pytest.approx(slept[0]) == 1.0
    assert stub.UploadFile.call_count == 3
