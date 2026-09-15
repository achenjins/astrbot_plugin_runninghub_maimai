import asyncio
from types import SimpleNamespace

import pytest
import requests

from rh_generic_lib.runninghub_client import RunningHubClient, RunningHubError, RunningHubTransportError


@pytest.fixture
def client():
    return RunningHubClient(base_url="https://example.test", api_key="key", workflow_id="42", query_retries=2)


@pytest.mark.parametrize("body", [[], None, "html"])
def test_submission_invalid_response_is_uncertain(client, monkeypatch, body):
    monkeypatch.setattr(requests, "post", lambda *a, **k: SimpleNamespace(raise_for_status=lambda: None, json=lambda: body))
    with pytest.raises(RunningHubTransportError):
        asyncio.run(client.submit([]))


@pytest.mark.parametrize("status,error", [(403, RunningHubError), (429, RunningHubTransportError), (503, RunningHubTransportError)])
def test_http_refusal_vs_temporary_failure(client, monkeypatch, status, error):
    def fail():
        raise requests.HTTPError(response=SimpleNamespace(status_code=status))
    monkeypatch.setattr(requests, "post", lambda *a, **k: SimpleNamespace(raise_for_status=fail))
    with pytest.raises(error) as raised:
        asyncio.run(client.submit([]))
    assert type(raised.value) is error


@pytest.mark.parametrize("retries,expected", [(0, 1), (2, 3)])
def test_query_retry_setting_is_bounded_and_preserves_zero(client, monkeypatch, retries, expected):
    calls = []
    client.query_retries = retries

    async def query(tid):
        calls.append(tid)
        raise RunningHubTransportError("temporary")

    async def sleep(seconds):
        pass

    client.query = query
    monkeypatch.setattr(asyncio, "sleep", sleep)
    with pytest.raises(RunningHubTransportError):
        asyncio.run(client.wait_for_result("remote"))
    assert calls == ["remote"] * expected


@pytest.mark.parametrize("method,body", [("upload", []), ("workflow", {"data": []})])
def test_malformed_upload_and_workflow_data_are_clear_errors(client, monkeypatch, method, body):
    monkeypatch.setattr(requests, "post", lambda *a, **k: SimpleNamespace(raise_for_status=lambda: None, json=lambda: body))
    with pytest.raises(RunningHubError):
        asyncio.run(client.upload_file(b"data", "image.png") if method == "upload" else client.get_workflow_json("42"))
