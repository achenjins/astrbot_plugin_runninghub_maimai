"""AstrBot compatibility at the shared-core boundary; all transfers are mocked."""
import asyncio
import importlib
import importlib.util
import json
from pathlib import Path
import sys
import threading
from unittest.mock import MagicMock, patch

import pytest

from rh_generic_lib.image_memory import ImageMemory, MAX_IMAGE_BYTES
from rh_generic_lib.runninghub_client import RunningHubClient, RunningHubError, RunningHubTaskError
from rh_generic_lib.task_journal import TaskJournal


def test_per_download_limit_is_concurrent_safe_and_image_limit_is_preserved():
    client = RunningHubClient(base_url="https://example.test", api_key="key", workflow_id="1", max_file_bytes=64)
    def response(*args, **kwargs):
        value = MagicMock()
        value.__enter__.return_value = value
        value.iter_content.return_value = [b"123456"]
        return value
    async def run():
        with patch.object(client._session, "get", side_effect=response):
            results = await asyncio.gather(client.download_bytes("image", max_bytes=4),
                                           client.download_bytes("image", max_bytes=6), return_exceptions=True)
        assert isinstance(results[0], RunningHubError)
        assert results[1] == b"123456"
        assert client.max_file_bytes == 64
        seen = []
        async def download(url, *, max_bytes=None):
            seen.append(max_bytes)
            return b"image"
        client.download_bytes = download
        await client.download_base64("image")
        assert seen == [MAX_IMAGE_BYTES]
    try:
        asyncio.run(run())
    finally:
        client.close()


def test_poll_exception_exposes_both_upstream_and_legacy_status():
    client = RunningHubClient(base_url="https://example.test", api_key="key", workflow_id="1")
    async def query(task_id):
        return {"status": "CANCELLED"}
    client.query = query
    try:
        with pytest.raises(RunningHubTaskError) as caught:
            asyncio.run(client.wait_for_result("remote"))
        assert caught.value.status == caught.value.task_status == "CANCELLED"
    finally:
        client.close()


def test_image_memory_wrapper_enforces_size_before_writing(tmp_path):
    memory = ImageMemory(tmp_path / "images", ttl_seconds=60)
    with pytest.raises(ValueError, match="20MB"):
        asyncio.run(memory.remember("owner", "images", {"media_id": "1", "type": "image"},
                                    b"x" * (MAX_IMAGE_BYTES + 1), "", 20))
    assert not memory.path.exists()


def test_journal_preserves_old_astrbot_metadata_and_finished_time(tmp_path):
    path = tmp_path / "journal.json"
    record = {"task_id": "rh-old", "status": "success", "created_at": 10, "updated_at": 20,
              "platform_id": "qq-one", "platform_name": "aiocqhttp", "inputs": {"im-old": {"sha256": "blob"}},
              "actions": {"request": {"state": "completed"}}, "inputs_ready": True, "hidden": True,
              "stage": "delivering", "started_at": 11, "finished_at": 19, "busy_since": 12,
              "submitted_nodes": [{"nodeId": "1", "fieldName": "seed", "fieldValue": "42"}]}
    path.write_text(json.dumps([record]), encoding="utf-8")
    async def run():
        journal = TaskJournal(path)
        await journal.load()
        await journal.update("rh-old", message="resent")
        restored = TaskJournal(path)
        await restored.load()
        saved = restored.get("rh-old")
        for key, value in record.items():
            if key != "updated_at":
                assert saved[key] == value
        saved["inputs"].clear()
        assert restored.get("rh-old")["inputs"] == record["inputs"]
        await restored.update("rh-new", status="queued")
        await restored.update("rh-new", status="cancelled")
        finished = restored.get("rh-new")["finished_at"]
        assert finished > 0
        await restored.update("rh-new", message="acknowledged")
        assert restored.get("rh-new")["finished_at"] == finished
    asyncio.run(run())


def test_cancelled_journal_writer_finishes_before_lock_release(tmp_path):
    path = tmp_path / "journal.json"
    entered, release = threading.Event(), threading.Event()
    journal = TaskJournal(path)
    original = journal._write_file
    def blocked(records):
        entered.set()
        release.wait(3)
        original(records)
    async def run():
        await journal.load()
        journal._write_file = blocked
        writer = asyncio.create_task(journal.update("rh-one", status="queued", platform_id="qq"))
        assert await asyncio.to_thread(entered.wait, 2)
        writer.cancel()
        await asyncio.sleep(0)
        assert journal._lock.locked()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await writer
        assert json.loads(path.read_bytes())[0]["platform_id"] == "qq"
        assert journal.get("rh-one")["platform_id"] == "qq"
    asyncio.run(run())


def test_shared_modules_load_inside_plugin_namespace_without_global_alias(tmp_path):
    root = Path(__file__).resolve().parents[1]
    package = "offline_astrbot_namespace"
    spec = importlib.util.spec_from_file_location(package, root / "__init__.py",
                                                submodule_search_locations=[str(root)])
    module = importlib.util.module_from_spec(spec)
    before = set(sys.modules)
    sys.modules[package] = module
    try:
        # Do not execute the plugin entry point; this isolates package import boundaries.
        client = importlib.import_module(package + ".rh_generic_lib.runninghub_client")
        upload = importlib.import_module(package + ".rh_generic_lib.upstream.file_source")
        memory = importlib.import_module(package + ".rh_generic_lib.image_memory")
        assert upload.RunningHubError is client.RunningHubError
        assert memory.ImageMemory.__mro__[1].__module__.startswith(package + ".")
        assert client.RunningHubClient.__mro__[1].__module__.startswith(package + ".")
    finally:
        for name in set(sys.modules) - before:
            if name == package or name.startswith(package + "."):
                sys.modules.pop(name, None)
